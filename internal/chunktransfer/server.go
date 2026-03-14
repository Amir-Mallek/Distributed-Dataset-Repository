package chunktransfer

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	pb2 "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/metastorage"
	st "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/metastorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	maxBlocks      = 1024
	chunkFileSize  = 64 * 1024 * 1024 // 64 MB
	defaultBaseDir = "chunks"
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
	db      *st.DiskEngine
	baseDir string
}

func NewServer(baseDir string) (*Server, error) {
	if baseDir == "" {
		baseDir = defaultBaseDir
	}

	db, err := st.NewDiskEngine(baseDir)
	if err != nil {
		return nil, err
	}
	return &Server{db: db, baseDir: baseDir}, nil
}

func (s *Server) getFilePath(clientID, datasetID string, chunkID uint32) string {
	return filepath.Join(s.baseDir, clientID, datasetID, fmt.Sprintf("%d", chunkID))
}

func verifyChecksum(data []byte, checksum uint32) bool {
	return crc32.ChecksumIEEE(data) == checksum
}

func createChunkFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	// Pre-allocate 64 MB
	if err := f.Truncate(chunkFileSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// todo : make error messages more descriptive
func (s *Server) createChunk(clientID, datasetID string, chunkID uint32) (*os.File, error) {
	if err := s.db.CreateChunk(chunkID, clientID, datasetID, chunkFileSize); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create chunk: %v", err)
	}
	filePath := s.getFilePath(clientID, datasetID, chunkID)
	file, err := createChunkFile(filePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create chunk file: %v", err)
	}
	return file, nil
}

func (s *Server) WriteChunk(stream pb.ChunkTransferService_WriteChunkServer) error {
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive metadata: %v", err)
	}

	metaMsg, ok := req.Msg.(*pb.WriteChunkRequest_Meta)
	if !ok {
		return status.Error(codes.InvalidArgument, "first message must be chunk metadata")
	}
	meta := metaMsg.Meta

	checksums := make([]uint32, maxBlocks)
	chunkID := meta.ChunkId
	clientID := meta.ClientId
	datasetID := meta.DatasetId

	forwarder, err := s.NewForwarder(stream.Context(), meta)
	if err != nil {
		return err
	}
	if forwarder != nil {
		defer forwarder.Abort()
	}

	file, err := s.createChunk(clientID, datasetID, chunkID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create chunk file: %v", err)
	}
	defer file.Close()

	blockCount := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			if err := s.db.SealChunk(chunkID, checksums); err != nil {
				return status.Errorf(codes.Internal, "failed to seal chunk: %v", err)
			}
			if forwarder != nil {
				if err := forwarder.CloseAndWait(); err != nil {
					return err
				}
			}
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive block: %v", err)
		}
		if blockCount >= maxBlocks {
			return status.Errorf(codes.ResourceExhausted, "exceeded max blocks per chunk (%d)", maxBlocks)
		}

		blockMsg, ok := msg.Msg.(*pb.WriteChunkRequest_Block)
		if !ok || blockMsg.Block == nil {
			return status.Error(codes.InvalidArgument, "only block messages are allowed after metadata")
		}
		block := blockMsg.Block

		if !verifyChecksum(block.Data, block.Checksum) {
			return status.Error(codes.DataLoss, "checksum mismatch for block")
		}

		if _, err := file.Write(block.Data); err != nil {
			return status.Errorf(codes.Internal, "failed to write block: %v", err)
		}

		if forwarder != nil {
			if err := forwarder.Send(block); err != nil {
				return err
			}
		}

		checksums[blockCount] = block.Checksum
		blockCount++
	}
}

func (s *Server) ReadFromChunk(req *pb.ReadFromChunkRequest, stream pb.ChunkTransferService_ReadFromChunkServer) error {
	meta, err := s.db.GetChunkMetadata(req.ChunkId)
	if err != nil {
		return status.Errorf(codes.NotFound, "chunk not found: %v", err)
	}
	if meta.Status != pb2.ChunkStatus_SEALED {
		return status.Error(codes.FailedPrecondition, "chunk is not sealed")
	}

	filePath := s.getFilePath(meta.ClientId, meta.DatasetId, meta.ChunkId)
	f, err := os.Open(filePath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open chunk file: %v", err)
	}
	defer f.Close()

	start := int64(req.RangeStart)
	end := int64(req.RangeEnd)
	totalSize := int64(meta.TotalSize)

	if start > totalSize {
		return status.Error(codes.InvalidArgument, "range start exceeds chunk size")
	}
	if end <= start {
		return status.Error(codes.InvalidArgument, "range end must be greater than range start")
	}
	if end > totalSize {
		end = totalSize
	}

	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return status.Errorf(codes.Internal, "failed to seek in chunk file: %v", err)
	}

	buf := make([]byte, 64*1024)
	remaining := end - start

	for remaining > 0 {
		toRead := int64(64 * 1024)
		if toRead > remaining {
			toRead = remaining
		}
		readSize := int(toRead)

		nbRead, err := f.Read(buf[:readSize])
		if int64(nbRead) == toRead {
			if sendErr := stream.Send(&pb.ChunkData{Data: buf[:readSize]}); sendErr != nil {
				return status.Errorf(codes.Internal, "failed to send data: %v", sendErr)
			}
			remaining -= toRead
		} else {
			return status.Errorf(codes.Internal, "failed to read chunk data: expected %d bytes, got %d", toRead, nbRead)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read chunk data: %v", err)
		}
	}

	return nil
}
