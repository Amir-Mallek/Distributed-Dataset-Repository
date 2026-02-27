package chunktransfer

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	st "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/metastorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	maxBlocks     = 1000
	chunkFileSize = 64 * 1024 * 1024 // 64 MB
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
	db      *st.DiskEngine
	baseDir string
}

func NewServer(baseDir string) (*Server, error) {
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
	// Seek back to start for writing
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*emptypb.Empty, error) {
	if err := s.db.CreateChunk(req.ChunkId, req.ClientId, req.DatasetId, req.ChunkSize); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create chunk: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) WriteBlock(stream pb.ChunkTransferService_WriteBlockServer) error {
	var file *os.File
	var chunkID uint32
	var clientID, datasetID string
	var initialized bool
	checksums := make([]uint32, maxBlocks)
	blockCount := uint32(0)

	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()

	for {
		block, err := stream.Recv()
		if err == io.EOF {
			if file != nil {
				if err := file.Close(); err != nil {
					return status.Errorf(codes.Internal, "failed to close chunk file: %v", err)
				}
				file = nil
			}
			if !initialized {
				return status.Error(codes.InvalidArgument, "no blocks received")
			}
			if err := s.db.SealChunk(chunkID, checksums[:blockCount]); err != nil {
				return status.Errorf(codes.Internal, "failed to seal chunk: %v", err)
			}
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive block: %v", err)
		}

		if !initialized {
			chunkID = block.ChunkId
			clientID = block.ClientId
			datasetID = block.DatasetId
			initialized = true

			filePath := s.getFilePath(clientID, datasetID, chunkID)
			file, err = createChunkFile(filePath)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to create chunk file: %v", err)
			}
		}

		if blockCount >= maxBlocks {
			return status.Errorf(codes.ResourceExhausted, "exceeded max blocks per chunk (%d)", maxBlocks)
		}

		if !verifyChecksum(block.Data, block.Checksum) {
			return status.Error(codes.DataLoss, "checksum mismatch for block")
		}

		if _, err := file.Write(block.Data); err != nil {
			return status.Errorf(codes.Internal, "failed to write block: %v", err)
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
		toRead := int64(len(buf))
		if toRead > remaining {
			toRead = remaining
		}

		n, err := f.Read(buf[:toRead])
		if n > 0 {
			if sendErr := stream.Send(&pb.ChunkData{Data: buf[:n]}); sendErr != nil {
				return status.Errorf(codes.Internal, "failed to send data: %v", sendErr)
			}
			remaining -= int64(n)
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
