package chunktransfer

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	st "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
	chunks map[uint32]string
	mu     sync.RWMutex
	db     *st.DiskEngine
}

func verifyChunk(data []byte, checksum uint32) bool {
	return crc32.ChecksumIEEE(data) == checksum
}

func NewServer(baseDir string) (*Server, error) {
	db, err := st.NewDiskEngine(baseDir)
	if err != nil {
		return nil, err
	}
	return &Server{
		chunks: make(map[uint32]string),
		db:     db,
	}, nil
}

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*emptypb.Empty, error) {
	err := s.db.CreateChunk(req.ChunkId, req.ClientId, req.DatasetId, uint32(req.ChunkSize))
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create chunk: %v", err))
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) WriteBlock(stream pb.ChunkTransferService_WriteBlockServer) error {
	// block other read/write operations
	s.mu.Lock()
	defer s.mu.Unlock()
	var filePath string
	var chunkId uint32
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// seal the chunk after all blocks are received
			if chunkId == 0 {
				return status.Error(codes.InvalidArgument, "no blocks received")
			}
			err = s.db.SealChunk(chunkId)
			if err != nil {
				return status.Error(codes.Internal, fmt.Sprintf("failed to seal chunk: %v", err))
			}
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to receive block: %v", err))
		}

		// track the chunk ID
		chunkId = chunk.ChunkId

		if filePath == "" {
			filePath = s.db.GetFilePath(chunk.ClientId, chunk.DatasetId, chunk.ChunkId)
		}

		// verify chunk integrity before writing
		if !verifyChunk(chunk.Data, chunk.Checksum) {
			return status.Error(codes.DataLoss, "checksum mismatch for block")
		}

		err = s.db.WriteBlock(chunk.ChunkId, chunk.BlockIndex, chunk.Data, chunk.Checksum, filePath)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to write block: %v", err))
		}
	}
}

func (s *Server) ReadFromChunk(req *pb.ReadFromChunkRequest, stream pb.ChunkTransferService_ReadFromChunkServer) error {
	// read lock: allow multiple readers but gets blocked by a write lock
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, err := s.db.GetChunkForRead(req.ChunkId)
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("chunk not found: %v", err))
	}

	f, err := os.Open(s.db.GetFilePath(meta.ClientId, meta.DatasetId, meta.ChunkId))
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to open chunk file: %v", err))
	}
	defer f.Close()

	// validate and compute length to read
	start := int64(req.RangeStart)
	end := int64(req.RangeEnd)
	totalSize := int64(meta.TotalSize)

	// check for invalid range
	if start > totalSize {
		return status.Error(codes.InvalidArgument, "range start exceeds chunk size")
	}
	if end <= start {
		return status.Error(codes.InvalidArgument, "range end must be greater than range start")
	}
	if end > totalSize {
		end = totalSize
	}

	length := end - start

	// seek to start
	_, err = f.Seek(start, io.SeekStart)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to seek in chunk file: %v", err))
	}

	bufSize := 64 * 1024
	buf := make([]byte, bufSize)
	remaining := length

	for remaining > 0 {
		toRead := bufSize
		if int64(toRead) > remaining {
			toRead = int(remaining)
		}

		n, err := f.Read(buf[:toRead])
		if n > 0 {
			if sendErr := stream.Send(&pb.ChunkData{Data: buf[:n]}); sendErr != nil {
				return status.Error(codes.Internal, fmt.Sprintf("failed to send data: %v", sendErr))
			}
			remaining -= int64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("failed to read chunk data: %v", err))
		}
	}

	return nil
}
