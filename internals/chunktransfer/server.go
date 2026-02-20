package chunktransfer

import (
	"context"
	"io"
	"sync"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
	chunks map[uint32]string
	mu     sync.RWMutex
}

func NewServer() *Server {
	return &Server{chunks: make(map[uint32]string)}
}

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*emptypb.Empty, error) {
	// to be replaced when moncef offers an api to write to disk

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exist := s.chunks[req.ChunkId]; exist {
		return nil, status.Error(codes.AlreadyExists, "chunk already exists")
	}
	s.chunks[req.ChunkId] = "chunk disk path"
	return &emptypb.Empty{}, nil
}

func (s *Server) WriteBlock(stream pb.ChunkTransferService_WriteBlockServer) error {
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		s.mu.RLock()
		_, exist := s.chunks[chunk.ChunkId]
		s.mu.RUnlock()
		if !exist {
			return status.Error(codes.NotFound, "chunk not found")
		}
		// call the api to save block
	}
}

func (s *Server) ReadFromChunk(req *pb.ReadFromChunkRequest, stream pb.ChunkTransferService_ReadFromChunkServer) error {
	s.mu.RLock()
	_, exist := s.chunks[req.ChunkId]
	s.mu.RUnlock()

	if !exist {
		return status.Error(codes.NotFound, "chunk does not exist")
	}

	// iterate over blocks and stream them using
	// if err := stream.Send(block); err != nil {
	// 	return err
	// }
	return nil
}
