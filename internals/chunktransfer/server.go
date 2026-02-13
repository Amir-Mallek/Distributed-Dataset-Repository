package chunktransfer

import (
	"context"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	pb.UnimplementedChunkTransferServiceServer
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*emptypb.Empty, error) {
	// TODO: implement
	return &emptypb.Empty{}, nil
}

func (s *Server) WriteBlock(stream pb.ChunkTransferService_WriteBlockServer) error {
	// TODO: implement
	return nil
}

func (s *Server) ReadFromChunk(req *pb.ReadFromChunkRequest, stream pb.ChunkTransferService_ReadFromChunkServer) error {
	// TODO: implement
	return nil
}
