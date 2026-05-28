package master

import (
	"context"
	"errors"

	chunkmappb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	"github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunkmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server implements pb.MasterServiceServer by delegating every call to the
// chunkmap engine. It holds no state of its own.
type Server struct {
	pb.UnimplementedMasterServiceServer
	engine *chunkmap.Engine
}

// NewServer creates a MasterService gRPC server backed by a chunkmap.Engine
// rooted at baseDir.
func NewServer(baseDir string) (*Server, error) {
	e, err := chunkmap.NewEngine(baseDir)
	if err != nil {
		return nil, err
	}
	return &Server{engine: e}, nil
}

// Close releases the underlying BoltDB handle.
func (s *Server) Close() error {
	return s.engine.Close()
}

// grpcErr converts known engine sentinel errors to the appropriate gRPC status
// codes, and wraps everything else as Internal.
func grpcErr(err error) error {
	switch {
	case errors.Is(err, chunkmap.ErrChunkNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, chunkmap.ErrChunkAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, chunkmap.ErrNotSealed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, chunkmap.ErrHasActiveReaders):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}

func (s *Server) InsertChunk(_ context.Context, req *pb.InsertChunkRequest) (*emptypb.Empty, error) {
	if err := s.engine.InsertChunk(req.ChunkId, req.ClientId, req.DatasetId, req.ChunkIndex); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) AssignReplicas(_ context.Context, req *pb.AssignReplicasRequest) (*emptypb.Empty, error) {
	if err := s.engine.AssignReplicas(req.ChunkId, req.Servers, req.Primary); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ConfirmReplica(_ context.Context, req *pb.ConfirmReplicaRequest) (*emptypb.Empty, error) {
	if err := s.engine.ConfirmReplica(req.ChunkId, req.ServerAddr); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) SealChunk(_ context.Context, req *pb.SealChunkRequest) (*emptypb.Empty, error) {
	if err := s.engine.SealChunk(req.ChunkId, req.ActualSize, req.BlockChecksums); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetChunk(_ context.Context, req *pb.ChunkRequest) (*chunkmappb.ChunkRecord, error) {
	record, err := s.engine.GetChunk(req.ChunkId)
	if err != nil {
		return nil, grpcErr(err)
	}
	return record, nil
}

func (s *Server) GetServers(_ context.Context, req *pb.ChunkRequest) (*pb.GetServersResponse, error) {
	servers, err := s.engine.GetServers(req.ChunkId)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &pb.GetServersResponse{Servers: servers}, nil
}

func (s *Server) AcquireReadLock(_ context.Context, req *pb.ChunkRequest) (*emptypb.Empty, error) {
	if err := s.engine.AcquireReadLock(req.ChunkId); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ReleaseReadLock(_ context.Context, req *pb.ChunkRequest) (*emptypb.Empty, error) {
	if err := s.engine.ReleaseReadLock(req.ChunkId); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) MarkCorrupted(_ context.Context, req *pb.MarkCorruptedRequest) (*emptypb.Empty, error) {
	if err := s.engine.MarkCorrupted(req.ChunkId, req.ServerAddr); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) DeleteChunk(_ context.Context, req *pb.ChunkRequest) (*emptypb.Empty, error) {
	if err := s.engine.DeleteChunk(req.ChunkId); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

