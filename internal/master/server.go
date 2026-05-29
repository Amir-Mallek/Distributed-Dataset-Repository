package master

import (
	"context"
	"errors"

	chunkmappb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	"github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunkmap"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements pb.MasterServiceServer by delegating every call to the
// chunkmap engine. It holds no state of its own.
type Server struct {
	pb.UnimplementedClientServiceServer
	pb.UnimplementedStorageServiceServer
	engine      *chunkmap.Engine
	distributor Distributor
}

// NewServer creates a MasterService gRPC server backed by a chunkmap.Engine
// rooted at baseDir.
func NewServer(baseDir string, d Distributor) (*Server, error) {
	e, err := chunkmap.NewEngine(baseDir)
	if err != nil {
		return nil, err
	}
	return &Server{engine: e, distributor: d}, nil
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

// Note: InsertChunk RPC removed in new proto; chunk creation handled via RequestChunkWrite

// CreateDataset registers a new dataset and returns its generated ID.
func (s *Server) CreateDataset(_ context.Context, req *pb.CreateDatasetRequest) (*pb.CreateDatasetResponse, error) {
	dsid := uuid.NewString()
	ds := &chunkmappb.DatasetRecord{
		DatasetId: dsid,
		Name:      req.Name,
		ClientId:  req.ClientId,
		CreatedAt: timestamppb.Now(),
	}
	if err := s.engine.CreateDataset(ds); err != nil {
		return nil, grpcErr(err)
	}
	return &pb.CreateDatasetResponse{DatasetId: dsid, Dataset: ds}, nil
}

// RequestChunkWrite assigns a new chunk id and selects replica servers.
func (s *Server) RequestChunkWrite(_ context.Context, req *pb.RequestChunkWriteRequest) (*pb.RequestChunkWriteResponse, error) {
	chunkID := uuid.NewString()

	// find dataset to get client id if present
	ds, _ := s.engine.GetDataset(req.DatasetId)
	clientID := ""
	if ds != nil {
		clientID = ds.ClientId
	}

	// pick servers
	selected, err := s.distributor.SelectServers(int(req.ReplicationFactor), nil)
	if err != nil {
		return nil, grpcErr(err)
	}

	serverIDs := make([]string, 0, len(selected))
	var replicas []*chunkmappb.ServerRecord
	for _, n := range selected {
		serverIDs = append(serverIDs, n.ID)
		replicas = append(replicas, &chunkmappb.ServerRecord{
			ServerId: n.ID,
			Address:  n.Address,
			Status:   chunkmappb.ServerStatus_SERVER_HEALTHY, // hedhi mch atyar haja khtr lezem yaamel check fel bd
		})
	}

	// insert chunk record
	if err := s.engine.InsertChunk(chunkID, clientID, req.DatasetId, req.ChunkIndex); err != nil {
		return nil, grpcErr(err)
	}
	// assign replicas and persist
	if err := s.engine.AssignReplicas(chunkID, serverIDs, serverIDs[0]); err != nil {
		return nil, grpcErr(err)
	}

	return &pb.RequestChunkWriteResponse{ChunkId: chunkID, Replicas: replicas}, nil
}

func (s *Server) RegisterServer(_ context.Context, req *pb.RegisterServerRequest) (*pb.RegisterServerResponse, error) {
	sr := &chunkmappb.ServerRecord{ServerId: req.ServerId, Address: req.Address, LastHeartbeat: timestamppb.Now(), Status: chunkmappb.ServerStatus_SERVER_HEALTHY}
	if err := s.engine.CreateOrUpdateServer(sr); err != nil {
		return nil, grpcErr(err)
	}
	// inform distributor if it supports dynamic nodes
	if rr, ok := s.distributor.(*RoundRobinDistributor); ok {
		rr.AddOrUpdateNode(StorageNode{ID: req.ServerId, Address: req.Address, IsAlive: true})
	}
	return &pb.RegisterServerResponse{Server: sr}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	sr := &chunkmappb.ServerRecord{ServerId: req.ServerId, Address: req.Address, LastHeartbeat: timestamppb.Now(), Status: chunkmappb.ServerStatus_SERVER_HEALTHY}
	if err := s.engine.CreateOrUpdateServer(sr); err != nil {
		return nil, grpcErr(err)
	}
	if rr, ok := s.distributor.(*RoundRobinDistributor); ok {
		rr.AddOrUpdateNode(StorageNode{ID: req.ServerId, Address: req.Address, IsAlive: true})
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ConfirmReplica(_ context.Context, req *pb.ConfirmReplicaRequest) (*emptypb.Empty, error) {
	if err := s.engine.ConfirmReplica(req.ChunkId, req.ServerId); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) MarkCorrupted(_ context.Context, req *pb.MarkCorruptedRequest) (*emptypb.Empty, error) {
	if err := s.engine.MarkCorrupted(req.ChunkId, req.ServerId); err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}
