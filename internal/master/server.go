package master

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"time"

	chunkmappb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
	transferpb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	"github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunkmap"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultSweepInterval    = 10 * time.Second
	defaultHeartbeatTimeout = 30 * time.Second
	defaultChunkSizeBytes   = uint32(64 * 1024 * 1024)
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

func (s *Server) StartHeartbeatSweep(ctx context.Context, interval, timeout time.Duration) {
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	if timeout <= 0 {
		timeout = defaultHeartbeatTimeout
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepStaleServers(timeout)
			}
		}
	}()
}

func (s *Server) sweepStaleServers(timeout time.Duration) {
	now := time.Now()
	servers := s.engine.ListServers()

	for _, srv := range servers {
		if srv == nil || srv.ServerId == "" {
			continue
		}
		if srv.Status == chunkmappb.ServerStatus_SERVER_OFFLINE {
			continue
		}
		if srv.LastHeartbeat == nil {
			continue
		}
		if now.Sub(srv.LastHeartbeat.AsTime()) <= timeout {
			continue
		}

		if err := s.engine.UpdateServerStatus(srv.ServerId, chunkmappb.ServerStatus_SERVER_OFFLINE); err != nil {
			log.Printf("failed to mark server %s offline: %v", srv.ServerId, err)
			continue
		}
		if rr, ok := s.distributor.(*RoundRobinDistributor); ok {
			rr.AddOrUpdateNode(StorageNode{ID: srv.ServerId, Address: srv.Address, IsAlive: false})
		}

		s.handleServerDown(srv.ServerId)
	}
}

func (s *Server) handleServerDown(serverID string) {
	chunks := s.engine.ListChunks()
	for _, ch := range chunks {
		if ch == nil {
			continue
		}
		desired := len(ch.Replicas)
		if !chunkHasServer(ch, serverID) {
			continue
		}
		if err := s.engine.RemoveReplicaFromChunk(ch.ChunkId, serverID); err != nil {
			log.Printf("failed to remove replica %s from chunk %s: %v", serverID, ch.ChunkId, err)
			continue
		}
		s.ensureReplication(ch.ChunkId, desired)
	}
}

func chunkHasServer(ch *chunkmappb.ChunkRecord, serverID string) bool {
	for _, r := range ch.Replicas {
		if r != nil && r.ServerId == serverID {
			return true
		}
	}
	for _, id := range ch.ConfirmedReplicaIds {
		if id == serverID {
			return true
		}
	}
	if ch.PrimaryServerId == serverID {
		return true
	}
	return false
}

func (s *Server) ensureReplication(chunkID string, desired int) {
	if desired <= 0 {
		return
	}
	chunk, err := s.engine.GetChunk(chunkID)
	if err != nil {
		log.Printf("failed to load chunk %s: %v", chunkID, err)
		return
	}
	if len(chunk.Replicas) >= desired {
		return
	}

	sourceAddr := s.pickSourceAddr(chunk)
	if sourceAddr == "" {
		log.Printf("no available source replica for chunk %s", chunkID)
		return
	}

	exclude := make([]string, 0, len(chunk.Replicas))
	for _, r := range chunk.Replicas {
		if r != nil && r.ServerId != "" {
			exclude = append(exclude, r.ServerId)
		}
	}

	missing := desired - len(chunk.Replicas)
	selected, err := s.distributor.SelectServers(missing, exclude)
	if err != nil {
		log.Printf("not enough healthy servers to re-replicate chunk %s: %v", chunkID, err)
		return
	}

	for _, node := range selected {
		if node.Address == "" || node.ID == "" {
			continue
		}
		serverRecord := &chunkmappb.ServerRecord{ServerId: node.ID, Address: node.Address, Status: chunkmappb.ServerStatus_SERVER_HEALTHY}
		if err := s.engine.AddReplica(chunkID, serverRecord); err != nil {
			log.Printf("failed to add replica %s to chunk %s: %v", node.ID, chunkID, err)
			continue
		}
		if err := s.replicateChunk(chunk, sourceAddr, node.Address); err != nil {
			log.Printf("replication failed for chunk %s to %s: %v", chunkID, node.Address, err)
			_ = s.engine.RemoveReplicaFromChunk(chunkID, node.ID)
			continue
		}
		_ = s.engine.ConfirmReplica(chunkID, node.ID)
	}
}

func (s *Server) pickSourceAddr(chunk *chunkmappb.ChunkRecord) string {
	servers := s.engine.ListServers()
	serverMap := make(map[string]*chunkmappb.ServerRecord, len(servers))
	for _, srv := range servers {
		if srv != nil && srv.ServerId != "" {
			serverMap[srv.ServerId] = srv
		}
	}
	for _, id := range chunk.ConfirmedReplicaIds {
		if srv, ok := serverMap[id]; ok && srv.Status == chunkmappb.ServerStatus_SERVER_HEALTHY && srv.Address != "" {
			return srv.Address
		}
	}
	for _, r := range chunk.Replicas {
		if r != nil && r.Address != "" {
			return r.Address
		}
	}
	return ""
}

func (s *Server) replicateChunk(chunk *chunkmappb.ChunkRecord, sourceAddr, targetAddr string) error {
	if sourceAddr == "" || targetAddr == "" {
		return errors.New("missing source or target address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	srcConn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer srcConn.Close()

	tgtConn, err := grpc.Dial(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer tgtConn.Close()

	srcClient := transferpb.NewChunkTransferServiceClient(srcConn)
	tgtClient := transferpb.NewChunkTransferServiceClient(tgtConn)

	readStream, err := srcClient.ReadFromChunk(ctx, &transferpb.ReadFromChunkRequest{ChunkId: chunk.ChunkId, RangeStart: 0, RangeEnd: defaultChunkSizeBytes})
	if err != nil {
		return err
	}

	writeStream, err := tgtClient.WriteChunk(ctx)
	if err != nil {
		return err
	}

	meta := &transferpb.ChunkMetadata{
		ChunkId:    chunk.ChunkId,
		ClientId:   chunk.ClientId,
		DatasetId:  chunk.DatasetId,
		ReplicaSet: []string{targetAddr},
	}
	if err := writeStream.Send(&transferpb.WriteChunkRequest{Msg: &transferpb.WriteChunkRequest_Meta{Meta: meta}}); err != nil {
		return err
	}

	for {
		msg, err := readStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		block := msg.Data
		if len(block) == 0 {
			continue
		}
		checksum := crc32.ChecksumIEEE(block)
		if err := writeStream.Send(&transferpb.WriteChunkRequest{Msg: &transferpb.WriteChunkRequest_Block{Block: &transferpb.ChunkBlock{Data: block, Checksum: checksum}}}); err != nil {
			return err
		}
	}

	if _, err := writeStream.CloseAndRecv(); err != nil {
		return err
	}
	return nil
}

func (s *Server) MarkCorrupted(_ context.Context, req *pb.MarkCorruptedRequest) (*emptypb.Empty, error) {
	if err := s.engine.MarkCorrupted(req.ChunkId, req.ServerId); err != nil {
		return nil, grpcErr(err)
	}
	if err := s.engine.RemoveReplicaFromChunk(req.ChunkId, req.ServerId); err != nil {
		return nil, grpcErr(err)
	}
	chunk, err := s.engine.GetChunk(req.ChunkId)
	if err == nil {
		s.ensureReplication(req.ChunkId, len(chunk.Replicas)+1)
	}
	return &emptypb.Empty{}, nil
}
