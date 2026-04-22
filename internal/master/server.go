package master

import (
	"context"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
)

type MasterServer struct {
	pb.UnimplementedMasterServiceServer
	distributor Distributor
}

func NewMasterServer(d Distributor) *MasterServer {
	return &MasterServer{distributor: d}
}

func (s *MasterServer) GetServers(ctx context.Context, req *pb.GetServersRequest) (*pb.GetServersResponse, error) {
	selected, err := s.distributor.SelectServers(int(req.ReplicationFactor), nil)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, node := range selected {
		addrs = append(addrs, node.Address)
	}

	return &pb.GetServersResponse{
		ChunkId:         req.ChunkIndex,
		ServerAddresses: addrs,
	}, nil
}
