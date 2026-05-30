package master

import (
	"context"
	"log"
	"net"
	"time"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	master "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/master"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const defaultAddr = ":50051"

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "master",
		Short: "Master server for Distributed Dataset Repository",
	}
	cmd.AddCommand(newServeCmd())
	return cmd
}

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run master gRPC server",
		RunE: func(_ *cobra.Command, _ []string) error {
			lis, err := net.Listen("tcp", defaultAddr)
			if err != nil {
				return err
			}
			defer lis.Close()

			// create distributor and server
			d := master.NewRoundRobinDistributor(nil)
			s, err := master.NewServer("metadata", d)
			if err != nil {
				return err
			}
			s.StartHeartbeatSweep(context.Background(), 10*time.Second, 15*time.Second)
			grpcServer := grpc.NewServer()
			pb.RegisterClientServiceServer(grpcServer, s)
			pb.RegisterStorageServiceServer(grpcServer, s)

			log.Printf("master listening on %s", defaultAddr)
			return grpcServer.Serve(lis)
		},
	}
}
