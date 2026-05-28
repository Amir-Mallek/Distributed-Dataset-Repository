package storage

import (
	"log"
	"net"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	defaultAddr    = ":50051"
	defaultDataDir = "chunks"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the storage server",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runServer()
		},
	}
}

func runServer() error {
	lis, err := net.Listen("tcp", defaultAddr)
	if err != nil {
		return err
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	server, err := chunktransfer.NewServer(defaultDataDir)
	if err != nil {
		return err
	}
	pb.RegisterChunkTransferServiceServer(grpcServer, server)

	log.Printf("storage server listening on %s", defaultAddr)
	if err := grpcServer.Serve(lis); err != nil {
		return err
	}

	return nil
}
