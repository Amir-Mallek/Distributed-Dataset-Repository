package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
	"google.golang.org/grpc"
)

const (
	port    = ":50051"
	baseDir = "chunks"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	server, err := chunktransfer.NewServer(baseDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	pb.RegisterChunkTransferServiceServer(grpcServer, server)

	fmt.Printf("Storage server listening on %s\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
