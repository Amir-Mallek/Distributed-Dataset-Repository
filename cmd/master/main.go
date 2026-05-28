package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	"github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/master"
	"google.golang.org/grpc"
)

const (
	port    = ":50052"
	baseDir = "masterdata"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv, err := master.NewServer(baseDir)
	if err != nil {
		log.Fatalf("failed to create master server: %v", err)
	}
	defer srv.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServiceServer(grpcServer, srv)

	fmt.Printf("Master server listening on %s\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

