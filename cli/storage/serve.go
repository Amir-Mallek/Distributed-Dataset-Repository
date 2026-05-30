package storage

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	masterpb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	selfAddr := os.Getenv("SERVER_ADDR")
	masterAddr := os.Getenv("MASTER_ADDR")
	serverID := os.Getenv("SERVER_ID")
	server, err := chunktransfer.NewServer(defaultDataDir, selfAddr, masterAddr, serverID)
	if err != nil {
		return err
	}
	pb.RegisterChunkTransferServiceServer(grpcServer, server)

	// optional registration with master
	serverAddr := selfAddr
	if masterAddr != "" && serverID != "" {
		go func() {
			// try connecting and send periodic heartbeat
			for {
				conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("failed to connect to master %s: %v", masterAddr, err)
					time.Sleep(5 * time.Second)
					continue
				}
				mclient := masterpb.NewStorageServiceClient(conn)
				// register once
				_, err = mclient.RegisterServer(context.Background(), &masterpb.RegisterServerRequest{ServerId: serverID, Address: serverAddr})
				if err != nil {
					log.Printf("register server failed: %v", err)
				}

				// heartbeat loop
				ticker := time.NewTicker(10 * time.Second)
				for range ticker.C {
					_, err := mclient.Heartbeat(context.Background(), &masterpb.HeartbeatRequest{ServerId: serverID, Address: serverAddr})
					if err != nil {
						log.Printf("heartbeat failed: %v", err)
						break
					}
				}
				_ = conn.Close()
				time.Sleep(5 * time.Second)
			}
		}()
	}

	log.Printf("storage server listening on %s", defaultAddr)
	if err := grpcServer.Serve(lis); err != nil {
		return err
	}

	return nil
}
