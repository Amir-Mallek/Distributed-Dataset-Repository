package main

import (
	"context"
	"fmt"
	"log"
	"os"

	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
)

const serverAddr = "localhost:50051"

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: client <client id> <dataset id> <file path>")
	}

	clientId := os.Args[1]
	datasetId := os.Args[2]
	filePath := os.Args[3]

	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	client, err := chunktransfer.NewClient(serverAddr)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	chunkId := uint32(1)
	fmt.Printf("Sending file %q as chunk %d (%d bytes) to %s...\n", filePath, chunkId, len(data), serverAddr)

	if err := client.SendChunk(context.Background(), clientId, datasetId, chunkId, data); err != nil {
		log.Fatalf("failed to send chunk: %v", err)
	}

	fmt.Printf("Chunk %d sent successfully!\n", chunkId)
}
