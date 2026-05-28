package client

import (
	"context"
	"fmt"
	"os"

	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
	"github.com/spf13/cobra"
)

func newWriteChunkCmd() *cobra.Command {
	var chunkID uint32
	var clientID string
	var datasetID string

	cmd := &cobra.Command{
		Use:   "write-chunk <file path>",
		Short: "Write a file to a chunk",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runWriteChunk(args[0], chunkID, clientID, datasetID)
		},
	}

	cmd.Flags().Uint32Var(&chunkID, "chunk-id", 1, "chunk ID to write")
	cmd.Flags().StringVar(&clientID, "client-id", defaultClientID, "client identifier")
	cmd.Flags().StringVar(&datasetID, "dataset-id", defaultDatasetID, "dataset identifier")

	return cmd
}

func runWriteChunk(filePath string, chunkID uint32, clientID string, datasetID string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	client, err := chunktransfer.NewClient(defaultServerAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	fmt.Printf("Writing %q as chunk %d (%d bytes) to %s...\n", filePath, chunkID, len(data), defaultServerAddr)

	if err := client.SendChunk(context.Background(), chunkID, clientID, datasetID, nil, data); err != nil {
		return err
	}

	fmt.Printf("Chunk %d written successfully!\n", chunkID)
	return nil
}
