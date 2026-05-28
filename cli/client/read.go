package client

import (
	"context"
	"fmt"
	"os"

	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
	"github.com/spf13/cobra"
)

func newReadFromChunkCmd() *cobra.Command {
	var chunkID uint32
	var rangeStart uint32
	var rangeEnd uint32
	var outFile string

	cmd := &cobra.Command{
		Use:   "read-chunk",
		Short: "Read bytes from a chunk",
		RunE: func(_ *cobra.Command, _ []string) error {
			return readFromChunk(chunkID, rangeStart, rangeEnd, outFile)
		},
	}

	cmd.Flags().Uint32Var(&chunkID, "chunk-id", 1, "chunk ID to read")
	cmd.Flags().Uint32Var(&rangeStart, "start", 0, "inclusive byte start offset")
	cmd.Flags().Uint32Var(&rangeEnd, "end", 1024, "exclusive byte end offset")
	cmd.Flags().StringVar(&outFile, "out", "download.bin", "output file path")

	return cmd
}

func readFromChunk(chunkID uint32, rangeStart uint32, rangeEnd uint32, outFile string) error {
	if rangeEnd <= rangeStart {
		return fmt.Errorf("invalid range: --end must be greater than --start")
	}

	client, err := chunktransfer.NewClient(defaultServerAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	data, err := client.ReadFromChunk(context.Background(), chunkID, rangeStart, rangeEnd)
	if err != nil {
		return err
	}

	if err := os.WriteFile(outFile, data, 0644); err != nil {
		return err
	}

	fmt.Printf("Read %d bytes from chunk %d to %q\n", len(data), chunkID, outFile)
	return nil
}
