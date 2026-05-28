package client

import "github.com/spf13/cobra"

const (
	defaultServerAddr = "localhost:50051"
	defaultClientID   = "client-1"
	defaultDatasetID  = "dataset-1"
)

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Client utilities for chunk storage",
	}

	cmd.AddCommand(newReadFromChunkCmd())
	cmd.AddCommand(newWriteChunkCmd())

	return cmd
}
