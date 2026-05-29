package client

import "github.com/spf13/cobra"

const (
	defaultServerAddr = "localhost:50051"
	defaultClientID   = "client-1"
	defaultDatasetID  = "dataset-1"
)

func composeHostAddress(addr string) string {
	switch addr {
	case "storage1:50051":
		return "localhost:50052"
	case "storage2:50051":
		return "localhost:50053"
	case "storage3:50051":
		return "localhost:50054"
	case "storage4:50051":
		return "localhost:50055"
	default:
		return addr
	}
}

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Client utilities for chunk storage",
	}

	cmd.AddCommand(newUploadCmd())
	cmd.AddCommand(newReadFromChunkCmd())

	return cmd
}
