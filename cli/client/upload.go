package client

import (
	"context"
	"fmt"

	masterpb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	"github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newUploadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload <file>",
		Short: "Upload a file as a dataset (creates dataset + uploads chunks)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			// connect to master
			conn, err := grpc.Dial(defaultServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("failed to connect to master: %w", err)
			}
			defer conn.Close()
			mc := masterpb.NewClientServiceClient(conn)
			uploader := client.NewFileUploader(mc)
			return uploader.UploadFile(context.Background(), args[0])
		},
	}
	return cmd
}
