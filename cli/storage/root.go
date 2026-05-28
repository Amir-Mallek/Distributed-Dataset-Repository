package storage

import "github.com/spf13/cobra"

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stserver",
		Short: "Storage server commands",
	}

	cmd.AddCommand(newServeCmd())
	return cmd
}
