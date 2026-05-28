package master

import (
	"log"

	"github.com/spf13/cobra"
)

// make basic print hello master command

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "master",
		Short: "Master server for Distributed Dataset Repository",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Hello, Master!")
		},
	}

	return cmd
}
