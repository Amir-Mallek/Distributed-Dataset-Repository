package main

import (
	"log"

	clientcli "github.com/Amir-Mallek/Distributed-Dataset-Repository/cli/client"
)

func main() {
	if err := clientcli.NewRootCmd().Execute(); err != nil {
		log.Fatal(err)
	}
}
