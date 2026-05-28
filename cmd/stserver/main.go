package main

import (
	"log"

	storagecli "github.com/Amir-Mallek/Distributed-Dataset-Repository/cli/storage"
)

func main() {
	if err := storagecli.NewRootCmd().Execute(); err != nil {
		log.Fatal(err)
	}
}
