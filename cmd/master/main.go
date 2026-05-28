package main

import (
	"log"

	mastercli "github.com/Amir-Mallek/Distributed-Dataset-Repository/cli/master"
)

func main() {
	if err := mastercli.NewRootCmd().Execute(); err != nil {
		log.Fatal(err)
	}
}
