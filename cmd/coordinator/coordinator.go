package main

import (
	"log"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	cdr := coordinator.Coordinator{
		Path:      "127.0.0.1:1234",
		Transport: &netrpc.Client{},
	}
	log.Fatal(cdr.ListenAndServe())
}
