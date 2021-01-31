package main

import (
	"log"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport"
)

func main() {
	cdr := coordinator.Coordinator{
		Path:      "127.0.0.1:1234",
		Transport: &transport.NetRPC{},
	}
	log.Fatal(cdr.ListenAndServe())
}
