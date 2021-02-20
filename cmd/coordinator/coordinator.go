package main

import (
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	c := coordinator.Coordinator{
		Address:   "0.0.0.0:1234",
		Transport: netrpc.NewNodeClient,
	}

	binding := netrpc.CoordinatorBinding{Svc: &c}
	if err := rpc.RegisterName("RPC", &binding); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	// Start the Coordinator
	go c.Start()

	// Start the rpc server
	log.Println("Listening at " + c.Address)
	log.Fatal(http.ListenAndServe(c.Address, nil))
}
