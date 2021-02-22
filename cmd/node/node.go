package main

import (
	"flag"
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/node"
	"github.com/despreston/go-craq/store/boltdb"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var addr, pub, cdr string

	// local address
	flag.StringVar(&addr, "a", "127.0.0.1:1235", "-a")

	// public address
	flag.StringVar(&pub, "p", "127.0.0.1:1235", "-p")

	// coordinator address
	flag.StringVar(&cdr, "c", "0.0.0.0:1234", "-c")

	flag.Parse()

	db := boltdb.New("my.db", "yessir")
	if err := db.Connect(); err != nil {
		log.Fatal(err)
	}

	defer db.DB.Close()

	n := node.New(node.Opts{
		Address:           addr,
		CdrAddress:        cdr,
		PubAddress:        pub,
		Store:             db,
		Transport:         netrpc.NewNodeClient,
		CoordinatorClient: netrpc.NewCoordinatorClient(),
		Log:               log.Default(),
	})

	b := netrpc.NodeBinding{Svc: n}
	if err := rpc.RegisterName("RPC", &b); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	// Start the node
	go n.Start()

	// Start the rpc server
	log.Println("Listening at " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
