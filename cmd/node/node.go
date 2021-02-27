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
	var addr, pub, cdr, dbFile string

	flag.StringVar(&addr, "a", ":1235", "Local address to listen on")
	flag.StringVar(&pub, "p", ":1235", "Public address reachable by coordinator and other nodes")
	flag.StringVar(&cdr, "c", ":1234", "Coordinator address")
	flag.StringVar(&dbFile, "f", "craq.db", "Bolt DB database file")
	flag.Parse()

	db := boltdb.New(dbFile, "yessir")
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
