package main

import (
	"flag"
	"log"

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
	flag.StringVar(&cdr, "c", "127.0.0.1:1234", "-c")

	flag.Parse()

	db := boltdb.New("my.db", "yessir")
	if err := db.Connect(); err != nil {
		log.Fatal(err)
	}

	defer db.DB.Close()

	opts := node.Opts{
		Address:    addr,
		CdrAddress: cdr,
		PubAddress: pub,
		Store:      db,
		Transport:  &netrpc.Client{},
	}

	log.Fatal(node.New(opts).ListenAndServe())
}
