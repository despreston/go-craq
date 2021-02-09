package main

import (
	"flag"
	"log"

	"github.com/despreston/go-craq/node"
	"github.com/despreston/go-craq/store/kv"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "1235", "port")
	flag.Parse()

	opts := node.Opts{
		Path:      "127.0.0.1:" + port,
		CdrPath:   "127.0.0.1:1234",
		Store:     kv.New(),
		Transport: &netrpc.Client{},
	}

	log.Fatal(node.New(opts).ListenAndServe())
}
