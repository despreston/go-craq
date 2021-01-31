package main

import (
	"flag"
	"log"

	"github.com/despreston/go-craq/kv"
	"github.com/despreston/go-craq/node"
	"github.com/despreston/go-craq/transport"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "1235", "port")
	flag.Parse()

	opts := node.Opts{
		Path:      "127.0.0.1:" + port,
		CdrPath:   "127.0.0.1:1234",
		Store:     kv.New(),
		Transport: &transport.NetRPC{},
	}

	log.Fatal(node.New(opts).ListenAndServe())
}
