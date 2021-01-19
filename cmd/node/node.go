package main

import (
	"flag"
	"log"

	"github.com/despreston/go-craq/node"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "1235", "port")
	flag.Parse()

	n := node.Node{
		Path:    "127.0.0.1:" + port,
		CdrPath: "127.0.0.1:1234",
	}

	log.Fatal(n.ListenAndServe())
}
