package main

import (
	"log"

	"github.com/despreston/go-craq/node"
)

func main() {
	n := node.Node{
		Path:    "127.0.0.1:1235",
		CdrPath: "127.0.0.1:1234",
	}
	log.Fatal(n.ListenAndServe())
}
