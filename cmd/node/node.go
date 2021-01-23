package main

import (
	"flag"
	"log"

	"github.com/despreston/go-craq/kv"
	"github.com/despreston/go-craq/node"
)

//-----------------------------------------------------------------------------
// Fake store for temporary testing
// type store struct{}

// func (store) Read(key string) (*node.Object, bool) {
// 	return &node.Object{}, true
// }

// func (store) Write(key string, value []byte, version uint64) error {
// 	log.Printf("key %s, value: %s, version: %d\n", key, string(value), version)
// 	return nil
// }

// func (store) MarkClean(key string, version uint64) error {
// 	log.Printf("marking clean key: %s, version: %d\n", key, version)
// 	return nil
// }

func main() {
	var port string
	flag.StringVar(&port, "port", "1235", "port")
	flag.Parse()

	store := kv.New()

	opts := node.Opts{
		Path:    "127.0.0.1:" + port,
		CdrPath: "127.0.0.1:1234",
		Store:   store,
	}

	log.Fatal(node.New(opts).ListenAndServe())
}
