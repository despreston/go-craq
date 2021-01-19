package main

import (
	"log"

	"github.com/despreston/go-craq/coordinator"
)

func main() {
	cdr := coordinator.Coordinator{Path: "127.0.0.1:1234"}
	log.Fatal(cdr.ListenAndServe())
}
