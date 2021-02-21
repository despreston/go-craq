package main

import (
	"flag"
	"log"
	"os"

	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var cdr, node string

	flag.StringVar(&cdr, "c", "0.0.0.0:1234", "coordinator address")
	flag.StringVar(&node, "n", "0.0.0.0:1235", "node address to read from")
	flag.Parse()

	if len(os.Args) < 2 {
		log.Fatal("No command given.")
	}

	if len(os.Args) < 3 {
		log.Fatal("No key given.")
	}

	cmd := os.Args[1]
	key := os.Args[2]

	switch cmd {
	case "write":
		if len(os.Args) < 4 {
			log.Fatal("No value given.")
		}
		val := os.Args[3]
		c := netrpc.NewCoordinatorClient()
		c.Connect(cdr)
		log.Println(c.Write(key, []byte(val)))
	case "read":
		n := netrpc.NewNodeClient()
		n.Connect(node)
		k, v, err := n.Read(key)
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("key: %s, value: %s", k, string(v))
	}
}
