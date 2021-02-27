package main

import (
	"flag"
	"log"
	"strings"

	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var cdr, node string

	flag.StringVar(&cdr, "c", ":1234", "coordinator address")
	flag.StringVar(&node, "n", ":1235", "node address to read from")
	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		log.Fatal("No command given.")
	}

	cmd := args[0]

	if cmd == "readall" {
		n := netrpc.NewNodeClient()

		if err := n.Connect(node); err != nil {
			log.Fatalf("Failed to connect to node\n  %#v", err)
		}

		items, err := n.ReadAll()
		if err != nil {
			log.Fatal(err.Error())
		}

		for _, item := range *items {
			log.Printf("key: %s, value: %s", item.Key, string(item.Value))
		}

		return
	}

	if len(args) < 2 {
		log.Fatal("No key given.")
	}

	key := args[1]

	switch cmd {
	case "write":
		if len(args) < 3 {
			log.Fatal("No value given.")
		}
		val := strings.Join(args[2:], " ")
		c := netrpc.NewCoordinatorClient()
		c.Connect(cdr)
		log.Println(c.Write(key, []byte(val)))
	case "read":
		n := netrpc.NewNodeClient()

		if err := n.Connect(node); err != nil {
			log.Fatalf("Failed to connect to node\n  %#v", err)
		}

		k, v, err := n.Read(key)
		if err != nil {
			log.Fatal(err.Error())
		}

		log.Printf("key: %s, value: %s", k, string(v))
	}
}
