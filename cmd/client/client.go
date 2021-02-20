package main

import (
	"flag"
	"log"
	"os"

	"github.com/despreston/go-craq/client"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var cdr, node string

	flag.StringVar(&cdr, "c", "192.168.0.30:1234", "coordinator address")
	flag.StringVar(&node, "n", "192.168.0.30:1236", "node address to read from")
	flag.Parse()

	if len(os.Args) < 2 {
		log.Fatal("No command given.")
	}

	if len(os.Args) < 3 {
		log.Fatal("No key given.")
	}

	cmd := os.Args[1]

	c, err := client.New(cdr, node, &netrpc.Client{})
	if err != nil {
		log.Fatal(err)
	}

	key := os.Args[2]

	switch cmd {
	case "write":
		if len(os.Args) < 4 {
			log.Fatal("No value given.")
		}
		val := os.Args[3]
		log.Println(c.Write(key, []byte(val)))
	case "read":
		log.Println(c.Read(key))
	}
}
