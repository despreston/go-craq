package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/despreston/go-craq/client"
	"github.com/despreston/go-craq/transport"
)

func main() {
	var cdr, node string

	flag.StringVar(&cdr, "c", "localhost:1234", "coordinator address")
	flag.StringVar(&node, "n", "localhost:1235", "node address to read from")
	flag.Parse()

	if len(os.Args) < 2 {
		log.Fatal("No command given.")
	}

	if len(os.Args) < 3 {
		log.Fatal("No key given.")
	}

	cmd := os.Args[1]

	fmt.Printf("cmd: %s, coordinator: %s\n", cmd, cdr)

	c, err := client.New(cdr, node, &transport.NetRPC{})
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
