package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/despreston/go-craq/client"
)

func main() {
	var cdrPort, nodePort, port string

	flag.StringVar(&cdrPort, "c", "1234", "coordinator port")
	flag.StringVar(&port, "p", "3000", "port")
	flag.StringVar(&nodePort, "n", "1235", "port")
	flag.Parse()

	if len(os.Args) < 2 {
		log.Fatal("No command given.")
	}

	if len(os.Args) < 3 {
		log.Fatal("No key given.")
	}

	cmd := os.Args[1]

	fmt.Printf("cmd: %s, port: %s, coordinator: %s\n", cmd, port, cdrPort)

	c, err := client.New(cdrPort, nodePort, port)
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
