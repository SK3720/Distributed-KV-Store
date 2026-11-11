package main

// A small command-line client for the cluster. It finds the leader on its own.
//
//	go run ./cmd/client put name Shiv
//	go run ./cmd/client get name
//	go run ./cmd/client delete name
//	go run ./cmd/client -nodes localhost:9001,localhost:9002 get name

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func main() {
	nodes := flag.String("nodes", "localhost:9001,localhost:9002,localhost:9003", "comma-separated node addresses")
	flag.Parse()

	args := flag.Args()
	// flag.Args() is whatever is left after the flags, e.g. ["put", "name", "Shiv"].
	if len(args) == 0 {
		usage()
	}

	client := node.NewClusterClient(strings.Split(*nodes, ","))

	switch strings.ToLower(args[0]) {

	case "put":
		if len(args) != 3 {
			usage()
		}

		err := client.Put(args[1], args[2])
		if err != nil {
			fail(err)
		}

		fmt.Println("OK")

	case "get":
		if len(args) != 2 {
			usage()
		}

		value, found, err := client.Get(args[1])
		if err != nil {
			fail(err)
		}

		if !found {
			fmt.Println("<not found>")
			return
		}

		fmt.Println(value)

	case "delete":
		if len(args) != 2 {
			usage()
		}

		err := client.Delete(args[1])
		if err != nil {
			fail(err)
		}

		fmt.Println("OK")

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: client [-nodes a,b,c] put <key> <value> | get <key> | delete <key>")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
