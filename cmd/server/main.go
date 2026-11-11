package main

// Runs a single node of the KV store cluster.
//
// Example 3-node cluster (one command per terminal):
//
//	go run ./cmd/server -id node1 -addr localhost:9001 -peers localhost:9002,localhost:9003 -data ./data
//	go run ./cmd/server -id node2 -addr localhost:9002 -peers localhost:9001,localhost:9003 -data ./data
//	go run ./cmd/server -id node3 -addr localhost:9003 -peers localhost:9001,localhost:9002 -data ./data

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func main() {
	id := flag.String("id", "", "unique ID for this node (required)")
	address := flag.String("addr", "localhost:9001", "address this node listens on")
	peers := flag.String("peers", "", "comma-separated addresses of the other nodes")
	dataDir := flag.String("data", "", "folder to save Raft state in (empty = memory only)")
	flag.Parse()
	// flag.Parse reads the command-line arguments into the variables above.

	if *id == "" {
		log.Fatal("-id is required")
	}

	n := node.NewNode(*id, *address)
	n.Peers = splitList(*peers)
	n.DataDir = *dataDir
	n.Verbose = true

	err := n.Start()
	if err != nil {
		log.Fatalf("failed to start node: %v", err)
	}

	log.Printf("[%s] listening on %s, peers: %v", *id, *address, n.Peers)

	// Wait for Ctrl+C (or a kill signal), then shut down cleanly.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	log.Printf("[%s] shutting down", *id)
	n.Stop()
}

// splitList turns "a,b,c" into ["a", "b", "c"], ignoring empty items.
func splitList(value string) []string {
	var items []string

	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			items = append(items, item)
		}
	}

	return items
}
