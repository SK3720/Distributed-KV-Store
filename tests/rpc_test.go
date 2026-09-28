package tests

import (
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestRPCClient(t *testing.T) {
	server := node.NewNode("node1", "localhost:9001")

	go server.StartRPC()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	client := node.NewClient("localhost:9001")

	// PUT
	err := client.Put("name", "Shiv")
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	// GET
	value, found, err := client.Get("name")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	if !found {
		t.Fatal("expected key to be found")
	}

	if value != "Shiv" {
		t.Fatalf("expected value 'Shiv', got '%s'", value)
	}

	// DELETE
	err = client.Delete("name")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// GET after DELETE
	_, found, err = client.Get("name")
	if err != nil {
		t.Fatalf("GET after DELETE failed: %v", err)
	}

	if found {
		t.Fatal("expected key to be deleted")
	}
}
