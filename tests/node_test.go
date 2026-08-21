package tests

import (
	"testing"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestNode(t *testing.T) {
	n := node.NewNode("A", "localhost:8001")

	if n.ID != "A" {
		t.Fatal("incorrect node ID")
	}

	if n.Address != "localhost:8001" {
		t.Fatal("incorrect node address")
	}

	n.Store.Put("name", "Shiv")

	value, ok := n.Store.Get("name")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Shiv" {
		t.Fatal("incorrect value stored")
	}
}
