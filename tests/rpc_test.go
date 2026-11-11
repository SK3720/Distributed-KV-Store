package tests

import (
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestRPCClient(t *testing.T) {
	server := newTestNode(t, "node1", "localhost:9020", nil, "")

	err := server.Start()
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	// Writes now go through Raft, so wait for the single node to elect itself leader.
	waitForLeader(t, []*node.Node{server}, 5*time.Second)

	client := node.NewClient("localhost:9020")

	// The leader can't answer reads until its first (NOOP) entry is committed,
	// so the first GET may briefly fail. Wait until reads are ready.
	waitFor(t, 2*time.Second, "leader ready for reads", func() bool {
		_, _, err := client.Get("anything")
		return err == nil
	})

	// PUT
	err = client.Put("name", "Shiv")
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
