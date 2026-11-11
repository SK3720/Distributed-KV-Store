package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestThreeNodeCluster(t *testing.T) {
	fmt.Println("\n=== 3-Node KV Store Cluster ===")

	fmt.Println("Starting Node 1 on localhost:9001")
	fmt.Println("Starting Node 2 on localhost:9002")
	fmt.Println("Starting Node 3 on localhost:9003")

	nodes := startCluster(t, 9001, 3, "")

	leader := waitForLeader(t, nodes, 5*time.Second)
	fmt.Printf("Leader elected → %s\n", leader.ID)

	// The cluster client finds the leader on its own.
	client := node.NewClusterClient(addressesOf(nodes))

	fmt.Println("\n--- PUT ---")
	fmt.Println("Cluster ← PUT name = Shiv")

	err := client.Put("name", "Shiv")
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	fmt.Println("✓ PUT committed by a majority")

	fmt.Println("\n--- GET ---")
	value, found, err := client.Get("name")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	fmt.Printf("Leader → GET name = %q\n", value)

	if !found || value != "Shiv" {
		t.Fatalf("leader returned unexpected value: %s", value)
	}

	fmt.Println("\n--- Checking Every Node's Store ---")

	for _, n := range nodes {
		waitFor(t, 2*time.Second, n.ID+" has name = Shiv", func() bool {
			return hasValue(n, "name", "Shiv")
		})

		fmt.Printf("%s → name = \"Shiv\"\n", n.ID)
	}

	fmt.Println("\n✓ All three nodes are running")
	fmt.Println("✓ KV stores are replicated through Raft")
}

func TestRaftElectionTimeout(t *testing.T) {
	fmt.Println("\n=== Raft Single-Node Election ===")

	server := newTestNode(t, "node1", "localhost:9010", nil, "")

	fmt.Println("Starting Node 1...")
	go server.StartRaft()

	fmt.Println("Waiting for election timeout...")

	// Wait for the election timeout to expire.
	time.Sleep(2200 * time.Millisecond)

	fmt.Printf("Node 1 state → %s\n", server.State())

	if server.State() != node.Leader {
		t.Fatalf("expected node to become leader, got %v", server.State())
	}

	fmt.Println("✓ Node 1 became LEADER")
}

func TestThreeNodeLeaderElection(t *testing.T) {
	fmt.Println("\n=== 3-Node Raft Leader Election ===")

	// Configure each node's peers.
	node1 := newTestNode(t, "node1", "localhost:9101", []string{"localhost:9102", "localhost:9103"}, "")
	node2 := newTestNode(t, "node2", "localhost:9102", []string{"localhost:9101", "localhost:9103"}, "")
	node3 := newTestNode(t, "node3", "localhost:9103", []string{"localhost:9101", "localhost:9102"}, "")

	fmt.Println("Cluster configuration:")
	fmt.Println("  Node 1 → localhost:9101")
	fmt.Println("  Node 2 → localhost:9102")
	fmt.Println("  Node 3 → localhost:9103")

	// Start RPC servers.
	fmt.Println("\nStarting RPC servers...")

	go node1.StartRPC()
	go node2.StartRPC()
	go node3.StartRPC()

	// Start Raft election loops.
	fmt.Println("Starting Raft election loops...")

	go node1.StartRaft()
	go node2.StartRaft()
	go node3.StartRaft()

	fmt.Println("Waiting for leader election...")

	// Give the cluster time to elect a leader.
	time.Sleep(2500 * time.Millisecond)

	nodes := []*node.Node{
		node1,
		node2,
		node3,
	}

	fmt.Println("\n--- Final Cluster State ---")

	leaderCount := 0

	for _, n := range nodes {
		fmt.Printf("%s → %s\n", n.ID, n.State())

		if n.State() == node.Leader {
			leaderCount++
		}
	}

	fmt.Printf("\nLeaders elected: %d\n", leaderCount)

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}

	fmt.Println("✓ Exactly one leader elected")
	fmt.Println("✓ Majority-based leader election successful")
}
