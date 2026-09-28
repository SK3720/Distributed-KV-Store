package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestThreeNodeCluster(t *testing.T) {
	fmt.Println("\n=== 3-Node KV Store Cluster ===")

	node1 := node.NewNode("node1", "localhost:9001")
	node2 := node.NewNode("node2", "localhost:9002")
	node3 := node.NewNode("node3", "localhost:9003")

	fmt.Println("Starting Node 1 on localhost:9001")
	fmt.Println("Starting Node 2 on localhost:9002")
	fmt.Println("Starting Node 3 on localhost:9003")

	go node1.StartRPC()
	go node2.StartRPC()
	go node3.StartRPC()

	time.Sleep(100 * time.Millisecond)

	// Create clients for each node.
	client1 := node.NewClient("localhost:9001")
	client2 := node.NewClient("localhost:9002")
	client3 := node.NewClient("localhost:9003")

	fmt.Println("\n--- PUT ---")
	fmt.Println("Node 1 ← PUT name = Shiv")

	err := client1.Put("name", "Shiv")
	if err != nil {
		t.Fatalf("Node 1 PUT failed: %v", err)
	}

	fmt.Println("✓ PUT successful")

	fmt.Println("\n--- GET ---")
	value, found, err := client1.Get("name")
	if err != nil {
		t.Fatalf("Node 1 GET failed: %v", err)
	}

	fmt.Printf("Node 1 → GET name = %q\n", value)

	if !found || value != "Shiv" {
		t.Fatalf("Node 1 returned unexpected value: %s", value)
	}

	fmt.Println("✓ Node 1 has the value")

	fmt.Println("\n--- Checking Other Nodes ---")

	_, found, err = client2.Get("name")
	if err != nil {
		t.Fatalf("Node 2 GET failed: %v", err)
	}

	if found {
		t.Fatal("Node 2 should not have the value yet")
	}

	fmt.Println("Node 2 → GET name = <not found>")

	_, found, err = client3.Get("name")
	if err != nil {
		t.Fatalf("Node 3 GET failed: %v", err)
	}

	if found {
		t.Fatal("Node 3 should not have the value yet")
	}

	fmt.Println("Node 3 → GET name = <not found>")

	fmt.Println("\n✓ All three nodes are running")
	fmt.Println("✓ KV stores are currently independent")
}

func TestRaftElectionTimeout(t *testing.T) {
	fmt.Println("\n=== Raft Single-Node Election ===")

	server := node.NewNode("node1", "localhost:9010")

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

	node1 := node.NewNode("node1", "localhost:9101")
	node2 := node.NewNode("node2", "localhost:9102")
	node3 := node.NewNode("node3", "localhost:9103")

	// Configure each node's peers.
	node1.Peers = []string{
		"localhost:9102",
		"localhost:9103",
	}

	node2.Peers = []string{
		"localhost:9101",
		"localhost:9103",
	}

	node3.Peers = []string{
		"localhost:9101",
		"localhost:9102",
	}

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
