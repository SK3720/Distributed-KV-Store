package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

func TestLogReplication(t *testing.T) {
	fmt.Println("\n=== Raft Log Replication ===")

	nodes := startCluster(t, 9201, 3, "")
	waitForLeader(t, nodes, 5*time.Second)

	client := node.NewClusterClient(addressesOf(nodes))

	// Write 10 keys, then delete the first one.
	for i := 0; i < 10; i++ {
		err := client.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		if err != nil {
			t.Fatalf("PUT key-%d failed: %v", i, err)
		}
	}

	err := client.Delete("key-0")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	fmt.Println("✓ 10 PUTs and 1 DELETE committed")

	// Every node should end up with exactly the same data.
	for _, n := range nodes {
		waitFor(t, 2*time.Second, n.ID+" matches the leader", func() bool {
			if _, found := n.Store.Get("key-0"); found {
				return false
			}

			for i := 1; i < 10; i++ {
				if !hasValue(n, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)) {
					return false
				}
			}

			return true
		})

		fmt.Printf("✓ %s has every entry\n", n.ID)
	}
}

func TestFollowerRedirect(t *testing.T) {
	fmt.Println("\n=== Follower Redirects Clients to the Leader ===")

	nodes := startCluster(t, 9211, 3, "")
	leader := waitForLeader(t, nodes, 5*time.Second)

	var follower *node.Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	// Wait until the follower has received a heartbeat and knows who the leader is.
	waitFor(t, 2*time.Second, "follower learns the leader", func() bool {
		return follower.LeaderAddress() == leader.Address
	})

	err := node.NewClient(follower.Address).Put("name", "Shiv")

	var notLeader *node.NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("expected a NotLeaderError from the follower, got %v", err)
	}

	if notLeader.LeaderAddress != leader.Address {
		t.Fatalf("expected redirect to %s, got %q", leader.Address, notLeader.LeaderAddress)
	}

	fmt.Printf("✓ %s redirected the client to %s\n", follower.ID, notLeader.LeaderAddress)
}

func TestLeaderFailover(t *testing.T) {
	fmt.Println("\n=== Leader Failover ===")

	nodes := startCluster(t, 9221, 3, "")
	oldLeader := waitForLeader(t, nodes, 5*time.Second)

	client := node.NewClusterClient(addressesOf(nodes))

	err := client.Put("before", "1")
	if err != nil {
		t.Fatalf("PUT before failover failed: %v", err)
	}

	fmt.Printf("Stopping leader %s...\n", oldLeader.ID)
	oldLeader.Stop()

	var remaining []*node.Node
	for _, n := range nodes {
		if n != oldLeader {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 5*time.Second)
	fmt.Printf("✓ New leader elected → %s\n", newLeader.ID)

	err = client.Put("after", "2")
	if err != nil {
		t.Fatalf("PUT after failover failed: %v", err)
	}

	// The new leader must still have the entry committed by the old leader.
	value, found, err := client.Get("before")
	if err != nil || !found || value != "1" {
		t.Fatalf("expected before = 1, got %q (found=%v, err=%v)", value, found, err)
	}

	value, found, err = client.Get("after")
	if err != nil || !found || value != "2" {
		t.Fatalf("expected after = 2, got %q (found=%v, err=%v)", value, found, err)
	}

	fmt.Println("✓ Committed data survived the failover")
	fmt.Println("✓ Cluster keeps accepting writes with 2 of 3 nodes")
}

func TestFollowerCatchUp(t *testing.T) {
	fmt.Println("\n=== Restarted Follower Catches Up ===")

	dataDir := t.TempDir()
	// t.TempDir gives each test its own empty folder, deleted automatically afterwards.

	nodes := startCluster(t, 9231, 3, dataDir)
	leader := waitForLeader(t, nodes, 5*time.Second)

	client := node.NewClusterClient(addressesOf(nodes))

	err := client.Put("first", "1")
	if err != nil {
		t.Fatalf("PUT first failed: %v", err)
	}

	var follower *node.Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	waitFor(t, 2*time.Second, "follower has first", func() bool {
		return hasValue(follower, "first", "1")
	})

	fmt.Printf("Stopping follower %s...\n", follower.ID)
	follower.Stop()

	// These writes happen while the follower is down.
	for i := 0; i < 5; i++ {
		err := client.Put(fmt.Sprintf("missed-%d", i), "x")
		if err != nil {
			t.Fatalf("PUT while follower is down failed: %v", err)
		}
	}

	fmt.Printf("Restarting follower %s...\n", follower.ID)
	restarted := newTestNode(t, follower.ID, follower.Address, follower.Peers, dataDir)

	err = restarted.Start()
	if err != nil {
		t.Fatalf("failed to restart follower: %v", err)
	}

	waitFor(t, 3*time.Second, "restarted follower catches up", func() bool {
		if !hasValue(restarted, "first", "1") {
			return false
		}

		for i := 0; i < 5; i++ {
			if !hasValue(restarted, fmt.Sprintf("missed-%d", i), "x") {
				return false
			}
		}

		return true
	})

	fmt.Println("✓ Restarted follower received every entry it missed")
}

func TestPersistenceAfterRestart(t *testing.T) {
	fmt.Println("\n=== State Survives a Restart ===")

	dataDir := t.TempDir()

	first := newTestNode(t, "node1", "localhost:9241", nil, dataDir)

	err := first.Start()
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	waitForLeader(t, []*node.Node{first}, 5*time.Second)

	err = first.Put("name", "Shiv")
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}

	termBefore := first.Term()
	first.Stop()

	fmt.Println("Node stopped, starting a new one from the same data folder...")

	second := newTestNode(t, "node1", "localhost:9241", nil, dataDir)

	err = second.Start()
	if err != nil {
		t.Fatalf("failed to restart node: %v", err)
	}

	waitForLeader(t, []*node.Node{second}, 5*time.Second)

	if second.Term() <= termBefore {
		t.Fatalf("expected a term newer than %d after restart, got %d", termBefore, second.Term())
	}

	waitFor(t, 2*time.Second, "value restored from the log", func() bool {
		value, found, err := second.Get("name")
		return err == nil && found && value == "Shiv"
	})

	fmt.Println("✓ Term and log were loaded from disk")
	fmt.Println("✓ KV store was rebuilt by replaying the log")
}

func TestNoCommitWithoutMajority(t *testing.T) {
	fmt.Println("\n=== No Commit Without a Majority ===")

	nodes := startCluster(t, 9251, 3, "")
	leader := waitForLeader(t, nodes, 5*time.Second)

	// Commit one write first, so the leader is fully set up before we cut it off.
	err := leader.Put("ready", "1")
	if err != nil {
		t.Fatalf("first PUT failed: %v", err)
	}

	// Stop both followers, leaving the leader alone.
	for _, n := range nodes {
		if n != leader {
			n.Stop()
		}
	}

	fmt.Println("Both followers stopped, writing to the lone leader...")

	err = leader.Put("lost", "x")
	if !errors.Is(err, node.ErrCommitTimeout) {
		t.Fatalf("expected ErrCommitTimeout, got %v", err)
	}

	if _, found := leader.Store.Get("lost"); found {
		t.Fatal("an uncommitted entry was applied to the store")
	}

	// The leader also can't prove it's still leader, so it must refuse reads.
	_, _, err = leader.Get("lost")
	if !errors.Is(err, node.ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}

	fmt.Println("✓ Write was not committed without a majority")
	fmt.Println("✓ Lone leader refused to serve a possibly stale read")
}
