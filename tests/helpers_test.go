package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

// newTestNode creates a node and makes sure it is stopped when the test finishes,
// so the next test can reuse the same ports.
func newTestNode(t *testing.T, id string, address string, peers []string, dataDir string) *node.Node {
	n := node.NewNode(id, address)
	n.Peers = peers
	n.DataDir = dataDir

	t.Cleanup(n.Stop)
	// t.Cleanup runs the function after the test ends, even if the test fails.

	return n
}

// startCluster starts `size` nodes on ports basePort, basePort+1, ...
func startCluster(t *testing.T, basePort int, size int, dataDir string) []*node.Node {
	addresses := make([]string, size)
	for i := range addresses {
		addresses[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	nodes := make([]*node.Node, size)

	for i, address := range addresses {
		nodes[i] = newTestNode(t, fmt.Sprintf("node%d", i+1), address, peersOf(addresses, address), dataDir)

		err := nodes[i].Start()
		if err != nil {
			t.Fatalf("failed to start %s: %v", address, err)
		}
	}

	return nodes
}

// peersOf returns every address except self.
func peersOf(addresses []string, self string) []string {
	var peers []string

	for _, address := range addresses {
		if address != self {
			peers = append(peers, address)
		}
	}

	return peers
}

// addressesOf returns the addresses of the given nodes.
func addressesOf(nodes []*node.Node) []string {
	addresses := make([]string, len(nodes))
	for i, n := range nodes {
		addresses[i] = n.Address
	}

	return addresses
}

// waitForLeader waits until exactly one of the nodes is leader, and returns it.
func waitForLeader(t *testing.T, nodes []*node.Node, timeout time.Duration) *node.Node {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leaders []*node.Node

		for _, n := range nodes {
			if n.State() == node.Leader {
				leaders = append(leaders, n)
			}
		}

		if len(leaders) == 1 {
			return leaders[0]
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("no single leader elected within %v", timeout)
	return nil
}

// waitFor keeps checking condition until it returns true, or fails the test after timeout.
func waitFor(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for: %s", description)
}

// hasValue reports whether a node's local store holds key = value.
func hasValue(n *node.Node, key string, value string) bool {
	got, found := n.Store.Get(key)
	return found && got == value
}
