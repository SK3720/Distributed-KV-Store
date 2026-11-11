package node

import (
	"errors"
	"sync"
	"time"
)

// ClusterClient talks to a whole cluster instead of a single node.
// It finds the leader on its own: if it reaches a follower it follows the "not leader" hint,
// and if a node is down it moves on to the next one.
type ClusterClient struct {
	Addresses []string

	// How long to keep retrying an operation before giving up.
	Timeout time.Duration

	mu     sync.Mutex
	leader string // last known leader address
	next   int    // next address to try when we don't know the leader
}

// NewClusterClient creates a client for the given node addresses.
func NewClusterClient(addresses []string) *ClusterClient {
	return &ClusterClient{
		Addresses: addresses,
		Timeout:   10 * time.Second,
	}
}

// Put stores a key-value pair in the cluster.
// PUT is safe to retry: writing the same value twice gives the same result.
func (c *ClusterClient) Put(key string, value string) error {
	return c.do(func(client *Client) error {
		return client.Put(key, value)
	})
}

// Delete removes a key from the cluster. Like PUT, it is safe to retry.
func (c *ClusterClient) Delete(key string) error {
	return c.do(func(client *Client) error {
		return client.Delete(key)
	})
}

// Get reads a value from the cluster's leader.
func (c *ClusterClient) Get(key string) (string, bool, error) {
	var value string
	var found bool

	err := c.do(func(client *Client) error {
		var err error
		value, found, err = client.Get(key)
		return err
	})

	return value, found, err
}

// do runs an operation against the leader, retrying on other nodes until it works or we run out of time.
func (c *ClusterClient) do(operation func(client *Client) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Addresses) == 0 {
		return errors.New("no node addresses configured")
	}

	deadline := time.Now().Add(c.Timeout)

	for {
		target := c.leader
		if target == "" {
			target = c.Addresses[c.next%len(c.Addresses)]
			c.next++
		}

		err := operation(NewClient(target))
		if err == nil {
			c.leader = target
			return nil
		}

		// If the node told us who the leader is, go there next.
		// Otherwise forget the leader and try the next address.
		var notLeader *NotLeaderError
		if errors.As(err, &notLeader) && notLeader.LeaderAddress != "" {
			c.leader = notLeader.LeaderAddress
		} else {
			c.leader = ""
		}

		if time.Now().After(deadline) {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}
