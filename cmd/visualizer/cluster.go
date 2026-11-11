package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

// cluster runs every node in this one process, wired together through relays.
type cluster struct {
	// mu protects nodes and alive, which change when a node is stopped or restarted.
	mu    sync.Mutex
	nodes map[string]*node.Node
	alive map[string]bool

	// These are set once at startup and never change, so they don't need the lock.
	ids         []string
	addresses   map[string]string // node ID → real address
	idByAddress map[string]string // real address → node ID
	peers       map[string][]string
	links       map[[2]string]*link // [from, to] → relay
	dataDir     string

	delayMS       atomic.Int64
	nextMessageID atomic.Int64

	hub     *hub
	tracker *tracker
}

func newCluster(count int, basePort int, dataDir string) (*cluster, error) {
	c := &cluster{
		nodes:       make(map[string]*node.Node),
		alive:       make(map[string]bool),
		addresses:   make(map[string]string),
		idByAddress: make(map[string]string),
		peers:       make(map[string][]string),
		links:       make(map[[2]string]*link),
		dataDir:     dataDir,
		hub:         newHub(),
		tracker:     newTracker(),
	}

	for i := 1; i <= count; i++ {
		id := fmt.Sprintf("N%d", i)
		address := fmt.Sprintf("localhost:%d", basePort+i-1)

		c.ids = append(c.ids, id)
		c.addresses[id] = address
		c.idByAddress[address] = id
	}

	// Create one relay per direction for every pair of nodes.
	for _, from := range c.ids {
		for _, to := range c.ids {
			if from == to {
				continue
			}

			listener, err := net.Listen("tcp", "localhost:0")
			// Port 0 asks the operating system for any free port.
			if err != nil {
				c.closeLinks()
				return nil, err
			}

			l := &link{from: from, to: to, listener: listener}
			c.links[[2]string{from, to}] = l

			// The node talks to the relay instead of directly to its peer.
			c.peers[from] = append(c.peers[from], listener.Addr().String())

			go c.serveLink(l)
		}
	}

	return c, nil
}

// start starts every node.
func (c *cluster) start() error {
	for _, id := range c.ids {
		err := c.startNode(id)
		if err != nil {
			return err
		}
	}

	return nil
}

// stop shuts down every node and relay.
func (c *cluster) stop() {
	for _, id := range c.ids {
		c.stopNode(id)
	}

	c.closeLinks()
}

func (c *cluster) closeLinks() {
	for _, l := range c.links {
		l.listener.Close()
	}
}

// startNode starts (or restarts) a node. A restarted node is a brand new Node that
// loads its saved state from the data folder, just like a real process restarting after a crash.
func (c *cluster) startNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.alive[id] {
		return nil
	}

	n := node.NewNode(id, c.addresses[id])
	n.Peers = c.peers[id]
	n.DataDir = c.dataDir

	err := n.Start()
	if err != nil {
		return err
	}

	c.nodes[id] = n
	c.alive[id] = true

	return nil
}

// stopNode stops a node, like a crash.
func (c *cluster) stopNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.alive[id] {
		return
	}

	c.nodes[id].Stop()
	c.alive[id] = false
}

// setLinkCut cuts (or heals) the connection between two nodes, in both directions.
func (c *cluster) setLinkCut(a string, b string, cut bool) error {
	forward, ok1 := c.links[[2]string{a, b}]
	backward, ok2 := c.links[[2]string{b, a}]
	if !ok1 || !ok2 {
		return fmt.Errorf("no link between %q and %q", a, b)
	}

	forward.cut.Store(cut)
	backward.cut.Store(cut)

	return nil
}

// heal restores every link and restarts every stopped node.
func (c *cluster) heal() error {
	for _, l := range c.links {
		l.cut.Store(false)
	}

	return c.start()
}

// leaderClient returns a client for the current leader (the running leader with the newest term),
// or nil if there isn't one. A node cut off from the cluster may still think it's the leader,
// so picking the newest term makes the page's writes go to the real one.
func (c *cluster) leaderClient() *node.Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	address := ""
	bestTerm := -1

	for id, n := range c.nodes {
		if c.alive[id] && n.State() == node.Leader && n.Term() > bestTerm {
			address = c.addresses[id]
			bestTerm = n.Term()
		}
	}

	if address == "" {
		return nil
	}

	return node.NewClient(address)
}

// run sends an operation to the current leader, retrying (for example during an election)
// until it works or 6 seconds pass.
func (c *cluster) run(operation func(client *node.Client) error) error {
	deadline := time.Now().Add(6 * time.Second)
	err := errors.New("no leader elected yet")

	for time.Now().Before(deadline) {
		if leader := c.leaderClient(); leader != nil {
			err = operation(leader)
			if err == nil {
				return nil
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func (c *cluster) isKnownNode(id string) bool {
	_, ok := c.addresses[id]
	return ok
}

// nodeView is one node's state as sent to the browser.
type nodeView struct {
	ID          string            `json:"id"`
	Address     string            `json:"address"`
	Alive       bool              `json:"alive"`
	State       string            `json:"state"`
	Term        int               `json:"term"`
	CommitIndex int               `json:"commit_index"`
	Leader      string            `json:"leader"`
	LogLength   int               `json:"log_length"`
	Log         []logView         `json:"log"`
	Store       map[string]string `json:"store"`
}

type linkView struct {
	From string `json:"from"`
	To   string `json:"to"`
	Cut  bool   `json:"cut"`
}

type snapshot struct {
	Type    string     `json:"type"` // always "snapshot"
	Nodes   []nodeView `json:"nodes"`
	Links   []linkView `json:"links"`
	DelayMS int64      `json:"delay_ms"`
}

// snapshot reads the current state of every node using only the node's public methods.
func (c *cluster) snapshot() snapshot {
	keys := c.tracker.sortedKeys()

	c.mu.Lock()
	defer c.mu.Unlock()

	result := snapshot{Type: "snapshot", DelayMS: c.delayMS.Load()}

	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}

		view := nodeView{
			ID:          id,
			Address:     c.addresses[id],
			Alive:       c.alive[id],
			State:       string(n.State()),
			Term:        n.Term(),
			CommitIndex: n.CommitIndex(),
			Leader:      c.idByAddress[n.LeaderAddress()],
			Store:       make(map[string]string),
		}

		if !view.Alive {
			view.State = "stopped"
		}

		view.LogLength, view.Log = c.tracker.tail(id, 32)

		for _, key := range keys {
			if value, found := n.Store.Get(key); found {
				view.Store[key] = value
			}
		}

		result.Nodes = append(result.Nodes, view)
	}

	for key, l := range c.links {
		result.Links = append(result.Links, linkView{From: key[0], To: key[1], Cut: l.cut.Load()})
	}

	return result
}

// publishSnapshots sends the cluster state to the browser 10 times a second.
func (c *cluster) publishSnapshots() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		c.hub.publish(c.snapshot())
	}
}

// put saves a key-value pair through the current leader.
func (c *cluster) put(key string, value string) error {
	return c.run(func(client *node.Client) error {
		return client.Put(key, value)
	})
}
