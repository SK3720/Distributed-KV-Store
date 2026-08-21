package node

import "github.com/SK3720/Distributed-KV-Store/internal/kv"

type Node struct {
	ID      string
	Address string
	Store   *kv.Store
	running bool
	// Here, we store the pointer to the Store struct since the constructor of the Store struct returns a pointer to the Store struct.
	// This is also a convention of Struct composition in Go, where we embed a pointer to another struct within a struct to achieve composition and reuse of functionality.
}

func NewNode(id string, address string) *Node {
	return &Node{
		ID:      id,
		Address: address,
		Store:   kv.NewStore(),
	}
}

func (n *Node) Start() {
	n.running = true
}

func (n *Node) Stop() {
	n.running = false
}
