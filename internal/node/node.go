package node

import (
	"math/rand"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/kv"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

type Node struct {
	ID      string
	Address string
	Store   *kv.Store
	running bool

	// Raft state
	currentTerm int
	votedFor    string
	state       NodeState

	// Election timing
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	// Other nodes in the cluster
	Peers []string
}

func NewNode(id string, address string) *Node {
	return &Node{
		ID:              id,
		Address:         address,
		Store:           kv.NewStore(),
		running:         false,
		state:           Follower,
		electionTimeout: time.Duration(1000+rand.Intn(1000)) * time.Millisecond,
		lastHeartbeat:   time.Now(),
	}
}
