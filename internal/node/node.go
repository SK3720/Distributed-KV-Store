package node

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/kv"
)

type NodeState string

const (
	Follower  NodeState = "follower"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)

// Raft timing values.
const (
	// The leader sends a heartbeat this often.
	// It must be much smaller than the election timeout, otherwise followers would
	// start elections even though a healthy leader exists.
	heartbeatInterval = 100 * time.Millisecond

	// Each node picks a random election timeout in this range.
	// The randomness makes it unlikely that two nodes start an election at the same moment (a "split vote").
	minElectionTimeout = 1000 * time.Millisecond
	maxElectionTimeout = 2000 * time.Millisecond

	// The leader only answers reads while a majority has heard from it within this window.
	// It is a little shorter than minElectionTimeout to leave a safety margin for clock drift.
	leaseDuration = 900 * time.Millisecond

	// How long a client write waits to be committed before we give up and return an error.
	commitTimeout = 3 * time.Second
)

type Node struct {
	// mu protects all of the Raft state below.
	// Many goroutines use the node at once (RPC handlers, the election loop, the apply loop),
	// so every read or write of these fields must happen while holding this lock.
	mu sync.Mutex

	ID      string
	Address string
	Store   *kv.Store
	running bool

	// Raft state
	// currentTerm, votedFor and log are the "persistent state": they are saved to disk
	// (see persist.go) so a node that crashes and restarts doesn't forget its vote or its log.
	currentTerm int
	votedFor    string
	log         []LogEntry
	state       NodeState

	// Who we currently believe the leader is, so followers can redirect clients to it.
	leaderID      string
	leaderAddress string

	// commitIndex is the highest log index known to be stored on a majority (safe to apply).
	// lastApplied is the highest log index we have actually applied to the KV store.
	commitIndex int
	lastApplied int

	// Leader-only state, reset every time this node wins an election.
	nextIndex   map[string]int       // next log index to send to each peer
	matchIndex  map[string]int       // highest log index known to be replicated on each peer
	lastContact map[string]time.Time // when each peer last acknowledged us (used for the read lease)
	inFlight    map[string]bool      // true while an AppendEntries call to that peer is still running

	// Election timing
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	lastBroadcast   time.Time

	// applyCond lets goroutines sleep until the commit or apply position changes,
	// instead of checking in a busy loop.
	applyCond *sync.Cond

	// Client writes waiting for their log entry to be applied, keyed by log index.
	waiters map[int][]commitWaiter

	listener    net.Listener
	stopCh      chan struct{}
	initOnce    sync.Once
	initErr     error
	applierOnce sync.Once

	// Other nodes in the cluster
	Peers []string

	// Folder where Raft state is saved. Leave empty to keep everything in memory only.
	DataDir string

	// Print state changes (elections, new leaders) to the log.
	Verbose bool
}

func NewNode(id string, address string) *Node {
	n := &Node{
		ID:      id,
		Address: address,
		Store:   kv.NewStore(),
		running: false,
		state:   Follower,

		// The log starts with a placeholder entry at index 0 (term 0).
		// This means real entries start at index 1, and "the entry before the first one"
		// always exists, which removes a lot of special cases from the replication code.
		log: []LogEntry{{Term: 0}},

		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		lastContact: make(map[string]time.Time),
		inFlight:    make(map[string]bool),
		waiters:     make(map[int][]commitWaiter),

		electionTimeout: randomElectionTimeout(),
		lastHeartbeat:   time.Now(),
		stopCh:          make(chan struct{}),
	}

	n.applyCond = sync.NewCond(&n.mu)
	// The condition variable shares the node's mutex, so Wait() releases the lock while sleeping
	// and takes it back when woken up.

	return n
}

// Start loads any saved state, starts listening for RPCs, and starts the Raft loops.
// Unlike StartRPC/StartRaft it returns straight away, so it's the easiest way to run a node.
func (n *Node) Start() error {
	listener, err := n.listen()
	if err != nil {
		return err
	}

	go n.serve(listener)
	go n.StartRaft()

	return nil
}

// Stop shuts the node down: it closes the listener and stops every loop.
// A stopped node can't be started again. To "restart" it, create a new Node with the same ID, Address and DataDir.
func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return
	}

	n.running = false
	n.state = Follower
	close(n.stopCh)
	// Closing a channel wakes up EVERY goroutine waiting on it, which is how we tell all loops to exit.

	if n.listener != nil {
		n.listener.Close()
	}

	n.applyCond.Broadcast()
	// Wake anything sleeping on the condition so it notices we've stopped.
}

// init loads saved state from disk. It only ever runs once, no matter how many times it's called.
func (n *Node) init() error {
	n.initOnce.Do(func() {
		n.mu.Lock()
		defer n.mu.Unlock()

		n.initErr = n.loadState()
		if n.initErr == nil {
			n.running = true
		}
	})

	return n.initErr
}

// State returns the node's current Raft state.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.state
}

// Term returns the node's current term.
func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.currentTerm
}

// LeaderAddress returns the address of the node we currently believe is the leader ("" if unknown).
func (n *Node) LeaderAddress() string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.leaderAddress
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.commitIndex
}

// isMajority reports whether count nodes (including ourselves) is more than half of the cluster.
func (n *Node) isMajority(count int) bool {
	return count > (len(n.Peers)+1)/2
}

func (n *Node) logf(format string, args ...any) {
	if n.Verbose {
		log.Printf("["+n.ID+"] "+format, args...)
	}
}

func randomElectionTimeout() time.Duration {
	spread := int64(maxElectionTimeout - minElectionTimeout)
	return minElectionTimeout + time.Duration(rand.Int63n(spread))
}
