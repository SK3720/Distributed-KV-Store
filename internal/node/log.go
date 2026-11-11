package node

// Operations that can be stored in the Raft log.
const (
	OpPut    = "PUT"
	OpDelete = "DELETE"

	// A new leader appends a NOOP ("no operation") entry as soon as it is elected.
	// Raft only lets a leader commit entries from its OWN term by counting replicas,
	// so committing this empty entry also commits everything before it, and tells the
	// leader it is up to date and can safely answer reads.
	OpNoop = "NOOP"
)

// Command is a change to the KV store that is replicated through the Raft log.
type Command struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// LogEntry is a single entry in the Raft log.
// The Term records which leader created the entry, which is how nodes detect
// conflicting entries left behind by old leaders.
type LogEntry struct {
	Term    int     `json:"term"`
	Command Command `json:"command"`
}

// lastLogIndex returns the index of the last entry in the log. Caller must hold n.mu.
func (n *Node) lastLogIndex() int {
	return len(n.log) - 1
}

// lastLogTerm returns the term of the last entry in the log. Caller must hold n.mu.
func (n *Node) lastLogTerm() int {
	return n.log[n.lastLogIndex()].Term
}
