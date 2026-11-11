package node

import (
	"encoding/json"
	"errors"
	"net" // communication package for TCP connections
	"time"
)

// The Request struct allows us to define the structure of the incoming requests that the node will handle.
type Request struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`

	// Raft fields
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`

	// RequestVote fields
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`

	// AppendEntries fields
	LeaderID      string     `json:"leader_id"`
	LeaderAddress string     `json:"leader_address"`
	PrevLogIndex  int        `json:"prev_log_index"`
	PrevLogTerm   int        `json:"prev_log_term"`
	Entries       []LogEntry `json:"entries"`
	LeaderCommit  int        `json:"leader_commit"`
}

// The Response struct allows us to define the structure of the outgoing responses that the node will send back to the clients.
// (This is mainly used for the GET operation, where we need to send back the value associated with a key and whether the key was found or not.)
type Response struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
	Error string `json:"error"`

	// Set when Error is "not leader", so the client knows where to retry.
	LeaderAddress string `json:"leader_address"`

	// Raft fields
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`

	// AppendEntries fields
	Success       bool `json:"success"`
	ConflictIndex int  `json:"conflict_index"`
}

// How long the server waits for a single connection before giving up on it.
// It has to be longer than commitTimeout, since a PUT can wait that long for a commit.
const connectionTimeout = 10 * time.Second

// Operation names for the Raft RPCs.
const (
	opRequestVote   = "REQUEST_VOTE"
	opAppendEntries = "APPEND_ENTRIES"
)

func (n *Node) StartRPC() error {
	listener, err := n.listen()
	if err != nil {
		return err
	}

	return n.serve(listener)
}

// listen opens the TCP listener for this node.
func (n *Node) listen() (net.Listener, error) {
	err := n.init()
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", n.Address)
	// Creates a listener on the specified address and port.
	// The listener will accept incoming connections from clients.
	if err != nil {
		return nil, err
	}

	n.mu.Lock()
	n.listener = listener
	stopped := !n.running
	n.mu.Unlock()

	// If Stop() was called while we were opening the listener, close it again.
	if stopped {
		listener.Close()
		return nil, ErrStopped
	}

	return listener, nil
}

// serve accepts connections until the listener is closed.
func (n *Node) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		// Accepts incoming connections from clients.
		if err != nil {
			// Accept fails once Stop() closes the listener. That's a normal shutdown, not an error.
			select {
			case <-n.stopCh:
				return nil
			default:
				return err
			}
		}

		go n.handleConnection(conn)
		// go initiates a new goroutine to handle the connection concurrently.
		// This allows the node to handle multiple connections without blocking the main thread,
		// improving scalability and responsiveness.
		// The benefit of Go is that it allows for Goroutines, which are a lightweight mechanism for concurrent execution.
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(connectionTimeout))
	// A deadline stops a client that connects but never sends anything from holding a goroutine forever.

	var request Request
	// Set up a Request instance to hold the decoded data from the incoming request.

	err := json.NewDecoder(conn).Decode(&request)
	// The decoder reads from the connection until it has one complete JSON value.
	// This replaces the old fixed 1024-byte buffer, which would cut off large
	// AppendEntries messages that carry many log entries.
	if err != nil {
		return
	}

	var response Response
	// Set up a Response instance to hold the data that will be sent back to the client.

	switch request.Operation {

	// Client operations all go through Raft, so only the leader accepts them.
	case OpPut:
		err := n.Put(request.Key, request.Value)
		response.setError(err)

	case "GET":
		value, ok, err := n.Get(request.Key)
		response.Value = value
		response.Found = ok
		response.setError(err)

	case OpDelete:
		err := n.Delete(request.Key)
		response.setError(err)

	case opRequestVote:
		voteRequest := RequestVoteRequest{
			Term:         request.Term,
			CandidateID:  request.CandidateID,
			LastLogIndex: request.LastLogIndex,
			LastLogTerm:  request.LastLogTerm,
		}

		voteResponse := n.handleRequestVote(voteRequest)

		response.Term = voteResponse.Term
		response.VoteGranted = voteResponse.VoteGranted

	case opAppendEntries:
		appendRequest := AppendEntriesRequest{
			Term:          request.Term,
			LeaderID:      request.LeaderID,
			LeaderAddress: request.LeaderAddress,
			PrevLogIndex:  request.PrevLogIndex,
			PrevLogTerm:   request.PrevLogTerm,
			Entries:       request.Entries,
			LeaderCommit:  request.LeaderCommit,
		}

		appendResponse := n.handleAppendEntries(appendRequest)

		response.Term = appendResponse.Term
		response.Success = appendResponse.Success
		response.ConflictIndex = appendResponse.ConflictIndex

	default:
		response.Error = "unknown operation"
	}

	json.NewEncoder(conn).Encode(response)
	// The encoder turns the response into JSON and writes it straight to the connection.
}

// setError copies an error into the response so it can be sent over the network.
func (r *Response) setError(err error) {
	if err == nil {
		return
	}

	r.Error = err.Error()

	var notLeader *NotLeaderError
	// errors.As checks whether err is a *NotLeaderError, and if so fills in notLeader.
	if errors.As(err, &notLeader) {
		r.LeaderAddress = notLeader.LeaderAddress
	}
}

// toError turns the response's error text back into a Go error on the client side.
func (r Response) toError() error {
	switch r.Error {

	case "":
		return nil

	case notLeaderMessage:
		return &NotLeaderError{LeaderAddress: r.LeaderAddress}

	default:
		return errors.New(r.Error)
	}
}
