package node

import (
	"encoding/json"
	"net"
	"time"
)

// Network timeouts.
const (
	// How long to wait when connecting to a node.
	dialTimeout = 200 * time.Millisecond

	// Raft RPCs should be fast. If a peer takes longer than this, treat it as unreachable.
	raftRPCTimeout = 500 * time.Millisecond

	// Client requests can wait for a commit, so they get longer.
	clientRequestTimeout = commitTimeout + 2*time.Second
)

// Client communicates with a node over TCP.
type Client struct {
	Address string
}

// NewClient creates a new RPC client.
func NewClient(address string) *Client {
	return &Client{
		Address: address,
	}
}

// call sends one request to the node and waits for its response.
// Every operation below uses this, so the connection handling lives in one place.
func (c *Client) call(request Request, timeout time.Duration) (Response, error) {
	var response Response

	conn, err := net.DialTimeout("tcp", c.Address, dialTimeout)
	if err != nil {
		return response, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(timeout))
	// If the node doesn't answer in time, the read below fails instead of hanging forever.

	err = json.NewEncoder(conn).Encode(request)
	if err != nil {
		return response, err
	}

	err = json.NewDecoder(conn).Decode(&response)
	if err != nil {
		return response, err
	}

	return response, nil
}

// Get retrieves a value from the node.
// If the node isn't the leader, the error is a *NotLeaderError.
func (c *Client) Get(key string) (string, bool, error) {
	request := Request{
		Operation: "GET",
		Key:       key,
	}

	response, err := c.call(request, clientRequestTimeout)
	if err != nil {
		return "", false, err
	}

	err = response.toError()
	if err != nil {
		return "", false, err
	}

	return response.Value, response.Found, nil
}

// Put stores a key-value pair on the node.
// If the node isn't the leader, the error is a *NotLeaderError.
func (c *Client) Put(key string, value string) error {
	request := Request{
		Operation: OpPut,
		Key:       key,
		Value:     value,
	}

	response, err := c.call(request, clientRequestTimeout)
	if err != nil {
		return err
	}

	return response.toError()
}

// Delete removes a key-value pair from the node.
// If the node isn't the leader, the error is a *NotLeaderError.
func (c *Client) Delete(key string) error {
	request := Request{
		Operation: OpDelete,
		Key:       key,
	}

	response, err := c.call(request, clientRequestTimeout)
	if err != nil {
		return err
	}

	return response.toError()
}

// RequestVote asks another node to vote for this candidate.
func (c *Client) RequestVote(voteRequest RequestVoteRequest) (RequestVoteResponse, error) {
	request := Request{
		Operation:    opRequestVote,
		Term:         voteRequest.Term,
		CandidateID:  voteRequest.CandidateID,
		LastLogIndex: voteRequest.LastLogIndex,
		LastLogTerm:  voteRequest.LastLogTerm,
	}

	response, err := c.call(request, raftRPCTimeout)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	return RequestVoteResponse{
		Term:        response.Term,
		VoteGranted: response.VoteGranted,
	}, nil
}

// AppendEntries sends log entries (or a heartbeat) to a follower.
func (c *Client) AppendEntries(appendRequest AppendEntriesRequest) (AppendEntriesResponse, error) {
	request := Request{
		Operation:     opAppendEntries,
		Term:          appendRequest.Term,
		LeaderID:      appendRequest.LeaderID,
		LeaderAddress: appendRequest.LeaderAddress,
		PrevLogIndex:  appendRequest.PrevLogIndex,
		PrevLogTerm:   appendRequest.PrevLogTerm,
		Entries:       appendRequest.Entries,
		LeaderCommit:  appendRequest.LeaderCommit,
	}

	response, err := c.call(request, raftRPCTimeout)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return AppendEntriesResponse{
		Term:          response.Term,
		Success:       response.Success,
		ConflictIndex: response.ConflictIndex,
	}, nil
}
