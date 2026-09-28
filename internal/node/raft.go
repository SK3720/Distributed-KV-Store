package node

import "time"

// RequestVoteRequest contains the information a candidate
// sends to another node when requesting a vote.
type RequestVoteRequest struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

// RequestVoteResponse contains the response to a vote request.
type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// StartRaft starts the Raft election loop for the node.
func (n *Node) StartRaft() {
	for {
		if time.Since(n.lastHeartbeat) >= n.electionTimeout {
			if n.state == Follower || n.state == Candidate {
				n.startElection()
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// startElection begins a new Raft election.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID
	n.lastHeartbeat = time.Now()

	votes := 1 // vote for ourselves

	for _, peer := range n.Peers {
		client := NewClient(peer)

		term, voteGranted, err := client.RequestVote(
			n.currentTerm,
			n.ID,
		)

		if err != nil {
			continue
		}

		// If another node has a newer term,
		// step down to follower.
		if term > n.currentTerm {
			n.currentTerm = term
			n.state = Follower
			n.votedFor = ""
			return
		}

		if voteGranted {
			votes++
		}
	}

	// A majority is required to become leader.
	if votes > (len(n.Peers)+1)/2 {
		n.state = Leader
	}
}

// handleRequestVote handles a vote request from another node.
func (n *Node) handleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	response := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// If the candidate's term is older than ours,
	// reject the vote.
	if request.Term < n.currentTerm {
		return response
	}

	// If the candidate has a newer term,
	// update our term and become a follower.
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Grant the vote if we haven't voted for another
	// candidate during this term.
	if n.votedFor == "" || n.votedFor == request.CandidateID {
		n.votedFor = request.CandidateID
		response.VoteGranted = true
		n.lastHeartbeat = time.Now()
	}

	response.Term = n.currentTerm

	return response
}

// State returns the node's current Raft state.
func (n *Node) State() NodeState {
	return n.state
}
