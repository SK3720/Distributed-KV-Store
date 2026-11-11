package node

import (
	"log"
	"time"
)

// RequestVoteRequest contains the information a candidate
// sends to another node when requesting a vote.
type RequestVoteRequest struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`

	// The index and term of the candidate's last log entry.
	// Voters use these to check the candidate's log is at least as up-to-date as their own,
	// so a node that is missing committed entries can never become leader.
	LastLogIndex int `json:"last_log_index"`
	LastLogTerm  int `json:"last_log_term"`
}

// RequestVoteResponse contains the response to a vote request.
type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// StartRaft starts the Raft election loop for the node.
// It blocks until the node is stopped, so it's normally run in its own goroutine.
func (n *Node) StartRaft() {
	if err := n.init(); err != nil {
		log.Printf("[%s] failed to load saved state: %v", n.ID, err)
		return
	}

	n.applierOnce.Do(func() {
		go n.runApplier()
	})
	// The apply loop runs in the background and applies committed entries to the KV store.

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		// select waits on several channels at once and runs whichever case is ready first.
		case <-n.stopCh:
			return

		case <-ticker.C:
			n.tick()
		}
	}
}

// tick runs every 10ms and decides whether we need to send heartbeats or start an election.
func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Leader {
		if time.Since(n.lastBroadcast) >= heartbeatInterval {
			n.broadcastAppendEntries()
		}
		return
	}

	// Followers and candidates start an election if they haven't heard from a leader in a while.
	if time.Since(n.lastHeartbeat) >= n.electionTimeout {
		n.startElection()
	}
}

// startElection begins a new Raft election. Caller must hold n.mu.
func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID
	n.leaderID = ""
	n.leaderAddress = ""
	n.lastHeartbeat = time.Now()

	// Pick a fresh random timeout each election, so two nodes that split the vote
	// once are unlikely to split it again.
	n.electionTimeout = randomElectionTimeout()

	n.persist()
	n.logf("starting election for term %d", n.currentTerm)

	request := RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.ID,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}

	votes := 1 // vote for ourselves

	// A single-node cluster wins straight away.
	if n.isMajority(votes) {
		n.becomeLeader()
		return
	}

	for _, peer := range n.Peers {
		// Ask every peer in parallel, so one slow or dead node doesn't hold up the election.
		// We must NOT hold the lock while waiting on the network, so each goroutine
		// only takes the lock once the reply has arrived.
		go func(peer string) {
			response, err := NewClient(peer).RequestVote(request)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If another node has a newer term,
			// step down to follower.
			if response.Term > n.currentTerm {
				n.becomeFollower(response.Term)
				return
			}

			// Ignore late replies from an election we're no longer running
			// (we already won, lost, or moved on to a newer term).
			if n.state != Candidate || n.currentTerm != request.Term {
				return
			}

			if response.VoteGranted {
				votes++
				// votes is shared between these goroutines, which is safe because we hold n.mu here.

				// A majority is required to become leader.
				if n.isMajority(votes) {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader switches this node to leader. Caller must hold n.mu.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.ID
	n.leaderAddress = n.Address

	// We don't know what each follower has yet, so start by assuming they have
	// everything we have. If they don't, AppendEntries will fail and we'll back up.
	for _, peer := range n.Peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1
		n.matchIndex[peer] = 0
		delete(n.lastContact, peer)
	}

	n.log = append(n.log, LogEntry{
		Term:    n.currentTerm,
		Command: Command{Operation: OpNoop},
	})
	n.persist()

	n.logf("became leader for term %d", n.currentTerm)

	// Tell everyone straight away so they don't start their own elections.
	n.broadcastAppendEntries()
}

// becomeFollower steps down to follower, moving to a newer term if we see one. Caller must hold n.mu.
func (n *Node) becomeFollower(term int) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.leaderID = ""
		n.leaderAddress = ""
		n.persist()
	}

	if n.state == Leader {
		n.logf("stepping down in term %d", n.currentTerm)

		// A leader never updates lastHeartbeat, so reset it here.
		// Otherwise we would start an election the moment we step down.
		n.lastHeartbeat = time.Now()
	}

	n.state = Follower
}

// handleRequestVote handles a vote request from another node.
func (n *Node) handleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// If the candidate's term is older than ours,
	// reject the vote.
	if request.Term < n.currentTerm {
		return response
	}

	// "Leader stickiness": if we heard from a working leader very recently, ignore the request
	// completely (we don't even adopt the newer term). This stops a node that was cut off from
	// the cluster from kicking out a healthy leader when it reconnects, and it is what makes the
	// leader's read lease safe (see Get in commands.go).
	if n.leaderID != "" && n.state == Follower && time.Since(n.lastHeartbeat) < minElectionTimeout {
		return response
	}

	// If the candidate has a newer term,
	// update our term and become a follower.
	if request.Term > n.currentTerm {
		n.becomeFollower(request.Term)
	}

	// Only vote for a candidate whose log is at least as up-to-date as ours.
	// "Up-to-date" means: a later last term wins, and if the last terms match, the longer log wins.
	candidateUpToDate := request.LastLogTerm > n.lastLogTerm() ||
		(request.LastLogTerm == n.lastLogTerm() && request.LastLogIndex >= n.lastLogIndex())

	// Grant the vote if we haven't voted for another
	// candidate during this term.
	if (n.votedFor == "" || n.votedFor == request.CandidateID) && candidateUpToDate {
		n.votedFor = request.CandidateID
		n.persist()
		// The vote must be saved BEFORE we reply, otherwise a crash and restart
		// could let us vote twice in the same term.

		response.VoteGranted = true
		n.lastHeartbeat = time.Now()
	}

	response.Term = n.currentTerm

	return response
}
