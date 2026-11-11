package node

import "time"

// AppendEntriesRequest is sent by the leader to copy log entries to a follower.
// With no entries it doubles as a heartbeat ("I'm still the leader").
type AppendEntriesRequest struct {
	Term          int    `json:"term"`
	LeaderID      string `json:"leader_id"`
	LeaderAddress string `json:"leader_address"`

	// The index and term of the entry just BEFORE the new ones.
	// The follower only accepts the new entries if its log has this exact entry,
	// which guarantees both logs are identical up to that point.
	PrevLogIndex int `json:"prev_log_index"`
	PrevLogTerm  int `json:"prev_log_term"`

	Entries []LogEntry `json:"entries"`

	// The leader's commitIndex, so followers know what is safe to apply.
	LeaderCommit int `json:"leader_commit"`
}

// AppendEntriesResponse contains the follower's reply.
type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`

	// When Success is false, this tells the leader where to retry from,
	// so it can skip back a whole term at a time instead of one entry per round trip.
	ConflictIndex int `json:"conflict_index"`
}

// broadcastAppendEntries sends AppendEntries (or a heartbeat) to every peer. Caller must hold n.mu.
func (n *Node) broadcastAppendEntries() {
	n.lastBroadcast = time.Now()

	// With no peers this is the only place a single-node cluster commits entries.
	n.advanceCommitIndex()

	for _, peer := range n.Peers {
		go n.replicateTo(peer)
	}
}

// replicateTo sends one AppendEntries call to a peer and handles the reply.
func (n *Node) replicateTo(peer string) {
	n.mu.Lock()

	// Only one call per peer at a time. Anything we skip is picked up by the next heartbeat.
	if n.state != Leader || n.inFlight[peer] {
		n.mu.Unlock()
		return
	}
	n.inFlight[peer] = true

	next := n.nextIndex[peer]
	prevLogIndex := next - 1

	entries := make([]LogEntry, len(n.log[next:]))
	copy(entries, n.log[next:])
	// We copy the entries because the log can change once we release the lock.

	request := AppendEntriesRequest{
		Term:          n.currentTerm,
		LeaderID:      n.ID,
		LeaderAddress: n.Address,
		PrevLogIndex:  prevLogIndex,
		PrevLogTerm:   n.log[prevLogIndex].Term,
		Entries:       entries,
		LeaderCommit:  n.commitIndex,
	}

	n.mu.Unlock()
	// Never hold the lock while waiting on the network.

	sentAt := time.Now()
	response, err := NewClient(peer).AppendEntries(request)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.inFlight[peer] = false

	if err != nil {
		return
	}

	// A newer term means someone else is leader now.
	if response.Term > n.currentTerm {
		n.becomeFollower(response.Term)
		return
	}

	// Ignore replies to requests from a term we're no longer leading.
	if n.state != Leader || n.currentTerm != request.Term {
		return
	}

	// We record when the request was SENT, not when the reply came back.
	// The follower reset its timer when it received the request, so this is the safe (earlier) time.
	n.lastContact[peer] = sentAt

	if response.Success {
		match := prevLogIndex + len(entries)
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1

		n.advanceCommitIndex()

		// If new entries arrived while this call was running, send them now instead of waiting for the next heartbeat.
		if n.nextIndex[peer] <= n.lastLogIndex() {
			go n.replicateTo(peer)
		}
		return
	}

	// The follower's log didn't match. Back up and try again straight away.
	if response.ConflictIndex > 0 {
		n.nextIndex[peer] = response.ConflictIndex
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	go n.replicateTo(peer)
}

// handleAppendEntries handles an AppendEntries call from the leader.
func (n *Node) handleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reject calls from an old leader.
	if request.Term < n.currentTerm {
		return response
	}

	// This is the real leader for this term. If we were a candidate (or have an older term),
	// step down to follower.
	if request.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(request.Term)
	}

	n.leaderID = request.LeaderID
	n.leaderAddress = request.LeaderAddress
	n.lastHeartbeat = time.Now()
	response.Term = n.currentTerm

	// Consistency check 1: our log is too short to contain PrevLogIndex.
	// Ask the leader to continue from the end of our log.
	if request.PrevLogIndex > n.lastLogIndex() {
		response.ConflictIndex = n.lastLogIndex() + 1
		return response
	}

	// Consistency check 2: we have an entry at PrevLogIndex, but from a different term.
	// Ask the leader to skip back to the first entry of that (wrong) term.
	if n.log[request.PrevLogIndex].Term != request.PrevLogTerm {
		conflictTerm := n.log[request.PrevLogIndex].Term

		index := request.PrevLogIndex
		for index > 1 && n.log[index-1].Term == conflictTerm {
			index--
		}

		response.ConflictIndex = index
		return response
	}

	// The logs match up to PrevLogIndex. Now add the new entries.
	// We only cut our log at the first entry that actually CONFLICTS (same index, different term).
	// Blindly cutting at PrevLogIndex would be wrong if an old, delayed request arrives
	// after a newer one: it could delete entries we've already accepted.
	changed := false

	for i, entry := range request.Entries {
		index := request.PrevLogIndex + 1 + i

		if index <= n.lastLogIndex() {
			if n.log[index].Term == entry.Term {
				continue // we already have this entry
			}

			n.log = n.log[:index] // remove the conflicting entry and everything after it
		}

		n.log = append(n.log, request.Entries[i:]...)
		changed = true
		break
	}

	if changed {
		n.persist()
	}

	// Move our commitIndex forward, but never past the entries we know match the leader.
	if request.LeaderCommit > n.commitIndex {
		lastNewIndex := request.PrevLogIndex + len(request.Entries)
		newCommit := min(request.LeaderCommit, lastNewIndex)

		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCond.Broadcast()
		}
	}

	response.Success = true

	return response
}

// advanceCommitIndex commits the newest entry that a majority has stored. Caller must hold n.mu.
func (n *Node) advanceCommitIndex() {
	if n.state != Leader {
		return
	}

	for index := n.lastLogIndex(); index > n.commitIndex; index-- {
		// A leader may only commit entries from its own term by counting copies.
		// Older entries get committed indirectly when a newer entry after them commits.
		// (Terms only go up through the log, so once we hit an older term we can stop.)
		if n.log[index].Term != n.currentTerm {
			break
		}

		count := 1 // the leader has every entry
		for _, peer := range n.Peers {
			if n.matchIndex[peer] >= index {
				count++
			}
		}

		if n.isMajority(count) {
			n.commitIndex = index
			n.applyCond.Broadcast()
			break
		}
	}
}

// runApplier applies committed entries to the KV store, in log order, one at a time.
// Every node runs this, so every node's store ends up with the same data.
func (n *Node) runApplier() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		for n.running && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
			// Wait() releases the lock while sleeping, and takes it back before returning.
		}

		if !n.running {
			return
		}

		n.lastApplied++
		entry := n.log[n.lastApplied]

		n.applyCommand(entry.Command)
		n.notifyWaiters(n.lastApplied, entry)

		n.applyCond.Broadcast()
		// Wake up any reads waiting for this entry to be applied.
	}
}

// applyCommand performs a single committed command on the KV store.
func (n *Node) applyCommand(command Command) {
	switch command.Operation {

	case OpPut:
		n.Store.Put(command.Key, command.Value)

	case OpDelete:
		n.Store.Delete(command.Key)

	case OpNoop:
		// Nothing to do.
	}
}
