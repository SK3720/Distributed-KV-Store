package node

import (
	"errors"
	"time"
)

const notLeaderMessage = "not leader"

// NotLeaderError is returned when a client sends a request to a node that isn't the leader.
// LeaderAddress tells the client where to go instead (it may be empty during an election).
type NotLeaderError struct {
	LeaderAddress string
}

func (e *NotLeaderError) Error() string {
	return notLeaderMessage
}

var (
	ErrCommitTimeout  = errors.New("timed out waiting for the entry to be committed")
	ErrLeadershipLost = errors.New("leadership was lost before the entry was committed")
	ErrNotReady       = errors.New("leader has not committed an entry in its current term yet")
	ErrLeaseExpired   = errors.New("leader has not heard from a majority recently")
	ErrStopped        = errors.New("node is stopped")
)

// commitWaiter is a client write waiting for its log entry to be applied.
type commitWaiter struct {
	term int
	done chan error
}

// Put stores a key-value pair through Raft. It returns once a majority has the entry and it has been applied.
func (n *Node) Put(key string, value string) error {
	return n.submit(Command{Operation: OpPut, Key: key, Value: value})
}

// Delete removes a key through Raft. It returns once a majority has the entry and it has been applied.
func (n *Node) Delete(key string) error {
	return n.submit(Command{Operation: OpDelete, Key: key})
}

// submit appends a command to the leader's log and waits for it to be applied.
func (n *Node) submit(command Command) error {
	n.mu.Lock()

	if !n.running {
		n.mu.Unlock()
		return ErrStopped
	}

	// Only the leader can add entries to the log.
	if n.state != Leader {
		leader := n.leaderAddress
		n.mu.Unlock()
		return &NotLeaderError{LeaderAddress: leader}
	}

	entry := LogEntry{Term: n.currentTerm, Command: command}
	n.log = append(n.log, entry)
	index := n.lastLogIndex()
	n.persist()

	waiter := commitWaiter{term: entry.Term, done: make(chan error, 1)}
	// The channel has room for 1 value, so the apply loop never blocks when it sends the result.
	n.waiters[index] = append(n.waiters[index], waiter)

	n.broadcastAppendEntries()
	// Send the new entry to followers now rather than waiting for the next heartbeat.

	n.mu.Unlock()

	select {
	case err := <-waiter.done:
		return err

	case <-time.After(commitTimeout):
		n.mu.Lock()
		n.removeWaiter(index, waiter)
		n.mu.Unlock()
		return ErrCommitTimeout

	case <-n.stopCh:
		return ErrStopped
	}
}

// notifyWaiters tells waiting writes that the entry at index has been applied. Caller must hold n.mu.
func (n *Node) notifyWaiters(index int, entry LogEntry) {
	for _, waiter := range n.waiters[index] {
		// If the applied entry has a different term than the one we appended, a new leader
		// overwrote our entry, so our write never happened.
		if entry.Term == waiter.term {
			waiter.done <- nil
		} else {
			waiter.done <- ErrLeadershipLost
		}
	}

	delete(n.waiters, index)
}

// removeWaiter forgets a waiter that gave up. Caller must hold n.mu.
func (n *Node) removeWaiter(index int, target commitWaiter) {
	waiters := n.waiters[index]

	for i, waiter := range waiters {
		if waiter.done == target.done {
			n.waiters[index] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}

	if len(n.waiters[index]) == 0 {
		delete(n.waiters, index)
	}
}

// Get reads a value from the leader's store.
//
// Reads don't go through the log (that would make every read as slow as a write). Instead the leader
// makes sure it really is still the leader before answering, so a stale leader cut off from
// the cluster can't return old data:
//  1. It must have committed an entry in its current term (the NOOP), so it knows what's committed.
//  2. It must hold a "lease": a majority acknowledged it within leaseDuration. Followers refuse to
//     vote for anyone else during that window (see handleRequestVote), so no new leader can exist yet.
//  3. It waits until its store has applied everything up to its commitIndex.
func (n *Node) Get(key string) (string, bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return "", false, ErrStopped
	}

	if n.state != Leader {
		return "", false, &NotLeaderError{LeaderAddress: n.leaderAddress}
	}

	if n.log[n.commitIndex].Term != n.currentTerm {
		return "", false, ErrNotReady
	}

	if !n.hasLease() {
		return "", false, ErrLeaseExpired
	}

	readIndex := n.commitIndex

	for n.running && n.lastApplied < readIndex {
		n.applyCond.Wait()
	}

	if !n.running {
		return "", false, ErrStopped
	}

	value, found := n.Store.Get(key)
	return value, found, nil
}

// hasLease reports whether a majority has acknowledged us recently. Caller must hold n.mu.
func (n *Node) hasLease() bool {
	count := 1 // ourselves

	for _, peer := range n.Peers {
		if time.Since(n.lastContact[peer]) < leaseDuration {
			count++
		}
	}

	return n.isMajority(count)
}
