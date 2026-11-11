package main

// The Raft code doesn't expose each node's log, so the visualizer rebuilds it from the messages it relays:
//   - an AppendEntries request shows exactly what the leader's log looks like after PrevLogIndex
//   - a successful reply means the follower's log now matches the leader's up to the last entry sent
//
// This is the leader's view of each log, which is what Raft cares about.

import (
	"sort"
	"sync"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

type tracker struct {
	mu   sync.Mutex
	logs map[string][]node.LogEntry // node ID → log (index 0 is a placeholder, like in the node)
	keys map[string]bool            // every key we've seen, so we know what to read from each store
}

func newTracker() *tracker {
	return &tracker{
		logs: make(map[string][]node.LogEntry),
		keys: make(map[string]bool),
	}
}

// unknownTerm marks a log slot we know exists but haven't seen yet.
const unknownTerm = -1

// observeAppend records the leader's log from an AppendEntries request.
func (t *tracker) observeAppend(leader string, request node.Request) {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.logOf(leader)

	for len(current) < request.PrevLogIndex+1 {
		current = append(current, node.LogEntry{Term: unknownTerm})
	}

	updated := append(clone(current[:request.PrevLogIndex+1]), request.Entries...)
	t.logs[leader] = keepLongerIfConsistent(current, updated)

	for _, entry := range request.Entries {
		if entry.Command.Key != "" {
			t.keys[entry.Command.Key] = true
		}
	}
}

// observeAppendSuccess records that a follower's log now matches the leader's up to the last entry sent.
func (t *tracker) observeAppendSuccess(leader string, follower string, request node.Request) {
	t.mu.Lock()
	defer t.mu.Unlock()

	leaderLog := t.logOf(leader)
	length := min(request.PrevLogIndex+1+len(request.Entries), len(leaderLog))

	updated := clone(leaderLog[:length])
	t.logs[follower] = keepLongerIfConsistent(t.logOf(follower), updated)
}

// addKey remembers a key written through the visualizer.
func (t *tracker) addKey(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.keys[key] = true
}

// sortedKeys returns every key seen so far, in alphabetical order.
func (t *tracker) sortedKeys() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	keys := make([]string, 0, len(t.keys))
	for key := range t.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// logView is one log entry as sent to the browser.
type logView struct {
	Index     int    `json:"index"`
	Term      int    `json:"term"`
	Operation string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// tail returns a node's log length and its last `count` entries.
func (t *tracker) tail(id string, count int) (int, []logView) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log := t.logOf(id)
	length := len(log) - 1

	var views []logView
	for index := max(1, length-count+1); index <= length; index++ {
		entry := log[index]
		views = append(views, logView{
			Index:     index,
			Term:      entry.Term,
			Operation: entry.Command.Operation,
			Key:       entry.Command.Key,
			Value:     entry.Command.Value,
		})
	}

	return length, views
}

// logOf returns a node's tracked log. Caller must hold t.mu.
func (t *tracker) logOf(id string) []node.LogEntry {
	log, ok := t.logs[id]
	if !ok {
		log = []node.LogEntry{{Term: 0}}
		t.logs[id] = log
	}

	return log
}

// keepLongerIfConsistent stops the view from flickering when messages arrive out of order:
// an older, shorter message shouldn't shrink a log that a newer message already extended.
// We only keep the longer log if it agrees with the new one at the new one's last entry.
func keepLongerIfConsistent(current []node.LogEntry, updated []node.LogEntry) []node.LogEntry {
	if len(current) <= len(updated) {
		return updated
	}

	last := len(updated) - 1
	if current[last].Term == updated[last].Term {
		return current
	}

	return updated
}

func clone(entries []node.LogEntry) []node.LogEntry {
	return append([]node.LogEntry(nil), entries...)
}
