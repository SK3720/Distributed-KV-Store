package node

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// persistentState is the part of the Raft state that must survive a crash.
// Raft's safety depends on it: forgetting votedFor could let a node vote twice in one term,
// and forgetting the log could lose entries that were already committed.
type persistentState struct {
	CurrentTerm int        `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// statePath returns the file this node's state is saved in.
func (n *Node) statePath() string {
	// Replace characters that aren't allowed in file names (e.g. ":" on Windows).
	safeID := strings.NewReplacer(":", "_", "/", "_", "\\", "_").Replace(n.ID)

	return filepath.Join(n.DataDir, "raft-"+safeID+".json")
}

// persist saves the persistent state to disk. Caller must hold n.mu.
// It rewrites the whole file each time, which is simple but gets slower as the log grows.
// A production system would append to the file and take snapshots to keep it small.
func (n *Node) persist() {
	if n.DataDir == "" {
		return // in-memory only
	}

	data, err := json.Marshal(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
	if err != nil {
		log.Printf("[%s] failed to encode state: %v", n.ID, err)
		return
	}

	err = writeFileAtomic(n.statePath(), data)
	if err != nil {
		// A real system should crash here, since continuing without saved state isn't safe.
		log.Printf("[%s] failed to save state: %v", n.ID, err)
	}
}

// loadState reads the saved state from disk, if there is any. Caller must hold n.mu.
func (n *Node) loadState() error {
	if n.DataDir == "" {
		return nil
	}

	err := os.MkdirAll(n.DataDir, 0o755)
	if err != nil {
		return err
	}

	data, err := os.ReadFile(n.statePath())
	if os.IsNotExist(err) {
		return nil // first time this node has started
	}
	if err != nil {
		return err
	}

	var state persistentState

	err = json.Unmarshal(data, &state)
	if err != nil {
		return err
	}

	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor

	if len(state.Log) > 0 {
		n.log = state.Log
	}

	// commitIndex and lastApplied start at 0. The KV store is rebuilt by re-applying
	// the log once the leader tells us how far it has been committed.

	return nil
}

// writeFileAtomic writes to a temporary file and then renames it over the real one.
// A rename is all-or-nothing, so a crash mid-write can never leave a half-written state file.
func writeFileAtomic(path string, data []byte) error {
	tempPath := path + ".tmp"

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		file.Close()
		return err
	}

	err = file.Sync()
	// Sync forces the data out of the operating system's cache and onto the disk.
	if err != nil {
		file.Close()
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}
