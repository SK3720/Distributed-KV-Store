package main

// The visualizer never changes the Raft code. Instead, every node's peer list points at a small
// relay ("proxy") instead of the real peer. There is one relay per direction for each pair of nodes
// (N1→N2, N2→N1, ...), so the relay always knows who sent each message.
//
// Each relay:
//   - reads the RPC, reports it to the browser, and forwards it to the real node
//   - reads the reply, reports it, and sends it back
//   - can "cut" the link (drop everything) or add network delay, to inject faults

import (
	"encoding/json"
	"net"
	"sync/atomic"
	"time"

	"github.com/SK3720/Distributed-KV-Store/internal/node"
)

// link is a one-way connection from one node to another, through a relay.
type link struct {
	from     string
	to       string
	listener net.Listener
	cut      atomic.Bool
	// atomic.Bool can be read and written from many goroutines safely without a mutex.
}

// rpcEvent describes one message, sent to the browser so it can animate it.
type rpcEvent struct {
	Type    string `json:"type"` // always "rpc"
	ID      int64  `json:"id"`   // links a request to its response
	Phase   string `json:"phase"`
	Kind    string `json:"kind"`
	From    string `json:"from"` // the node that sent the request
	To      string `json:"to"`
	Term    int    `json:"term"`
	Entries int    `json:"entries"`
	OK      bool   `json:"ok"`
	Reason  string `json:"reason,omitempty"`
	DelayMS int64  `json:"delay_ms"`
}

// Phases of a message.
const (
	phaseRequest  = "request"
	phaseResponse = "response"
	phaseDrop     = "drop"
)

// serveLink accepts connections on a relay until it is closed.
func (c *cluster) serveLink(l *link) {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}

		go c.relay(l, conn)
	}
}

// relay forwards one RPC from l.from to l.to and reports what happened.
func (c *cluster) relay(l *link, incoming net.Conn) {
	defer incoming.Close()
	incoming.SetDeadline(time.Now().Add(5 * time.Second))

	var request node.Request

	err := json.NewDecoder(incoming).Decode(&request)
	if err != nil {
		return
	}

	event := rpcEvent{
		Type:    "rpc",
		ID:      c.nextMessageID.Add(1),
		Kind:    classify(request),
		From:    l.from,
		To:      l.to,
		Term:    request.Term,
		Entries: len(request.Entries),
		DelayMS: c.delayMS.Load(),
	}

	if event.Kind == "append" || event.Kind == "heartbeat" {
		c.tracker.observeAppend(l.from, request)
	}

	// A cut link drops the message. Closing the connection makes the sender see a network error,
	// exactly as if the other node were unreachable.
	if l.cut.Load() {
		event.Phase = phaseDrop
		event.Reason = "link cut"
		c.hub.publish(event)
		return
	}

	outgoing, err := net.DialTimeout("tcp", c.addresses[l.to], 200*time.Millisecond)
	if err != nil {
		event.Phase = phaseDrop
		event.Reason = "node down"
		c.hub.publish(event)
		return
	}
	defer outgoing.Close()
	outgoing.SetDeadline(time.Now().Add(5 * time.Second))

	event.Phase = phaseRequest
	c.hub.publish(event)

	c.sleepDelay()

	err = json.NewEncoder(outgoing).Encode(request)
	if err != nil {
		return
	}

	var response node.Response

	err = json.NewDecoder(outgoing).Decode(&response)
	if err != nil {
		return
	}

	// The link may have been cut while the message was in flight.
	if l.cut.Load() {
		return
	}

	switch event.Kind {
	case "vote":
		event.OK = response.VoteGranted
	default:
		event.OK = response.Success
		if response.Success {
			c.tracker.observeAppendSuccess(l.from, l.to, request)
		}
	}

	c.sleepDelay()

	event.Phase = phaseResponse
	c.hub.publish(event)

	json.NewEncoder(incoming).Encode(response)
}

// sleepDelay waits for the configured network delay (one way).
func (c *cluster) sleepDelay() {
	delay := c.delayMS.Load()
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}
}

// classify gives a message a short name for the browser.
// The operation names match the (unexported) constants in internal/node/rpc.go.
func classify(request node.Request) string {
	switch request.Operation {

	case "REQUEST_VOTE":
		return "vote"

	case "APPEND_ENTRIES":
		if len(request.Entries) == 0 {
			return "heartbeat"
		}
		return "append"
	}

	return "other"
}
