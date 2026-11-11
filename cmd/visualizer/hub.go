package main

// The hub pushes live updates to every open browser tab using Server-Sent Events (SSE):
// the browser keeps one HTTP request open and the server writes "data: ..." lines to it as things happen.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type hub struct {
	mu      sync.Mutex
	clients map[chan []byte]bool
}

func newHub() *hub {
	return &hub{clients: make(map[chan []byte]bool)}
}

// publish sends a message to every connected browser.
func (h *hub) publish(message any) {
	data, err := json.Marshal(message)
	if err != nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for client := range h.clients {
		select {
		case client <- data:
		default:
			// This browser is falling behind. Skip the message rather than slow down the cluster.
		}
	}
}

// serveEvents keeps a browser connection open and streams messages to it.
func (h *hub) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	client := make(chan []byte, 1024)

	h.mu.Lock()
	h.clients[client] = true
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, client)
		h.mu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return // the browser closed the page

		case data := <-client:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
			// Flush sends the data now instead of waiting for a buffer to fill up.
		}
	}
}
