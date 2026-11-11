package main

// Live visualizer for the Raft KV store.
//
// It runs a real cluster (the unchanged internal/node code) in this process and serves a web page
// that shows elections, heartbeats, log replication and commits as they happen. You can also stop
// nodes, cut network links and add latency to see how Raft recovers.
//
//	go run ./cmd/visualizer
//	then open http://localhost:8080

import (
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

//go:embed web/index.html
var indexHTML []byte

// go:embed copies the file into the program when it's compiled, so the binary works on its own.

func main() {
	nodeCount := flag.Int("nodes", 5, "number of nodes in the cluster")
	basePort := flag.Int("base-port", 7001, "port of the first node (the others use the ports after it)")
	httpAddress := flag.String("http", "localhost:8080", "address to serve the visualizer on")
	flag.Parse()

	if *nodeCount < 1 {
		log.Fatal("-nodes must be at least 1")
	}

	// Each run starts from a fresh, empty cluster. Saved state lives in a temporary folder
	// so restarted nodes can recover, and it's deleted when the visualizer exits.
	dataDir, err := os.MkdirTemp("", "kv-visualizer-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	c, err := newCluster(*nodeCount, *basePort, dataDir)
	if err != nil {
		log.Fatalf("failed to create cluster: %v", err)
	}

	err = c.start()
	if err != nil {
		log.Fatalf("failed to start cluster: %v", err)
	}

	go c.publishSnapshots()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("GET /api/events", c.hub.serveEvents)
	mux.HandleFunc("POST /api/put", c.handlePut)
	mux.HandleFunc("POST /api/node", c.handleNode)
	mux.HandleFunc("POST /api/link", c.handleLink)
	mux.HandleFunc("POST /api/delay", c.handleDelay)
	mux.HandleFunc("POST /api/heal", c.handleHeal)

	server := &http.Server{Addr: *httpAddress, Handler: mux}

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("web server failed: %v", err)
		}
	}()

	log.Printf("Running a %d-node cluster. Open http://%s", *nodeCount, *httpAddress)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	log.Println("Shutting down")
	server.Close()
	c.stop()
}

// apiRequest holds every field the browser may send. Each endpoint only uses the ones it needs.
type apiRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	ID     string `json:"id"`
	Action string `json:"action"`
	A      string `json:"a"`
	B      string `json:"b"`
	Cut    bool   `json:"cut"`
	MS     int64  `json:"ms"`
}

func readRequest(w http.ResponseWriter, r *http.Request) (apiRequest, bool) {
	var request apiRequest

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON"})
		return request, false
	}

	return request, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeResult(w http.ResponseWriter, err error) {
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (c *cluster) handlePut(w http.ResponseWriter, r *http.Request) {
	request, ok := readRequest(w, r)
	if !ok {
		return
	}

	if request.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "key is required"})
		return
	}

	c.tracker.addKey(request.Key)
	writeResult(w, c.put(request.Key, request.Value))
}

// handleNode stops or starts a node: {"id": "N1", "action": "stop" | "start"}.
func (c *cluster) handleNode(w http.ResponseWriter, r *http.Request) {
	request, ok := readRequest(w, r)
	if !ok {
		return
	}

	if !c.isKnownNode(request.ID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown node"})
		return
	}

	switch request.Action {
	case "stop":
		c.stopNode(request.ID)
		writeResult(w, nil)

	case "start":
		writeResult(w, c.startNode(request.ID))

	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "action must be stop or start"})
	}
}

// handleLink cuts or heals the link between two nodes: {"a": "N1", "b": "N2", "cut": true}.
func (c *cluster) handleLink(w http.ResponseWriter, r *http.Request) {
	request, ok := readRequest(w, r)
	if !ok {
		return
	}

	writeResult(w, c.setLinkCut(request.A, request.B, request.Cut))
}

// handleDelay sets the one-way network delay added to every message: {"ms": 50}.
// Raft RPCs time out after 500ms, so the delay is capped well below that.
func (c *cluster) handleDelay(w http.ResponseWriter, r *http.Request) {
	request, ok := readRequest(w, r)
	if !ok {
		return
	}

	c.delayMS.Store(max(0, min(request.MS, 200)))
	writeResult(w, nil)
}

func (c *cluster) handleHeal(w http.ResponseWriter, r *http.Request) {
	writeResult(w, c.heal())
}
