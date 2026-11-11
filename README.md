# Distributed-KV-Store

A distributed key-value store in Go, replicated with the Raft consensus algorithm.

## Features

- Leader election with randomized timeouts and up-to-date-log voting checks
- Log replication (AppendEntries) with fast conflict backtracking
- Commits once a majority stores an entry; every node applies entries in the same order
- Writes (`PUT`, `DELETE`) go through the Raft log and return once committed
- Reads (`GET`) are served by the leader using a lease, so a cut-off leader can't return stale data
- Followers redirect clients to the leader; `ClusterClient` finds the leader on its own
- Term, vote and log are saved to disk, so nodes can crash and restart safely

## Layout

```
cmd/server        run one node
cmd/client        command-line client
cmd/visualizer    live browser view of a running cluster
internal/kv       in-memory key-value store
internal/node     Raft + RPC
  node.go           Node struct, Start/Stop
  raft.go           elections and voting
  replication.go    AppendEntries, commit, apply loop
  commands.go       client PUT/GET/DELETE on the leader
  persist.go        saving/loading Raft state
  rpc.go            TCP server
  rpc_client.go     TCP client for a single node
  cluster_client.go client that finds the leader
tests/            tests
```

## Running a 3-node cluster

In three terminals:

```bash
go run ./cmd/server -id node1 -addr localhost:9001 -peers localhost:9002,localhost:9003 -data ./data
go run ./cmd/server -id node2 -addr localhost:9002 -peers localhost:9001,localhost:9003 -data ./data
go run ./cmd/server -id node3 -addr localhost:9003 -peers localhost:9001,localhost:9002 -data ./data
```

Then:

```bash
go run ./cmd/client put name Shiv
go run ./cmd/client get name
go run ./cmd/client delete name
```

## Live visualizer

Runs a 5-node cluster and shows it working in your browser: elections, heartbeats, log replication and commits, as they happen.

```bash
go run ./cmd/visualizer
```

Then open http://localhost:8080. Click a node to stop or restart it, click a link to cut it, or use the buttons to stop or isolate the leader and add network latency.

The Raft code is not modified for this. Each node's peer list points at a small relay in `cmd/visualizer`, which forwards every RPC to the real node and reports it to the page. The same relays are used to cut links and add delay.

## Tests

```bash
go test -v ./...
```

## Not implemented

- Log compaction / snapshots (the log grows forever)
- Changing cluster membership while running
