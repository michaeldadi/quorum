# Quorum

[![CI](https://github.com/michaeldadi/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/michaeldadi/quorum/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/michaeldadi/quorum)](https://goreportcard.com/report/github.com/michaeldadi/quorum)

Distributed key-value store with Raft consensus implemented from scratch in Go.

No external consensus libraries—just the [Raft paper](https://raft.github.io/raft.pdf) and Go's standard library.

## Features

- **Leader election** with randomized timeouts
- **Log replication** across nodes with consistency guarantees
- **Persistence** and crash recovery
- **Snapshots** for log compaction
- **Membership changes** (add/remove nodes at runtime)
- **Linearizable reads** via ReadIndex
- **HTTP API** for client interaction
- **Leader redirection** for seamless client experience

## Quick Start

### Using Docker Compose (recommended)

```bash
# Start a 3-node cluster
docker-compose up -d

# Check cluster status
curl http://localhost:8001/status

# Write a value
curl -X POST http://localhost:8001/put \
  -H "Content-Type: application/json" \
  -d '{"key": "hello", "value": "world"}'

# Read it back (from any node)
curl "http://localhost:8002/get?key=hello"

# Stop the cluster
docker-compose down
```

### Running Locally
```bash
# Build
make build

# Start 3 nodes in separate terminals
./bin/quorum --id=node-1 --port=9001 --http=8001
./bin/quorum --id=node-2 --port=9002 --http=8002
./bin/quorum --id=node-3 --port=9003 --http=8003
```

## Architecture
```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Clients                                     │
│                         (HTTP requests)                                  │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────┐
│                           HTTP API                                       │
│                    /put  /get  /delete  /status                         │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────┐
│                          KV Store                                        │
│              (state machine: applies committed commands)                 │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────┐
│                         Raft Consensus                                   │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐             │
│  │   Node 1    │◄────►│   Node 2    │◄────►│   Node 3    │             │
│  │  (Leader)   │      │ (Follower)  │      │ (Follower)  │             │
│  └─────────────┘      └─────────────┘      └─────────────┘             │
│         │                    │                    │                     │
│         └────────────────────┴────────────────────┘                     │
│                    RPC (RequestVote, AppendEntries)                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Write Path

```
Client                Leader              Followers
   │                    │                    │
   │─── PUT request ───►│                    │
   │                    │── AppendEntries ──►│
   │                    │◄─── Success ───────│
   │                    │                    │
   │                    │ (majority acked)   │
   │                    │── Commit ─────────►│
   │◄── OK ────────────│                    │
```

### Leader Election

```
      Follower                Candidate               Leader
          │                       │                      │
          │ (timeout)             │                      │
          │──────────────────────►│                      │
          │                       │                      │
          │                       │── RequestVote ──────►│
          │                       │◄─── VoteGranted ─────│
          │                       │                      │
          │                       │ (majority votes)     │
          │                       │─────────────────────►│
          │                       │                      │
          │◄──────────────────────┼── Heartbeats ────────│
```

## API Reference

### Key-Value Operations

| Endpoint                | Method | Description                         |
|-------------------------|--------|-------------------------------------|
| `/put`                  | POST   | Write a key-value pair              |
| `/get?key=X`            | GET    | Read a value (linearizable)         |
| `/get?key=X&local=true` | GET    | Read a value (eventual consistency) |
| `/delete?key=X`         | POST   | Delete a key                        |

### Cluster Operations

| Endpoint           | Method | Description                    |
|--------------------|--------|--------------------------------|
| `/status`          | GET    | Node status, term, leader info |
| `/health`          | GET    | Health check                   |
| `/cluster/members` | GET    | List cluster members           |
| `/cluster/add`     | POST   | Add a node to the cluster      |
| `/cluster/remove`  | POST   | Remove a node from the cluster |

### Examples

```bash
# Write
curl -X POST http://localhost:8001/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "value": "{\"name\": \"Alice\"}"}'

# Read (linearizable - goes through leader)
curl "http://localhost:8001/get?key=user:1"

# Read (local - faster, eventual consistency)
curl "http://localhost:8002/get?key=user:1&local=true"

# Delete
curl -X POST "http://localhost:8001/delete?key=user:1"

# Cluster status
curl http://localhost:8001/status

# Add a new node
curl -X POST http://localhost:8001/cluster/add \
  -H "Content-Type: application/json" \
  -d '{"nodeId": "node-4", "raftAddr": "localhost:9004"}'

# Remove a node
curl -X POST http://localhost:8001/cluster/remove \
  -H "Content-Type: application/json" \
  -d '{"nodeId": "node-4"}'
```

## Performance

Benchmarks on a 3-node cluster (local, single machine):

| Operation           | Throughput       | Avg Latency |
|---------------------|------------------|-------------|
| Put (single client) | ~2,000 ops/sec   | ~500µs      |
| Put (concurrent)    | ~5,000 ops/sec   | ~1ms        |
| Get (linearizable)  | ~3,000 ops/sec   | ~300µs      |
| Get (local)         | ~100,000 ops/sec | ~10µs       |

Run benchmarks yourself:

```bash
make test-chaos   # Chaos tests
go test -bench=. ./internal/bench/ -benchtime=5s
```

## Project Structure
```
quorum/
├── cmd/quorum/           # Entry point
├── internal/
│   ├── raft/             # Raft consensus implementation
│   │   ├── node.go       # Core node logic
│   │   ├── server.go     # RPC handlers
│   │   ├── client.go     # Outbound RPCs
│   │   ├── persist.go    # Persistence layer
│   │   └── ...
│   ├── kv/               # Key-value store
│   │   ├── store.go      # State machine
│   │   └── http.go       # HTTP API
│   ├── chaos/            # Chaos tests
│   └── bench/            # Benchmarks
├── pkg/logger/           # Structured logging
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Testing

```bash
# Unit and integration tests
make test

# With race detector
make test-race

# Chaos tests (takes ~2 min)
make test-chaos

# Benchmarks
go test -bench=. ./internal/bench/
```

## Raft Implementation Notes

This implementation follows [the Raft paper](https://raft.github.io/raft.pdf) closely:

- **Figure 2**: State, RPCs, and rules implemented as specified
- **Section 5**: Leader election with randomized timeouts (150-300ms)
- **Section 6**: Log compaction via snapshots
- **Section 7**: Membership changes (single-server)

Key design decisions:

1. **Persistence**: JSON files with atomic writes (rename)
2. **Snapshots**: Triggered every 100 entries
3. **Linearizable reads**: ReadIndex protocol (not read through Raft log)
4. **RPC**: Go's `net/rpc` for simplicity

## Resources

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://thesecretlivesofdata.com/raft/)
- [MIT 6.824 Labs](https://pdos.csail.mit.edu/6.824/)
- [etcd/raft](https://github.com/etcd-io/raft)

## License

MIT
