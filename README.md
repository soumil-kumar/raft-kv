# Raft Key-Value Store

> **Performance**: 1006 requests/sec, 94.3ms avg latency (10,000 requests, 100 concurrency, 3-node cluster, strictly consistent fsync per append)
> *Note on Throughput*: The current architecture strictly calls `fsync()` on every single log append on followers to guarantee Raft safety rules. This disk I/O bottleneck caps throughput. A production system would implement **Group Commit / Batching** on followers to alleviate this and achieve 10,000+ RPS.

A fault-tolerant, distributed key-value store built in Go, featuring an implementation of the Raft consensus protocol **inspired by the architecture of MIT's 6.824 Distributed Systems course**. Data is replicated across a cluster of nodes and survives node failures with zero data loss.

This project was built to deeply understand distributed systems, consensus algorithms, and the challenges of implementing the Raft paper (Diego Ongaro and John Ousterhout) without relying on external libraries like `hashicorp/raft`. The testing harness and core architectural patterns draw heavily from the MIT 6.824 lab structures.

## Key Features

- **Custom Raft Engine**: Full implementation of Leader Election, Log Replication, and Safety rules from the Raft paper.
- **Leader Forwarding**: Write to ANY node — followers transparently proxy writes to the current leader.
- **RPC Communication**: Uses standard Go `net/rpc` for inter-node `AppendEntries` and `RequestVote` calls.
- **Persistence**: Raft state (CurrentTerm, VotedFor, Log) is persisted to disk to survive crashes.
- **Consistent Reads**: Linearizable reads via `?consistent=true`.

## Architecture

Each node in the cluster runs two main components:
1. **The Raft Engine (`raft/`)**: A pure Go implementation of the consensus algorithm. It manages the election timers, handles RPCs, and commits entries to the replicated log.
2. **The KV Store (`store/`)**: An in-memory map that applies commands once the Raft engine confirms they have been committed by a majority of the cluster.

Clients interact with the HTTP API, which translates REST calls into Raft log commands.

## Quick Start

### Build

```bash
go build -o raft-kv.exe .
```

### Run via Docker (Recommended)

The easiest way to boot a fully functional 3-node cluster is using Docker Compose.

```bash
cd docker
docker-compose up -d --build
```
This provisions a persistent 3-node cluster exposed on HTTP ports `8001`, `8002`, and `8003`. 
You can view the logs to see the election process:
```bash
docker-compose logs -f
```

### Start a 3-Node Cluster (Manually)

Start 3 nodes in separate terminals. Because this is a static cluster, we pass all peer addresses on startup.

```bash
# Terminal 1
./raft-kv.exe --node-id=node1 --http-addr=:8001 --raft-addr=:9001 --peers=:9001,:9002,:9003 --peer-http=node1:8001,node2:8002,node3:8003

# Terminal 2
./raft-kv.exe --node-id=node2 --http-addr=:8002 --raft-addr=:9002 --peers=:9001,:9002,:9003 --peer-http=node1:8001,node2:8002,node3:8003

# Terminal 3
./raft-kv.exe --node-id=node3 --http-addr=:8003 --raft-addr=:9003 --peers=:9001,:9002,:9003 --peer-http=node1:8001,node2:8002,node3:8003
```

Within 150-300ms, the nodes will hold an election, and one will become the leader.

### Use It

```bash
# Write data (to any node — followers forward it to the leader)
curl -X PUT localhost:8001/store/name -d '{"value": "IIT KGP"}'

# Read data
curl localhost:8002/store/name
# → {"key":"name","value":"IIT KGP"}

# Delete a key
curl -X DELETE localhost:8001/store/name

# Check node status
curl localhost:8001/status
```

### Fault Tolerance Demo

1. Write a key to the cluster.
2. Look at the terminal logs to identify the Leader.
3. Kill the Leader (Ctrl+C).
4. Watch the logs of the remaining 2 nodes — they will detect the heartbeat timeout and elect a new leader.
5. Read your key from a surviving node. It is still there!

## Performance

The project includes a load-testing benchmark script (`scripts/benchmark.go`) to measure the throughput and latency of the consensus engine under concurrent load. 

**Benchmark Conditions:**
- 3-node local cluster
- 10,000 sequential `PUT` requests
- Concurrency: 100 workers

**Results:**
- **Throughput**: ~138 Requests/sec
- **p50 Latency**: ~2ms
- **p95 Latency**: ~6ms
- **p99 Latency**: ~19ms

## Tech Stack

- **Go 1.24** — Language
- **net/rpc** — Inter-node communication
- **Standard Library** — No external dependencies used for consensus.

## License

MIT
