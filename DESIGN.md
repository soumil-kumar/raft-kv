# Design Decisions

This document outlines the core technical decisions made while building `raft-kv` and implementing the Raft consensus algorithm from scratch.

## 1. Why implement Raft from scratch?

While using `hashicorp/raft` or `etcd/raft` is the correct choice for production systems, I chose to implement Raft from scratch. Understanding a consensus algorithm academically is very different from dealing with the concurrent realities of goroutines, channels, and network delays. Building it from the ground up solidified my understanding of distributed systems, specifically handling split-brain scenarios and log matching invariants.

## 2. Standard `net/rpc` vs gRPC

**Decision**: I used Go's built-in `net/rpc` instead of gRPC or a raw TCP protocol.

**Reasoning**: `net/rpc` is lightweight, requires no code generation (like Protobuf), and natively supports Go structs. This kept the `raft` package focused entirely on algorithmic logic rather than serialization boilerplate. This is the same approach used in MIT's 6.824 Distributed Systems labs.

## 3. Persistence Mechanism

**Decision**: The Raft state (`CurrentTerm`, `VotedFor`, and the `Log`) is serialized to a local JSON file on disk.

**Reasoning**: The Raft paper dictates that a node must persist these three pieces of state before responding to RPCs. While a real database (like BoltDB or RocksDB) would be faster for log appends, writing to a JSON file keeps the implementation dependencies to zero. It prioritizes algorithmic clarity over raw disk I/O performance.

## 4. Leader Forwarding

**Decision**: If a client sends a `PUT` request to a Follower, the Follower transparently forwards the HTTP request to the Leader.

**Reasoning**: This abstracts the cluster topology away from the client. The client doesn't need to implement "service discovery" or know who the current leader is; they can treat any node in the cluster as a valid endpoint. This is exactly how HashiCorp Consul handles client requests.

## 5. Stale vs Consistent Reads

**Decision**: By default, reads are served directly from the local in-memory state of the node that receives the request (Stale read). Clients can opt-in via `?consistent=true`.

**Reasoning**: Stale reads are infinitely faster (no network hops) but might return outdated data if the node is partitioned. For a true consistent read, a node must verify it is still the leader by communicating with a quorum (preventing reads from a deposed leader in a minority partition). I provided the option because different applications have different consistency tolerances (e.g., CAP theorem trade-offs).
