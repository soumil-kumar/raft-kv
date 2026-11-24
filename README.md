# raft-kv

A replicated, linearizable key-value store in Go, built on a Raft implementation
written from scratch. The server uses only the standard library; the one
dependency, [Porcupine](https://github.com/anishathalye/porcupine), is used by
tests to check linearizability.

## What it does

- **Raft**: pre-vote, leader election, log replication with fast conflict
  backtracking, check-quorum, leadership transfer, chunked snapshot transfer,
  and single-server membership changes with non-voting learners.
- **Durability**: a checksummed write-ahead log with group commit. Nothing is
  acknowledged before it is fsynced on a majority, the leader's own copy included.
- **Linearizable reads on every node** via ReadIndex, without writing to the log
  or waiting for any disk. `?stale=true` opts out.
- **Exactly-once retries** per client session, and compare-and-set.
- **Any node takes any request**; writes are forwarded to the leader.
- `/status`, `/metrics` (Prometheus), `/healthz`, `/readyz`, graceful shutdown.

[docs/DESIGN.md](docs/DESIGN.md) explains the decisions and their limits;
[docs/TESTING.md](docs/TESTING.md) explains how correctness is checked.

## Quick start

```bash
go build -o raft-kv.exe .        # drop .exe outside Windows
```

Run each of these in its own terminal:

```bash
./raft-kv.exe --node-id=n1 --http-addr=127.0.0.1:8001 --raft-addr=127.0.0.1:9001 --peers=n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --peer-http=n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003
./raft-kv.exe --node-id=n2 --http-addr=127.0.0.1:8002 --raft-addr=127.0.0.1:9002 --peers=n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --peer-http=n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003
./raft-kv.exe --node-id=n3 --http-addr=127.0.0.1:8003 --raft-addr=127.0.0.1:9003 --peers=n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --peer-http=n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003
```

```bash
curl -X PUT 127.0.0.1:8002/store/name -d '{"value": "raft"}'
curl 127.0.0.1:8003/store/name
curl -X DELETE 127.0.0.1:8001/store/name
curl 127.0.0.1:8001/status
```

Kill the leader (`"is_leader":true` in `/status`) and repeat: the other two carry on.

With Docker: `cd docker && docker compose up -d --build --wait`. The API is on
ports 8001 to 8003 and each node's data is in its own volume.

## API

| Request | Meaning |
|---|---|
| `GET /store/{key}` | Linearizable read; `?stale=true` reads local state |
| `PUT /store/{key}` `{"value":"..."}` | Write; `?if_version=N` makes it conditional (0 = must not exist) |
| `DELETE /store/{key}` | Delete; also takes `if_version` |
| `GET /store?prefix=&limit=` | Ordered scan |
| `GET /cluster/members` | Membership |
| `POST /cluster/members` `{"id","raft_addr","http_addr"}` | Add a learner (start it with `--join`) |
| `POST /cluster/members/{id}/promote` | Make a caught-up learner a voter |
| `DELETE /cluster/members/{id}` | Remove a member |
| `POST /cluster/transfer?target=` | Hand over leadership |

Errors say whether retrying is safe. **503** means nothing happened. **504
`unknown_outcome`** means a write may or may not have taken effect: retry it with
the same `X-Client-Id` and `X-Sequence-Num` and it runs at most once. **412** is a
version mismatch and **409** a stale sequence number.

## Performance

Measured with `cmd/raft-bench` on one laptop (Ryzen 7 4800H, NVMe, Windows 11).
All three nodes **and** the load generator share that machine and disk, so these
are comparisons, not capacity figures. 100-byte values, 4–5 s rounds; the range is
over rounds. No request failed.

| Workload | Workers | Requests/s | p50 ms | p99 ms |
|---|---|---|---|---|
| Write to leader | 1 | 131–165 | 5.9–6.0 | 11–38 |
| Write to leader | 10 | 451–916 | 9.6–12 | 26–64 |
| Write to leader | 50 | 2,900–3,353 | 11–12 | 43–79 |
| Write to leader | 100 | 5,151–6,915 | 11–13 | 45–47 |
| Write to leader | 200 | 10,568–13,783 | 12–13 | 44–66 |
| Linearizable read, any node | 100 | 31,876–38,211 | 2.3–2.7 | 7.9–9.9 |
| Stale read, any node | 100 | 59,413 | 1.2 | 7.9 |

- The same disk does about **400 sequential write+fsync per second**. Group commit
  is what lifts 200 workers to more than 25 times that.
- One worker is slower than the raw disk because an acknowledgement needs fsyncs on
  two nodes, which here contend for one drive.
- Writing 1.08 million distinct keys, throughput per 15 s round stayed between
  5,300 and 10,700/s (the first round was the slowest).
- Failover, from `kill -9` of the leader to the first successful write on a
  survivor: **0.79–1.28 s** over 15 trials (election timeout 500–1000 ms).

The same benchmark in Docker (Linux 5.15 under WSL2, same laptop, each node and the
load generator in its own container). This disk does about 245 sequential
write+fsync per second. No request failed.

| Workload | Workers | Requests/s | p50 ms | p99 ms |
|---|---|---|---|---|
| Write to leader | 1 | 107–122 | 8.1–8.5 | 11–40 |
| Write to leader | 10 | 504–611 | 17–19 | 22–43 |
| Write to leader | 50 | 2,027–2,566 | 19–24 | 29–38 |
| Write to leader | 100 | 3,337–3,828 | 24–29 | 46–91 |
| Write to leader | 200 | 3,077–6,349 | 28–50 | 80–416 |
| Linearizable read, any node | 100 | 22,963–25,263 | 3.3–3.5 | 13–16 |

In Docker, clients of a surviving node saw writes stop for 0.17–0.33 s when the leader
got `SIGTERM` (it hands over first) and 1.6–1.7 s when it got `SIGKILL`. The latter is
longer than on one host because a dead container's address does not refuse
connections, so a forwarded write waits for the 1 s dial timeout.

Reproduce with `go run ./cmd/raft-bench -leader -c 100 -urls http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003`
and `go run ./cmd/raft-bench -fsync .`.

## Limits

- No TLS or authentication: run it on a trusted network.
- All data must fit in memory, and a snapshot is built and sent from memory.
- A node that loses its disk must **not** restart under its old ID, because it has
  forgotten its votes. Remove it and join a new one.
- One Raft group: no sharding, and no transactions beyond single-key compare-and-set.
- Deduplication remembers the 100,000 most recently active clients.

## License

MIT
