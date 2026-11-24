# Testing

```bash
go test ./...                                  # everything, about 2.5 minutes
go test -race . ./raft ./store ./httpd         # needs cgo
E2E_SECONDS=120 E2E_RACE=1 go test ./e2e -run Linearizability -v
go test ./raft -run xxx -fuzz FuzzReadFrame -fuzztime 1m
RAFT_TEST_LOG=1 go test ./raft -run TestName -v   # show node logs
```

## Layers

1. **Storage** (`raft/storage_test.go`): the WAL is cut at every byte of its last
   frame and every byte of its acknowledged part is flipped in turn. Each crash window
   of a compaction is reproduced.
2. **Protocol rules** (`raft/protocol_test.go`): one node, RPCs sent by hand. Covers
   the consistency check, Figure 8, vote durability, learners and configuration rollback.
3. **Clusters** (`raft/raft_test.go`): in-process nodes on a simulated network with
   partitions, loss, delay, frozen nodes, stalled disks and crashes that lose a random
   part of the unsynced WAL. Two invariants are checked on every event: at most one
   leader per term, and all nodes apply the same entry at each index.
4. **HTTP** (`httpd/service_test.go`): real sockets and forwarding.
5. **End to end** (`e2e/`): real processes, `kill -9`, `SIGTERM` handover (Unix), proxies
   that cut or stall traffic.

## Linearizability

`e2e/linearizability_test.go` runs 8 clients doing get, put, delete and
compare-and-set against random nodes while processes are killed (singly or all at
once), cut off and stalled. Porcupine then checks the complete history. Clients retry
with the same sequence number until they get a definite answer, so a write that ran
twice is a violation too.

Longest recorded run: 120 s with race-instrumented servers, 49,517 operations, 137
faults, linearizable, no data races.

## Do the tests notice bugs?

25 deliberate bugs were put into `raft/` one at a time (wrong vote index, acknowledging
before fsync, skipping the ReadIndex quorum, not rolling back configurations, and so
on). The suite first missed six of them; tests were added until it caught all 25.
Breaking ReadIndex, deduplication or the election restriction each makes the
linearizability test report an illegal history.

## Bugs found in the first version

An earlier version passed all 32 of its scripted tests, none of which wrote more than
about 20 keys or restarted a node after a conflict. Fault injection found these:

| Bug | Effect | Regression test |
|---|---|---|
| Candidates sent the in-memory log length as `LastLogIndex` | No leader could be elected after the first snapshot | `TestFailoverAfterSnapshot` |
| The same value was one too high before any snapshot | Acknowledged writes lost, 5 of 5 trials | `TestStaleNodeCannotEraseCommittedWrite` |
| First snapshot chunk advanced the commit index | Followers kept 501 of 2,500 keys | `TestInstallMultiChunkSnapshot` |
| Truncation replay off by one | Replicas diverged after a restart | `TestOverwrittenEntryStaysGoneAfterRestart` |
| No WAL checksums or tail repair | Writes after a torn record lost | `TestStorageTornTailAtEveryOffset` |
| ReadIndex required followers to match the newest entry | 16 of 18 reads failed under load | `TestReadsSucceedUnderWriteLoad` |
| ReadIndex waited for every peer | 1 s reads with one hung follower | `TestFrozenFollowerDoesNotSlowReads` |
| Snapshot and log writer both persisted entries | Duplicate WAL entries | `TestSnapshotUnderLoadThenRestart` |
| Leader counted itself before its fsync | Commit without a durable majority | `TestLeaderDoesNotCommitBeforeItsOwnFsync` |
| Stale sequence numbers reported success | Writes silently dropped | `TestStaleSequenceIsReported` |
| A vote was forgotten within its term | Two votes in one term | `TestVoteIsKeptWithinATerm` |
