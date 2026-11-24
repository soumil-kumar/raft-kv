# Design

## One ordered path to disk

`raft.Raft` has one mutex and does no I/O while holding it. Every durable change
(term and vote, log entries, compaction) is appended to one queue in the order it
happened in memory. `walLoop` drains the queue, writes the whole batch as one
frame, and fsyncs once. Anyone who needs durability waits for a sequence number.

This gives three things:

- **Group commit for free.** While one fsync runs, everything else queues up and
  shares the next.
- **Crash consistency.** Disk state is always a prefix of the in-memory history.
  Appends, truncations and compactions cannot interleave wrongly.
- **Simple rules.** RPC replies wait for the queue position at which they were
  decided. The leader's own match index is the durable index.

Entries are sent to followers while the leader's own fsync is in flight (thesis
§10.2.1). This is safe because the leader counts itself only once durable.

A follower can acknowledge entry *e* after having moved to a newer term while
waiting for the fsync. That is still safe: *e* was in its log before it voted in the
newer term, so the election restriction took *e* into account.

## One path to the state machine

`applyLoop` is the only goroutine that calls `Apply` and `Restore`. A received
snapshot is handed to it rather than applied concurrently, so a snapshot can never
overwrite newer commands or the reverse.

## WAL format

`len | crc(len) | crc(payload) | payload`, one frame per fsync. Because nothing in
an unsynced frame was ever acknowledged, an incomplete or mismatching **last** frame
is a torn write and is cut off. A bad checksum anywhere else means acknowledged
data is damaged, and the node refuses to start: silently dropping it could lose a
committed write. An entry at an existing index replaces it and truncates the rest,
so there is no separate truncate record to get wrong.

The two cases that cannot be told apart are a torn last frame and bit rot in a last
frame that had been synced. Both are treated as torn.

Compaction writes a full new generation to a temporary file, fsyncs it, renames it,
and fsyncs the directory. (Windows has no directory fsync; NTFS journals renames.)

A failed fsync stops the node. Retrying is not safe, because the kernel may already
have dropped the dirty pages.

## Heartbeats are separate from replication

Each peer has two goroutines and two connections. `AppendEntries` waits for the
follower's disk; `Heartbeat` does not touch the disk or the log. A slow fsync or a
large snapshot therefore cannot cost the leader its leadership, and linearizable
reads never wait for a disk. A heartbeat carries `min(commitIndex, matchIndex)`, so
the follower can commit without a log check.

## Reads

ReadIndex (thesis §6.4): wait until an entry of the current term has committed, note
the commit index, get a majority to acknowledge a heartbeat sent **after** that, wait
until that index is applied, read. Concurrent reads share heartbeat rounds. Followers
ask the leader for the index and then serve the read themselves.

Lease-based reads would save the round trip but depend on bounded clock drift.

## Elections

Pre-vote keeps a partitioned node from inflating its term. Nodes that hear from a
leader ignore vote requests (leader stickiness), which also stops removed members
from causing elections. Check-quorum makes a leader that has lost its majority step
down, so clients get an answer instead of a hang.

## Membership

Single-server changes: a configuration takes effect when appended, rolls back if
truncated, and only one may be uncommitted at a time. The leader must first have
committed an entry of its own term, which closes the known hole in the algorithm as
first published. New nodes join as learners and are promoted only when nearly caught
up. The bootstrap configuration is stored with the log, so restarts do not depend on
flags.

## Client semantics

A proposal has three outcomes: committed, definitely not executed, or unknown. The
API keeps these apart. Retries carry a client ID and sequence number. The state
machine stores each client's last sequence number **and result**, replicated and
included in snapshots, so a retry gets the original answer. A lower number gets an
error, never a false success. Sessions are evicted in least-recently-applied order,
which is the same on every replica.

## Snapshots

Taken when at least `--snapshot-threshold` entries **and** as many bytes as the last
snapshot have accumulated, which bounds write amplification near 2x at any state
size. The map is cloned on the apply goroutine (this pauses applies) and encoded
elsewhere. Entries behind the snapshot stay in memory, so a slightly slow follower
does not trigger a full transfer.

## Not done

- Pipelined AppendEntries: one batch is in flight per follower.
- Streaming or incremental snapshots.
- Sharding, TLS, authentication.
