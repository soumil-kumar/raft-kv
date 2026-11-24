package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

var (
	// ErrNotLeader means the request was rejected before anything was appended;
	// it is always safe to retry it on the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrLeadershipLost means the entry was appended but this node stopped being
	// leader before learning its fate. The outcome is unknown: it may or may not
	// commit, so a retry must be deduplicated by the state machine.
	ErrLeadershipLost = errors.New("raft: leadership lost, outcome unknown")
	// ErrOverloaded is returned when too much uncommitted data is queued.
	ErrOverloaded = errors.New("raft: too many uncommitted proposals")
	// ErrTransferring is returned while leadership is being handed off.
	ErrTransferring = errors.New("raft: leadership transfer in progress")
	// ErrConfigPending is returned when a membership change is already running.
	ErrConfigPending = errors.New("raft: a membership change is already in progress")
	// ErrStopped is returned once the node has been stopped or has failed.
	ErrStopped = errors.New("raft: stopped")
)

// EntryType says how a log entry is interpreted when it is applied.
type EntryType uint8

const (
	EntryCommand EntryType = iota // opaque state machine command
	EntryNoop                     // appended by each new leader (Raft §8)
	EntryConfig                   // cluster membership (Raft thesis §4)
)

// LogEntry is one replicated log record. Index is absolute and 1-based.
type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState must be durable before a node answers any RPC that depends on it.
type HardState struct {
	Term     uint64
	VotedFor string
}

// Member is one server in the cluster.
type Member struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
	Learner  bool   `json:"learner,omitempty"`
}

// Configuration is the cluster membership in force at some log index.
type Configuration struct {
	Members []Member `json:"members"`
}

func (c Configuration) clone() Configuration {
	return Configuration{Members: slices.Clone(c.Members)}
}

// Find returns the member with the given ID.
func (c Configuration) Find(id string) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

func (c Configuration) isVoter(id string) bool {
	m, ok := c.Find(id)
	return ok && !m.Learner
}

func (c Configuration) voters() int {
	n := 0
	for _, m := range c.Members {
		if !m.Learner {
			n++
		}
	}
	return n
}

// quorum is the number of voters that forms a majority.
func (c Configuration) quorum() int { return c.voters()/2 + 1 }

// Validate rejects configurations that could never work.
func (c Configuration) Validate() error {
	ids, addrs := map[string]bool{}, map[string]bool{}
	for _, m := range c.Members {
		if m.ID == "" || m.RaftAddr == "" {
			return fmt.Errorf("member %+v: id and raft address are required", m)
		}
		if ids[m.ID] {
			return fmt.Errorf("duplicate member id %q", m.ID)
		}
		if addrs[m.RaftAddr] {
			return fmt.Errorf("duplicate raft address %q", m.RaftAddr)
		}
		ids[m.ID], addrs[m.RaftAddr] = true, true
	}
	if c.voters() == 0 {
		return errors.New("configuration has no voters")
	}
	return nil
}

func (c Configuration) encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // plain strings and bools: cannot fail
	}
	return b
}

func decodeConfiguration(b []byte) (Configuration, error) {
	var c Configuration
	err := json.Unmarshal(b, &c)
	return c, err
}

// SnapshotMeta identifies the log prefix a snapshot replaces.
type SnapshotMeta struct {
	Index  uint64
	Term   uint64
	Config Configuration
}

// --- RPCs ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
	// PreVote asks "would you vote for me?" without changing any state, so a
	// partitioned node cannot inflate terms (thesis §9.6).
	PreVote bool
	// Transfer bypasses leader stickiness: the leader itself asked for this.
	Transfer bool
}

type RequestVoteReply struct {
	Term    uint64
	Granted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64 // 0 when the follower's log is simply too short
	ConflictIndex uint64
}

// HeartbeatArgs asserts leadership without touching the log or the disk, so a
// slow fsync on a follower can delay neither liveness nor linearizable reads.
// Commit is capped at the index the leader knows the follower holds.
type HeartbeatArgs struct {
	Term     uint64
	LeaderID string
	Commit   uint64
}

type HeartbeatReply struct{ Term uint64 }

type InstallSnapshotArgs struct {
	Term     uint64
	LeaderID string
	Meta     SnapshotMeta
	Offset   uint64
	Data     []byte
	Done     bool
}

type InstallSnapshotReply struct {
	Term uint64
	// Offset is the next byte the follower expects.
	Offset uint64
	// Installed is set when the follower already holds this prefix durably.
	Installed bool
}

type TimeoutNowArgs struct {
	Term     uint64
	LeaderID string
}

type TimeoutNowReply struct{ Term uint64 }

type ReadIndexArgs struct{ From string }

type ReadIndexReply struct{ Index uint64 }
