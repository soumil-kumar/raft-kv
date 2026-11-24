package raft

import (
	"fmt"
	"testing"
	"time"
)

// These tests drive one node's RPC handlers directly, with no peers running and
// no elections of its own, so each Raft rule can be checked in isolation.

func solo(t *testing.T) (*cluster, *node) {
	t.Helper()
	c := newCluster(t, 1, nil) // replaced below with a 3-voter configuration
	c.crash("n1")
	c.boot = Configuration{}
	for _, id := range ids(3) {
		c.boot.Members = append(c.boot.Members, Member{ID: id, RaftAddr: id})
	}
	c.leaders = map[uint64]string{}
	c.tune = func(cfg *Config) { cfg.ElectionTimeout, cfg.RPCTimeout = time.Hour, 50*time.Millisecond }
	c.nodes["n1"] = &node{id: "n1", dir: t.TempDir()}
	return c, c.start("n1")
}

func entries(from uint64, terms ...uint64) []LogEntry {
	var out []LogEntry
	for i, tm := range terms {
		out = append(out, ent(from+uint64(i), tm))
	}
	return out
}

func appendTo(t *testing.T, r *Raft, a *AppendEntriesArgs) *AppendEntriesReply {
	t.Helper()
	if a.LeaderID == "" {
		a.LeaderID = "n2"
	}
	rep, err := r.HandleAppendEntries(a)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func logTerms(r *Raft) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uint64
	for _, e := range r.log.entries {
		out = append(out, e.Term)
	}
	return fmt.Sprint(out)
}

func TestAppendEntriesConsistencyCheck(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	if rep := appendTo(t, r, &AppendEntriesArgs{Term: 4, PrevLogIndex: 0, Entries: entries(1, 1, 1, 2, 2, 2, 4)}); !rep.Success {
		t.Fatal("first append refused")
	}
	cases := []struct {
		name            string
		prev, prevTerm  uint64
		conflictTerm    uint64
		conflictIndex   uint64
		leaderTerm      uint64
		expectedSuccess bool
	}{
		{"gap", 9, 4, 0, 7, 4, false},
		{"term mismatch points at start of term", 5, 3, 2, 3, 4, false},
		{"first-term mismatch", 2, 9, 1, 1, 4, false},
		{"stale term", 6, 4, 0, 0, 3, false},
		{"match", 6, 4, 0, 0, 4, true},
		{"match in the middle", 3, 2, 0, 0, 4, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := appendTo(t, r, &AppendEntriesArgs{Term: tc.leaderTerm, PrevLogIndex: tc.prev, PrevLogTerm: tc.prevTerm})
			if rep.Success != tc.expectedSuccess || rep.ConflictTerm != tc.conflictTerm || rep.ConflictIndex != tc.conflictIndex {
				t.Fatalf("got %+v", rep)
			}
		})
	}
	if got := logTerms(r); got != "[1 1 2 2 2 4]" {
		t.Fatalf("heartbeat-like appends changed the log: %s", got)
	}
}

func TestAppendEntriesOverwritesOnlyConflicts(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1, 1, 1, 1)})
	// A delayed duplicate that is a prefix must not truncate what follows.
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1, 1)})
	if got := logTerms(r); got != "[1 1 1 1]" {
		t.Fatalf("stale duplicate truncated the log: %s", got)
	}
	appendTo(t, r, &AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(3, 3)})
	if got := logTerms(r); got != "[1 1 3]" {
		t.Fatalf("got %s", got)
	}
}

func TestCommitNeverPassesWhatTheLeaderConfirmed(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1, 1, 1, 1)})
	// Entries 3 and 4 may conflict with the new leader; only 1-2 are confirmed.
	appendTo(t, r, &AppendEntriesArgs{Term: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 4})
	if got := r.Status().CommitIndex; got != 2 {
		t.Fatalf("committed %d, want 2", got)
	}
}

func TestNodeRefusesToOverwriteCommittedEntry(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1, 1), LeaderCommit: 2})
	if _, err := r.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: "n3", PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 2)}); err == nil {
		t.Fatal("overwrote a committed entry")
	}
	eventually(t, time.Second, "fail-stop", func() bool { return n.fatal.Load() != nil })
}

func TestFollowerAcksOnlyDurableEntries(t *testing.T) {
	_, n := solo(t)
	n.stall.Store(true)
	done := make(chan *AppendEntriesReply, 1)
	go func() {
		rep, _ := n.raft.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "n2", Entries: entries(1, 1)})
		done <- rep
	}()
	select {
	case <-done:
		t.Fatal("acknowledged an entry before fsync")
	case <-time.After(100 * time.Millisecond):
	}
	n.stall.Store(false)
	if rep := <-done; rep == nil || !rep.Success {
		t.Fatalf("got %+v", rep)
	}
}

func TestVoteIsDurableBeforeItIsGranted(t *testing.T) {
	c, n := solo(t)
	n.stall.Store(true)
	done := make(chan *RequestVoteReply, 1)
	go func() {
		rep, _ := n.raft.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n2"})
		done <- rep
	}()
	select {
	case <-done:
		t.Fatal("granted a vote before fsync")
	case <-time.After(100 * time.Millisecond):
	}
	n.stall.Store(false)
	if rep := <-done; rep == nil || !rep.Granted {
		t.Fatalf("got %+v", rep)
	}
	c.crash("n1")
	n = c.start("n1")
	if rep, err := n.raft.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n3"}); err != nil || rep.Granted {
		t.Fatalf("voted twice in term 5 across a restart: %+v %v", rep, err)
	}
	if rep, err := n.raft.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "n2"}); err != nil || !rep.Granted {
		t.Fatalf("refused to repeat its own vote: %+v %v", rep, err)
	}
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 3, Entries: entries(1, 1, 2, 2)})
	r.mu.Lock()
	r.leaderID = "" // no stickiness
	r.mu.Unlock()
	for i, tc := range []struct {
		index, term uint64
		want        bool
	}{
		{2, 2, false}, // same term, shorter: the original election bug
		{9, 1, false}, // longer but older
		{3, 2, true},
		{1, 3, true}, // shorter but newer
	} {
		rep, err := r.HandleRequestVote(&RequestVoteArgs{Term: 10 + uint64(i), CandidateID: "n2", LastLogIndex: tc.index, LastLogTerm: tc.term, PreVote: true})
		if err != nil || rep.Granted != tc.want {
			t.Errorf("last (%d,%d): granted=%v want %v", tc.index, tc.term, rep.Granted, tc.want)
		}
	}
	if st := r.Status(); st.Term != 3 {
		t.Fatalf("pre-votes changed the term to %d", st.Term)
	}
}

func TestLeaderStickiness(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1)})
	req := RequestVoteArgs{Term: 9, CandidateID: "n3", LastLogIndex: 99, LastLogTerm: 9}
	if rep, _ := r.HandleRequestVote(&req); rep.Granted || r.Status().Term != 1 {
		t.Fatal("helped depose a live leader")
	}
	req.Transfer = true
	if rep, _ := r.HandleRequestVote(&req); !rep.Granted {
		t.Fatal("refused a leader-initiated transfer")
	}
}

// Figure 8 of the Raft paper: an entry from an earlier term must not be
// committed by counting replicas, however many hold it.
func TestLeaderCommitsOnlyItsOwnTerm(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 2, Entries: entries(1, 2)})
	r.mu.Lock()
	r.term = 4
	r.persistHardState()
	r.becomeLeader() // appends its no-op at index 2
	r.waitDurable(r.walSeq)
	for _, p := range r.peers {
		p.match = 1
	}
	r.advanceCommit()
	early := r.commitIndex
	r.peers["n2"].match = 2
	r.advanceCommit()
	final := r.commitIndex
	r.mu.Unlock()
	if early != 0 {
		t.Fatalf("committed term-2 entry %d in term 4 by counting replicas", early)
	}
	if final != 2 {
		t.Fatalf("own-term entry should commit both: %d", final)
	}
}

func TestLearnersDoNotCount(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	conf := r.conf().clone()
	conf.Members = append(conf.Members,
		Member{ID: "l1", RaftAddr: "l1", Learner: true}, Member{ID: "l2", RaftAddr: "l2", Learner: true})
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: []LogEntry{{Index: 1, Term: 1, Type: EntryConfig, Data: conf.encode()}}})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.term = 2
	r.persistHardState()
	r.becomeLeader()
	r.waitDurable(r.walSeq)
	r.peers["l1"].match, r.peers["l2"].match = 2, 2
	r.advanceCommit()
	if r.commitIndex != 0 {
		t.Fatalf("learners formed a commit quorum: %d", r.commitIndex)
	}
	r.campaignToken++
	r.role = Candidate
	r.votes = map[string]bool{"n1": true, "l1": true, "l2": true}
	r.tally(r.campaignToken)
	if r.role == Leader {
		t.Fatal("learners elected a leader")
	}
	r.role = Leader // let cleanup stop the replication goroutines
}

func TestConfigurationRollsBackWithItsEntry(t *testing.T) {
	c, n := solo(t)
	r := n.raft
	bigger := r.conf().clone()
	bigger.Members = append(bigger.Members, Member{ID: "n4", RaftAddr: "n4"})
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: []LogEntry{{Index: 1, Term: 1, Type: EntryConfig, Data: bigger.encode()}}})
	if len(r.Status().Members) != 4 {
		t.Fatal("configuration must take effect when appended")
	}
	appendTo(t, r, &AppendEntriesArgs{Term: 2, Entries: entries(1, 2)})
	if got := len(r.Status().Members); got != 3 {
		t.Fatalf("truncated configuration still in force: %d members", got)
	}
	c.crash("n1")
	if got := len(c.start("n1").raft.Status().Members); got != 3 {
		t.Fatalf("truncated configuration came back after restart: %d members", got)
	}
}

func TestHeartbeatCommitIsBoundedByTheLog(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	appendTo(t, r, &AppendEntriesArgs{Term: 1, Entries: entries(1, 1, 1)})
	if _, err := r.HandleHeartbeat(&HeartbeatArgs{Term: 1, LeaderID: "n2", Commit: 50}); err != nil {
		t.Fatal(err)
	}
	if got := r.Status().CommitIndex; got != 2 {
		t.Fatalf("commit %d", got)
	}
}

func TestSnapshotChunksOutOfOrder(t *testing.T) {
	_, n := solo(t)
	r := n.raft
	data := []byte(`{"last":5,"cmds":["a","b"]}`)
	meta := SnapshotMeta{Index: 5, Term: 1, Config: r.conf()}
	chunk := func(off, end int, done bool) *InstallSnapshotReply {
		rep, err := r.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 1, LeaderID: "n2", Meta: meta,
			Offset: uint64(off), Data: data[off:end], Done: done})
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	if rep := chunk(10, len(data), true); rep.Offset != 0 || rep.Installed {
		t.Fatalf("accepted a tail with no head: %+v", rep)
	}
	chunk(0, 10, false)
	chunk(10, 20, false)
	if rep := chunk(10, 20, false); rep.Offset != 20 {
		t.Fatalf("duplicate chunk: %+v", rep)
	}
	if st := r.Status(); st.CommitIndex != 0 || st.SnapshotIndex != 0 {
		t.Fatalf("partial snapshot changed state: %+v", st)
	}
	if rep := chunk(20, len(data), true); !rep.Installed {
		t.Fatalf("got %+v", rep)
	}
	eventually(t, time.Second, "restore", func() bool { return r.Status().LastApplied == 5 })
	if got := n.sm.commands(); len(got) != 2 {
		t.Fatalf("restored %v", got)
	}
}
