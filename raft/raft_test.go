package raft

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestElectionAndReplication(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		t.Run(fmt.Sprint(n, " nodes"), func(t *testing.T) {
			c := newCluster(t, n, nil)
			for i := range 50 {
				c.write(fmt.Sprint(i))
			}
			if got := c.converge(); len(got) != 50 {
				t.Fatalf("applied %d commands", len(got))
			}
		})
	}
}

func TestStableLeaderWithoutFailures(t *testing.T) {
	c := newCluster(t, 3, nil)
	term := c.leader().raft.Status().Term
	time.Sleep(1500 * time.Millisecond) // ten election timeouts
	if got := c.leader().raft.Status().Term; got != term {
		t.Fatalf("term moved from %d to %d with no failures", term, got)
	}
}

func TestFailoverAndRejoin(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	old := c.leader().id
	c.crash(old)
	c.write("b")
	c.start(old)
	c.write("c")
	if got := c.converge(); len(got) != 3 {
		t.Fatalf("got %v", got)
	}
}

func TestFullClusterRestart(t *testing.T) {
	c := newCluster(t, 3, nil)
	for i := range 20 {
		c.write(fmt.Sprint(i))
	}
	for _, id := range ids(3) {
		c.crash(id)
	}
	for _, id := range ids(3) {
		c.start(id)
	}
	c.write("after")
	if got := c.converge(); len(got) != 21 || got[20] != "after" {
		t.Fatalf("got %v", got)
	}
}

// Regression: LastLogIndex was overstated by one, letting a node that missed a
// committed entry win the election and erase it.
func TestStaleNodeCannotEraseCommittedWrite(t *testing.T) {
	for trial := range 5 {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			c := newCluster(t, 3, nil)
			a := c.leader().id
			rest := c.others(a)
			b, stale := rest[0], rest[1]
			c.crash(stale)
			c.write("committed")
			c.crash(a)
			c.crash(b)
			c.start(stale) // the stale node gets a head start on its timer
			time.Sleep(140 * time.Millisecond)
			c.start(b)
			if l := c.leader(); l.id != b {
				t.Fatalf("%s won without the committed entry", l.id)
			}
			c.start(a)
			if got := c.converge(); len(got) != 1 || got[0] != "committed" {
				t.Fatalf("committed write lost: %v", got)
			}
		})
	}
}

// Regression: an overwritten, never-committed entry came back after a restart
// and shifted every later entry by one.
func TestOverwrittenEntryStaysGoneAfterRestart(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("k0")
	a := c.leader().id
	c.net.partition([]string{a}, c.others(a))
	go propose(c.nodes[a], "never-committed", time.Second)
	go propose(c.nodes[a], "never-committed-2", time.Second)
	c.leader(c.others(a)...)
	time.Sleep(50 * time.Millisecond)
	c.net.heal()
	c.write("k1")
	c.write("k2")
	c.converge()
	c.crash(a)
	c.start(a)
	c.write("k3")
	c.write("k4")
	got := c.converge()
	if fmt.Sprint(got) != "[k0 k1 k2 k3 k4]" {
		t.Fatalf("got %v", got)
	}
}

// Regression: the leader counted itself towards the commit quorum before its
// own copy was on disk.
func TestLeaderDoesNotCommitBeforeItsOwnFsync(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	dead := c.others(l.id)[0]
	c.crash(dead) // leader + one follower: both must be durable
	c.write("warm")
	before := l.raft.Status().CommitIndex
	l.stall.Store(true)
	done := make(chan error, 1)
	go func() { done <- propose(l, "x", 5*time.Second) }()
	time.Sleep(300 * time.Millisecond)
	if st := l.raft.Status(); st.CommitIndex != before {
		t.Fatalf("committed %d with the leader's disk stalled", st.CommitIndex)
	}
	l.stall.Store(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCommitNeedsOnlyAMajorityOfDisks(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	slow := c.nodes[c.others(l.id)[0]]
	slow.stall.Store(true)
	for i := range 20 {
		if err := propose(l, fmt.Sprint(i), time.Second); err != nil {
			t.Fatalf("write %d blocked on a slow minority: %v", i, err)
		}
	}
	slow.stall.Store(false)
	c.converge()
}

func TestDiskFailureStopsOnlyThatNode(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	l := c.leader()
	l.crashed.Store(true) // every further write fails before fsync
	propose(l, "doomed", 300*time.Millisecond)
	eventually(t, 3*time.Second, "fatal callback", func() bool { return l.fatal.Load() != nil })
	if err := propose(l, "x", 100*time.Millisecond); !errors.Is(err, ErrStopped) {
		t.Fatalf("failed node accepted a write: %v", err)
	}
	c.leader(c.others(l.id)...)
	c.write("b")
	c.crash(l.id)
	c.start(l.id)
	c.converge()
}

// Regression: a candidate that met a leader of its own term forgot its vote.
func TestVoteIsKeptWithinATerm(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	n := c.nodes[c.others(l.id)[0]]
	c.crash(l.id)
	c.crash(c.others(l.id)[1])
	r := n.raft
	r.mu.Lock()
	r.leaderID = ""
	r.campaign(false, false)
	term, vote := r.term, r.votedFor
	r.mu.Unlock()
	if vote != n.id {
		t.Fatalf("voted for %q", vote)
	}
	if _, err := r.HandleHeartbeat(&HeartbeatArgs{Term: term, LeaderID: "elsewhere"}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.leaderID = "" // drop stickiness so only votedFor can refuse
	lastIndex, lastTerm := r.log.lastIndex(), r.log.lastTerm()
	r.mu.Unlock()
	rep, err := r.HandleRequestVote(&RequestVoteArgs{Term: term, CandidateID: "other", LastLogIndex: lastIndex + 9, LastLogTerm: lastTerm + 9})
	if err != nil || rep.Granted {
		t.Fatalf("second vote in term %d: %+v %v", term, rep, err)
	}
	// ...and across a restart.
	c.crash(n.id)
	c.start(n.id)
	rep, err = c.nodes[n.id].raft.HandleRequestVote(&RequestVoteArgs{Term: term, CandidateID: "other", LastLogIndex: lastIndex + 9, LastLogTerm: lastTerm + 9})
	if err != nil || rep.Granted {
		t.Fatalf("second vote after restart: %+v %v", rep, err)
	}
}

func TestPreVotePreventsTermInflation(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	term := l.raft.Status().Term
	iso := c.others(l.id)[0]
	c.net.partition([]string{iso}, c.others(iso))
	time.Sleep(time.Second)
	if got := c.nodes[iso].raft.Status().Term; got != term {
		t.Fatalf("isolated node raised its term to %d", got)
	}
	c.net.heal()
	time.Sleep(500 * time.Millisecond)
	if st := l.raft.Status(); st.Role != "leader" || st.Term != term {
		t.Fatalf("rejoining node disturbed the leader: %+v", st)
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 5, nil)
	l := c.leader()
	rest := c.others(l.id)
	minority := []string{l.id, rest[0]}
	c.net.partition(minority, rest[1:])
	before := l.raft.Status().CommitIndex
	propose(l, "lost", time.Second)
	if got := l.raft.Status().CommitIndex; got != before {
		t.Fatalf("minority committed %d", got)
	}
	nl := c.leader(rest[1:]...)
	if err := propose(nl, "kept", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	c.net.heal()
	if got := c.converge(); fmt.Sprint(got) != "[kept]" {
		t.Fatalf("got %v", got)
	}
}
