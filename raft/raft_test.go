package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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

// Regression: after a snapshot, candidates advertised the length of their
// in-memory log rather than its absolute last index, so nobody could ever win
// an election again.
func TestFailoverAfterSnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold, cfg.TrailingLogs = 50, 5 })
	for i := range 300 {
		c.write(fmt.Sprint(i))
	}
	for _, n := range c.live() {
		eventually(t, 5*time.Second, n.id+" snapshot", func() bool { return n.raft.Status().SnapshotIndex > 0 })
	}
	for i := range 3 {
		old := c.leader().id
		c.crash(old)
		c.write(fmt.Sprint("failover-", i))
		c.start(old)
	}
	for _, id := range ids(3) {
		c.crash(id)
	}
	for _, id := range ids(3) {
		c.start(id)
	}
	c.write("end")
	if got := c.converge(); len(got) != 304 {
		t.Fatalf("applied %d commands, want 304", len(got))
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

// Regression: only the first chunk of a snapshot larger than the chunk size was
// installed, yet the follower was considered caught up.
func TestInstallMultiChunkSnapshot(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs, cfg.SnapshotChunk = 100, 5, 512
	})
	lag := c.others(c.leader().id)[0]
	c.write("first")
	c.crash(lag)
	for i := range 400 {
		c.write(fmt.Sprintf("value-%04d-xxxxxxxxxxxxxxxxxxxxxxxx", i))
	}
	c.start(lag)
	if got := c.converge(); len(got) != 401 {
		t.Fatalf("got %d commands", len(got))
	}
	if n := c.nodes[lag].raft.Status().Installed; n == 0 {
		t.Fatal("test did not exercise InstallSnapshot")
	}
	// The installed state must itself survive a restart and later appends.
	c.crash(lag)
	c.write("x")
	c.start(lag)
	c.write("y")
	if got := c.converge(); len(got) != 403 {
		t.Fatalf("after restart got %d commands", len(got))
	}
}

func TestInstallSnapshotOverLossyNetwork(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs, cfg.SnapshotChunk = 50, 5, 1024
	})
	lag := c.others(c.leader().id)[0]
	c.crash(lag)
	for i := range 300 {
		c.write(fmt.Sprintf("value-%04d", i))
	}
	c.net.mu.Lock()
	c.net.dropProb, c.net.maxDelay = 0.15, 5*time.Millisecond
	c.net.mu.Unlock()
	c.start(lag)
	if got := c.converge(); len(got) < 300 {
		t.Fatalf("got %d commands", len(got))
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

// Regression: entries queued for disk while a snapshot was taken were written
// twice, so a restarted leader had a shifted log.
func TestSnapshotUnderLoadThenRestart(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold, cfg.TrailingLogs = 40, 10 })
	c.leader()
	var wg sync.WaitGroup
	for w := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 40 {
				c.write(fmt.Sprintf("%d-%d", w, i))
			}
		}()
	}
	wg.Wait()
	c.converge()
	for _, id := range ids(3) {
		last := c.nodes[id].raft.Status().LastIndex
		eventually(t, 5*time.Second, "durable log", func() bool { return c.nodes[id].raft.Status().DurableIndex == last })
		c.crash(id)
		c.start(id)
		// Compare before anything new is replicated: entry indices must line up.
		if got := c.nodes[id].raft.Status().LastIndex; got < last {
			t.Fatalf("%s lost entries across restart: %d < %d", id, got, last)
		}
	}
	c.write("end")
	if got := c.converge(); len(got) != 801 {
		t.Fatalf("got %d commands", len(got))
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

func TestIsolatedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	c.net.partition([]string{l.id}, c.others(l.id))
	eventually(t, 2*time.Second, "check-quorum step down", func() bool { return !l.raft.IsLeader() })
	if err := propose(l, "x", time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

func TestLostLeadershipReportsUnknownOutcome(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	c.net.partition([]string{l.id}, c.others(l.id))
	start := time.Now()
	if err := propose(l, "x", 10*time.Second); !errors.Is(err, ErrLeadershipLost) {
		t.Fatalf("got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("caller waited %v", d)
	}
}

func TestMinorityCannotCommitOrRead(t *testing.T) {
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
	for _, id := range minority {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		if err := c.nodes[id].raft.LinearizableRead(ctx); err == nil {
			t.Fatalf("%s served a linearizable read from the minority", id)
		}
		cancel()
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

// Regression: reads failed with "lost quorum" whenever followers trailed the
// leader's newest entry, which is always true under write load.
func TestReadsSucceedUnderWriteLoad(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.leader()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					c.write(fmt.Sprint(i))
				}
			}
		}()
	}
	for i := range 300 {
		n := c.nodes[ids(3)[i%3]] // leader and followers alike
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := n.raft.LinearizableRead(ctx); err != nil {
			t.Errorf("read %d on %s: %v", i, n.id, err)
		}
		cancel()
	}
	close(stop)
	wg.Wait()
}

// Regression: ReadIndex waited for every peer, so one hung follower cost a full
// RPC timeout per read.
func TestFrozenFollowerDoesNotSlowReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	l := c.leader()
	c.net.freeze(c.others(l.id)[0], true)
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	for range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := l.raft.LinearizableRead(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	if d := time.Since(start) / 50; d > 30*time.Millisecond {
		t.Fatalf("reads took %v each", d)
	}
}

func TestReadsDoNotWaitForFollowerDisks(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	l := c.leader()
	for _, id := range c.others(l.id) {
		c.nodes[id].stall.Store(true)
	}
	go propose(l, "stuck", time.Second) // leaves followers busy in fsync
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := l.raft.LinearizableRead(ctx); err != nil {
		t.Fatalf("read blocked behind a follower fsync: %v", err)
	}
	for _, id := range c.others(l.id) {
		c.nodes[id].stall.Store(false)
	}
}

// Every read must see every write acknowledged before it started, on any node.
func TestFollowerReadsSeeAcknowledgedWrites(t *testing.T) {
	c := newCluster(t, 3, nil)
	for i := range 60 {
		c.write(fmt.Sprint(i))
		for _, n := range c.live() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := n.raft.LinearizableRead(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if got := len(n.sm.commands()); got < i+1 {
				t.Fatalf("%s saw %d of %d acknowledged writes", n.id, got, i+1)
			}
		}
	}
}

// A fresh leader must not serve reads until it has committed in its own term.
func TestNewLeaderReadsWaitForItsFirstCommit(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	r := c.leader().raft
	r.mu.Lock()
	save := r.termStart
	r.termStart = r.log.lastIndex() + 1 // as if the no-op had not committed yet
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := r.LinearizableRead(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	r.mu.Lock()
	r.termStart = save
	r.mu.Unlock()
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	l := c.leader()
	target := c.others(l.id)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := l.raft.TransferLeadership(ctx, target); err != nil {
		t.Fatal(err)
	}
	if got := c.leader().id; got != target {
		t.Fatalf("leader is %s, want %s", got, target)
	}
	if d := time.Since(start); d > 140*time.Millisecond {
		t.Logf("transfer took %v (an election timeout is 150ms)", d)
	}
	c.write("b")
	c.converge()
}

func TestTransferToDeadNodeTimesOutAndRecovers(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	dead := c.others(l.id)[0]
	c.crash(dead)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := l.raft.TransferLeadership(ctx, dead); err == nil {
		t.Fatal("transfer to a dead node succeeded")
	}
	c.write("still works")
}

func TestMembershipGrowAndShrink(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.SnapshotThreshold, cfg.TrailingLogs = 50, 5 })
	for i := range 200 {
		c.write(fmt.Sprint(i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, id := range []string{"n4", "n5"} {
		c.nodes[id] = &node{id: id, dir: t.TempDir()}
		c.startWith(id, Configuration{}) // joins empty
		if err := c.leader().raft.AddLearner(ctx, Member{ID: id, RaftAddr: id}); err != nil {
			t.Fatal(err)
		}
		if st := c.leader().raft.Status(); len(st.Members) < 4 {
			t.Fatalf("members %v", st.Members)
		}
		eventually(t, 10*time.Second, "promotion of "+id, func() bool {
			return c.leader().raft.Promote(ctx, id) == nil
		})
	}
	c.converge()

	// Five voters tolerate two failures.
	l := c.leader().id
	down := c.others(l)[:2]
	c.crash(down[0])
	c.crash(down[1])
	c.write("two down")
	c.start(down[0])
	c.start(down[1])

	// Remove the leader itself, then one more.
	old := c.leader()
	if err := old.raft.RemoveMember(ctx, old.id); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "removed leader to step down", func() bool { return !old.raft.IsLeader() })
	nl := c.leader(c.others(old.id)...)
	if _, ok := (Configuration{Members: nl.raft.Status().Members}).Find(old.id); ok {
		t.Fatal("removed member still listed")
	}
	c.write("after removal")
	term := nl.raft.Status().Term
	time.Sleep(600 * time.Millisecond)
	if got := c.leader(c.others(old.id)...).raft.Status().Term; got != term {
		t.Fatalf("removed node disrupted the cluster: term %d -> %d", term, got)
	}
	c.crash(old.id)
	delete(c.nodes, old.id)

	// Membership must survive a full restart, including through snapshots.
	for i := range 100 {
		c.write(fmt.Sprint("more-", i))
	}
	for _, n := range c.live() {
		c.crash(n.id)
	}
	for id := range c.nodes {
		c.startWith(id, Configuration{})
	}
	c.write("restarted")
	if got := len(c.leader().raft.Status().Members); got != 4 {
		t.Fatalf("%d members after restart, want 4", got)
	}
	c.converge()
}

func TestMembershipGuards(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.leader()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.raft.AddLearner(ctx, Member{ID: "n1", RaftAddr: "dup"}); err == nil {
		t.Fatal("duplicate id accepted")
	}
	if err := l.raft.Promote(ctx, "ghost"); err == nil {
		t.Fatal("promoted a non-member")
	}
	if err := l.raft.AddLearner(ctx, Member{ID: "n9", RaftAddr: "nowhere"}); err != nil {
		t.Fatal(err)
	}
	for i := range promoteLag + 10 {
		c.write(fmt.Sprint(i))
	}
	if err := l.raft.Promote(ctx, "n9"); err == nil {
		t.Fatal("promoted a learner that has nothing")
	}
	// While one change is uncommitted, a second must be refused.
	c.net.partition([]string{l.id}, c.others(l.id))
	go l.raft.RemoveMember(ctx, "n9")
	time.Sleep(30 * time.Millisecond)
	if err := l.raft.AddLearner(ctx, Member{ID: "n8", RaftAddr: "n8"}); !errors.Is(err, ErrConfigPending) {
		t.Fatalf("got %v", err)
	}
	c.net.heal()
}

func TestBackpressure(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) {
		cfg.MaxUncommittedBytes = 1000
		cfg.ElectionTimeout = 2 * time.Second // stay leader while cut off
	})
	l := c.leader()
	c.net.partition([]string{l.id}, c.others(l.id))
	var refused atomic.Int32
	for range 50 {
		if err := propose(l, string(make([]byte, 100)), 5*time.Millisecond); errors.Is(err, ErrOverloaded) {
			refused.Add(1)
		}
	}
	if refused.Load() == 0 {
		t.Fatal("leader queued unbounded uncommitted data")
	}
}

// TestChaos runs concurrent writers through random partitions, crashes with
// unsynced data loss, message loss and delay, then checks that no acknowledged
// write vanished or was applied out of agreement. Safety invariants are checked
// on every apply and election throughout.
func TestChaos(t *testing.T) {
	seeds := 3
	if testing.Short() {
		seeds = 1
	}
	for s := range seeds {
		for _, size := range []int{3, 5} {
			t.Run(fmt.Sprintf("%d nodes/%d", size, s), func(t *testing.T) { chaos(t, size, 6*time.Second) })
		}
	}
}

func chaos(t *testing.T, size int, d time.Duration) {
	c := newCluster(t, size, func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs, cfg.SnapshotChunk = 60, 20, 1024
	})
	c.net.mu.Lock()
	c.net.dropProb, c.net.maxDelay = 0.03, 4*time.Millisecond
	c.net.mu.Unlock()

	var nodesMu sync.RWMutex // guards node restarts against writers
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ackMu sync.Mutex
	acked := map[string]bool{}
	for w := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				cmd := fmt.Sprintf("w%d-%d", w, i)
				nodesMu.RLock()
				for _, n := range c.live() {
					if n.raft.IsLeader() && propose(n, cmd, 300*time.Millisecond) == nil {
						ackMu.Lock()
						acked[cmd] = true
						ackMu.Unlock()
						break
					}
				}
				nodesMu.RUnlock()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	all := ids(size)
	for end := time.Now().Add(d); time.Now().Before(end); {
		time.Sleep(time.Duration(100+rand.IntN(250)) * time.Millisecond)
		nodesMu.Lock()
		switch rand.IntN(4) {
		case 0:
			rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
			k := 1 + rand.IntN(size-1)
			c.net.partition(all[:k], all[k:])
		case 1:
			c.net.heal()
		case 2: // crash anything from one node to the whole cluster
			for _, n := range c.live() {
				if rand.IntN(2) == 0 {
					n.stall.Store(rand.IntN(2) == 0) // maybe mid-fsync
					c.crash(n.id)
					n.stall.Store(false)
				}
			}
		case 3:
			for _, id := range ids(size) {
				if c.nodes[id].raft == nil {
					c.start(id)
				}
			}
		}
		nodesMu.Unlock()
	}
	close(stop)
	wg.Wait()
	c.net.heal()
	c.net.mu.Lock()
	c.net.dropProb = 0
	c.net.mu.Unlock()
	for _, id := range ids(size) {
		if c.nodes[id].raft == nil {
			c.start(id)
		}
	}
	c.write("final")
	final := map[string]int{}
	for _, cmd := range c.converge() {
		final[cmd]++
	}
	for cmd := range acked {
		if final[cmd] == 0 {
			t.Errorf("acknowledged write %s was lost", cmd)
		}
	}
	for cmd, n := range final {
		if n > 1 && cmd != "final" {
			t.Errorf("%s applied %d times", cmd, n)
		}
	}
	t.Logf("%d acknowledged, %d applied, %d terms had leaders", len(acked), len(final), len(c.leaders))
}

// A leader that was cut off still believes it leads until check-quorum fires.
// In that window only the ReadIndex heartbeat round prevents a stale read.
func TestPartitionedLeaderCannotServeReads(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.write("a")
	l := c.leader()
	c.net.partition([]string{l.id}, c.others(l.id))
	if !l.raft.IsLeader() {
		t.Skip("stepped down before the read")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.raft.LinearizableRead(ctx); err == nil {
		t.Fatal("cut-off leader served a linearizable read")
	}
}
