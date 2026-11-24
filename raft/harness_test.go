package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- simulated network ---

type network struct {
	mu       sync.Mutex
	nodes    map[string]*Raft
	blocked  map[[2]string]bool
	frozen   map[string]bool // accepts connections but never answers
	dropProb float64
	maxDelay time.Duration
}

func newNetwork() *network {
	return &network{nodes: map[string]*Raft{}, blocked: map[[2]string]bool{}, frozen: map[string]bool{}}
}

func (n *network) set(id string, r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = r
}

// partition splits the cluster; nodes talk only within their own group.
func (n *network) partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[[2]string]bool{}
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.blocked[[2]string{x, y}] = true
				}
			}
		}
	}
}

func (n *network) heal() { n.partition() }

func (n *network) freeze(id string, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.frozen[id] = on
}

func (n *network) route(from, to string) (r *Raft, frozen, lost bool, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r = n.nodes[to]
	if n.blocked[[2]string{from, to}] {
		r = nil
	}
	if n.maxDelay > 0 {
		delay = rand.N(n.maxDelay)
	}
	return r, n.frozen[to], rand.Float64() < n.dropProb, delay
}

var errNet = errors.New("simulated network failure")

func send[A, R any](ctx context.Context, n *network, from, to string, a *A, h func(*Raft, *A) (*R, error)) (*R, error) {
	r, frozen, lost, delay := n.route(from, to)
	if frozen {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if r == nil || lost {
		return nil, errNet
	}
	time.Sleep(delay)
	type out struct {
		reply *R
		err   error
	}
	ch := make(chan out, 1)
	go func() {
		rep, err := h(r, a)
		ch <- out{rep, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case o := <-ch:
		// The reply can be lost even though the request took effect.
		if back, _, lost, delay := n.route(to, from); back == nil || lost {
			return nil, errNet
		} else if delay > 0 {
			time.Sleep(delay)
		}
		return o.reply, o.err
	}
}

type endpoint struct {
	net  *network
	from string
}

func (e endpoint) RequestVote(ctx context.Context, to Member, a *RequestVoteArgs) (*RequestVoteReply, error) {
	return send(ctx, e.net, e.from, to.ID, a, (*Raft).HandleRequestVote)
}
func (e endpoint) Heartbeat(ctx context.Context, to Member, a *HeartbeatArgs) (*HeartbeatReply, error) {
	return send(ctx, e.net, e.from, to.ID, a, (*Raft).HandleHeartbeat)
}
func (e endpoint) AppendEntries(ctx context.Context, to Member, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return send(ctx, e.net, e.from, to.ID, a, (*Raft).HandleAppendEntries)
}
func (e endpoint) InstallSnapshot(ctx context.Context, to Member, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return send(ctx, e.net, e.from, to.ID, a, (*Raft).HandleInstallSnapshot)
}

// --- state machine ---

// recorder keeps the ordered list of commands it has applied.
type recorder struct {
	c  *cluster
	id string

	mu   sync.Mutex
	last uint64
	cmds []string
}

func (s *recorder) Apply(e LogEntry) any {
	s.c.checkApplied(s.id, e)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Index <= s.last {
		s.c.violation("%s applied %d after %d", s.id, e.Index, s.last)
	}
	s.last = e.Index
	s.cmds = append(s.cmds, string(e.Data))
	return len(s.cmds)
}

func (s *recorder) Snapshot() func() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmds, last := append([]string(nil), s.cmds...), s.last
	return func() ([]byte, error) { return json.Marshal(map[string]any{"last": last, "cmds": cmds}) }
}

func (s *recorder) Restore(b []byte) error {
	var v struct {
		Last uint64   `json:"last"`
		Cmds []string `json:"cmds"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v.Last < s.last {
		s.c.violation("%s restored %d over newer state %d", s.id, v.Last, s.last)
	}
	s.last, s.cmds = v.Last, v.Cmds
	return nil
}

func (s *recorder) commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cmds...)
}

// --- cluster ---

type node struct {
	id      string
	dir     string
	raft    *Raft
	sm      *recorder
	store   *FileStorage
	crashed atomic.Bool
	stall   atomic.Bool // block before fsync
	fatal   atomic.Value
}

type cluster struct {
	t     testing.TB
	net   *network
	nodes map[string]*node
	boot  Configuration
	tune  func(*Config)

	mu         sync.Mutex
	violations []string
	leaders    map[uint64]string   // term -> leader: Election Safety
	applied    map[uint64]LogEntry // index -> entry: State Machine Safety
}

// leaderWatch turns "became leader" log lines into an Election Safety check.
type leaderWatch struct {
	slog.Handler
	c  *cluster
	id string
}

func (h leaderWatch) WithAttrs(a []slog.Attr) slog.Handler {
	return leaderWatch{h.Handler.WithAttrs(a), h.c, h.id}
}

func (h leaderWatch) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Message == "became leader" {
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "term" {
				h.c.noteLeader(a.Value.Uint64(), h.id)
			}
			return true
		})
	}
	return h.Handler.Handle(ctx, rec)
}

func (c *cluster) noteLeader(term uint64, id string) {
	c.mu.Lock()
	prev, ok := c.leaders[term]
	c.leaders[term] = id
	c.mu.Unlock()
	if ok && prev != id {
		c.violation("two leaders in term %d: %s and %s", term, prev, id)
	}
}

func (c *cluster) checkApplied(id string, e LogEntry) {
	c.mu.Lock()
	prev, ok := c.applied[e.Index]
	if !ok {
		c.applied[e.Index] = e
	}
	c.mu.Unlock()
	if ok && (prev.Term != e.Term || string(prev.Data) != string(e.Data)) {
		c.violation("%s applied %q (term %d) at %d; another node applied %q (term %d)",
			id, e.Data, e.Term, e.Index, prev.Data, prev.Term)
	}
}

func (c *cluster) violation(f string, a ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.violations = append(c.violations, fmt.Sprintf(f, a...))
}

func ids(n int) []string {
	var out []string
	for i := range n {
		out = append(out, fmt.Sprintf("n%d", i+1))
	}
	return out
}

func newCluster(t testing.TB, n int, tune func(*Config)) *cluster {
	c := &cluster{
		t: t, net: newNetwork(), nodes: map[string]*node{}, tune: tune,
		leaders: map[uint64]string{}, applied: map[uint64]LogEntry{},
	}
	for _, id := range ids(n) {
		c.boot.Members = append(c.boot.Members, Member{ID: id, RaftAddr: id})
	}
	for _, id := range ids(n) {
		c.nodes[id] = &node{id: id, dir: t.TempDir()}
	}
	for _, id := range ids(n) {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.stall.Store(false)
			if n.raft != nil {
				n.raft.Stop()
			}
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, v := range c.violations {
			t.Errorf("SAFETY VIOLATION: %s", v)
		}
	})
	return c
}

func (c *cluster) start(id string) *node { return c.startWith(id, c.boot) }

func (c *cluster) startWith(id string, boot Configuration) *node {
	c.t.Helper()
	n := c.nodes[id]
	st, err := OpenFileStorage(n.dir)
	if err != nil {
		c.t.Fatal(err)
	}
	st.noSync = true // process crashes are simulated with syncedSize instead
	n.crashed.Store(false)
	st.beforeSync = func() error {
		for n.stall.Load() && !n.crashed.Load() {
			time.Sleep(time.Millisecond)
		}
		if n.crashed.Load() {
			return errors.New("simulated crash before fsync")
		}
		return nil
	}
	n.store = st
	n.sm = &recorder{c: c, id: id}
	var out io.Writer = io.Discard
	if os.Getenv("RAFT_TEST_LOG") != "" {
		out = os.Stderr
	}
	cfg := Config{
		ID: id, Bootstrap: boot,
		ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
		Logger:  slog.New(leaderWatch{slog.NewTextHandler(out, nil), c, id}),
		OnFatal: func(err error) { n.fatal.Store(err) },
	}
	if c.tune != nil {
		c.tune(&cfg)
	}
	r, err := New(cfg, st, endpoint{c.net, id}, n.sm)
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	n.raft = r
	c.net.set(id, r)
	return n
}

// crash stops a node as a power failure would: a random suffix of whatever was
// written but not fsynced disappears.
func (c *cluster) crash(id string) {
	n := c.nodes[id]
	c.net.set(id, nil)
	n.crashed.Store(true)
	n.raft.Stop()
	synced := n.store.syncedSize.Load()
	path := n.store.walPath(n.store.gen)
	if fi, err := os.Stat(path); err == nil && fi.Size() > synced {
		os.Truncate(path, synced+rand.Int64N(fi.Size()-synced+1))
	}
	n.raft = nil
}

func (c *cluster) live() []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.raft != nil {
			out = append(out, n)
		}
	}
	return out
}

func eventually(t testing.TB, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// leader waits for a leader among the given nodes (default: all live ones) that
// has committed in its own term.
func (c *cluster) leader(among ...string) *node {
	c.t.Helper()
	var found *node
	eventually(c.t, 10*time.Second, "a leader", func() bool {
		found = nil
		for _, n := range c.live() {
			if len(among) > 0 && !contains(among, n.id) {
				continue
			}
			n.raft.mu.Lock()
			ok := n.raft.role == Leader && n.raft.commitIndex >= n.raft.termStart
			term := n.raft.term
			n.raft.mu.Unlock()
			if ok && (found == nil || term > found.raft.Status().Term) {
				found = n
			}
		}
		return found != nil
	})
	return found
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func (c *cluster) others(id string) []string {
	var out []string
	for k := range c.nodes {
		if k != id {
			out = append(out, k)
		}
	}
	sort.Strings(out) // callers index into this more than once
	return out
}

func propose(n *node, cmd string, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, err := n.raft.Propose(ctx, []byte(cmd))
	return err
}

// write proposes through whichever node leads until one attempt succeeds.
func (c *cluster) write(cmd string) {
	c.t.Helper()
	eventually(c.t, 10*time.Second, "write "+cmd, func() bool {
		for _, n := range c.live() {
			if n.raft.IsLeader() && propose(n, cmd, 2*time.Second) == nil {
				return true
			}
		}
		return false
	})
}

// converge waits until every live node has applied exactly the same commands.
func (c *cluster) converge() []string {
	c.t.Helper()
	var want []string
	eventually(c.t, 15*time.Second, "replicas to converge", func() bool {
		nodes := c.live()
		want = nodes[0].sm.commands()
		commit := nodes[0].raft.Status().CommitIndex
		for _, n := range nodes {
			st := n.raft.Status()
			got := n.sm.commands()
			if st.CommitIndex != commit || st.LastApplied != commit || len(got) != len(want) {
				return false
			}
			for i := range got {
				if got[i] != want[i] {
					c.t.Fatalf("%s diverged at %d: %q vs %q", n.id, i, got[i], want[i])
				}
			}
		}
		return true
	})
	return want
}
