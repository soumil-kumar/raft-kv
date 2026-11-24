// Package raft is a from-scratch implementation of the Raft consensus algorithm.
//
// Concurrency model: one mutex guards all protocol state, and no disk or network
// I/O happens while it is held. Every durable mutation is appended to a single
// ordered queue under the lock and written by one goroutine (walLoop), which
// fsyncs whole batches. Anything that must not be visible before it is durable
// (RPC replies, vote requests, the leader's own match index) waits on that
// queue. Committed entries reach the state machine through one other goroutine
// (applyLoop), so commands and snapshots can never be reordered.
package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Role is a node's current position in the protocol.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

// StateMachine is the replicated application. All methods are called from a
// single goroutine in log order.
type StateMachine interface {
	// Apply executes one committed command. It must be deterministic. The result
	// is returned to the proposer if it is still waiting on this node.
	Apply(e LogEntry) any
	// Snapshot captures the current state quickly and returns a function that
	// serializes it; that function runs on another goroutine.
	Snapshot() (encode func() ([]byte, error))
	// Restore replaces the whole state.
	Restore(data []byte) error
}

// Config tunes a node. Zero values select the defaults noted on each field.
type Config struct {
	ID string
	// Bootstrap is the initial membership. It is used only when storage is
	// empty; leave it empty for a node that will join an existing cluster.
	Bootstrap Configuration

	ElectionTimeout   time.Duration // minimum; randomized in [T, 2T). Default 500ms.
	HeartbeatInterval time.Duration // Default 50ms.
	RPCTimeout        time.Duration // Default ElectionTimeout.

	MaxBatchEntries     int    // per AppendEntries. Default 4096.
	MaxBatchBytes       int    // per AppendEntries. Default 1 MiB.
	MaxUncommittedBytes int    // proposals beyond this are refused. Default 64 MiB.
	SnapshotThreshold   uint64 // entries between snapshots; 0 disables. Default set by caller.
	TrailingLogs        uint64 // entries kept in memory behind a snapshot. Default 4096.
	SnapshotChunk       int    // Default 1 MiB.

	Logger *slog.Logger
	// OnFatal is called once if the node can no longer guarantee durability.
	OnFatal func(error)
}

func (c *Config) setDefaults() {
	def := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 500 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeout
	}
	if c.TrailingLogs == 0 {
		c.TrailingLogs = 4096
	}
	def(&c.MaxBatchEntries, 4096)
	def(&c.MaxBatchBytes, 1<<20)
	def(&c.MaxUncommittedBytes, 64<<20)
	def(&c.SnapshotChunk, 1<<20)
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

type configAt struct {
	index uint64
	conf  Configuration
}

// walOp is one entry in the ordered persistence queue.
type walOp struct {
	save      SaveOp
	compact   *SnapshotMeta
	seq       uint64
	lastIndex uint64 // last log index once this op is durable
}

type proposal struct {
	term uint64
	ch   chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

// Raft is one node.
type Raft struct {
	cfg       Config
	id        string
	storage   Storage
	transport Transport
	sm        StateMachine
	logger    *slog.Logger

	mu sync.Mutex

	// Persistent state; every change is queued for walLoop under mu.
	term     uint64
	votedFor string
	log      raftLog
	snap     SnapshotMeta
	configs  []configAt // ascending, never empty; the last one is in force

	// Persistence queue.
	walQueue     []walOp
	walSeq       uint64
	durableSeq   uint64
	durableIndex uint64
	walCond      *sync.Cond // queue became non-empty
	durableCond  *sync.Cond // durableSeq advanced

	// Volatile state.
	role              Role
	leaderID          string
	commitIndex       uint64
	lastApplied       uint64
	electionDeadline  time.Time
	lastLeaderContact time.Time
	campaignToken     uint64
	votes             map[string]bool

	applyCond      *sync.Cond // something to apply
	readCond       *sync.Cond // commit, apply, ack or role changed
	pendingRestore []byte
	restorePending bool
	snapshotting   bool
	bytesSinceSnap uint64
	lastSnapSize   uint64
	incoming       *incomingSnapshot

	// Leader state.
	leaderCtx     context.Context
	leaderCancel  context.CancelFunc
	peers         map[string]*peer
	termStart     uint64 // index of this term's no-op
	readSeq       uint64
	proposals     map[uint64]*proposal
	uncommitted   int // bytes
	transferring  bool
	electionsSeen atomic.Uint64
	snapsTaken    atomic.Uint64
	snapsReceived atomic.Uint64

	stopCtx  context.Context
	stopFn   context.CancelFunc
	stopped  bool
	failOnce sync.Once
	wg       sync.WaitGroup
}

// New recovers a node from storage and starts it.
func New(cfg Config, storage Storage, transport Transport, sm StateMachine) (*Raft, error) {
	cfg.setDefaults()
	if cfg.ID == "" {
		return nil, errors.New("raft: config needs an ID")
	}
	st, err := storage.Load()
	if err != nil {
		return nil, err
	}
	if st.Empty && len(cfg.Bootstrap.Members) > 0 {
		if err := cfg.Bootstrap.Validate(); err != nil {
			return nil, fmt.Errorf("raft: bootstrap configuration: %w", err)
		}
		// Stored with the log so later restarts do not depend on flags.
		st.Meta = SnapshotMeta{Config: cfg.Bootstrap.clone()}
		if err := storage.Compact(st.Meta); err != nil {
			return nil, err
		}
	}

	r := &Raft{
		cfg: cfg, id: cfg.ID, storage: storage, transport: transport, sm: sm,
		logger:    cfg.Logger.With("node", cfg.ID),
		term:      st.HardState.Term,
		votedFor:  st.HardState.VotedFor,
		snap:      st.Meta,
		configs:   []configAt{{st.Meta.Index, st.Meta.Config}},
		proposals: map[uint64]*proposal{},
	}
	r.log.reset(st.Meta.Index, st.Meta.Term)
	r.log.append(st.Entries...)
	for _, e := range st.Entries {
		if err := r.noteConfig(e); err != nil {
			return nil, err
		}
	}
	r.commitIndex, r.lastApplied = st.Meta.Index, st.Meta.Index
	r.durableIndex = r.log.lastIndex()
	r.lastSnapSize = uint64(len(st.Snapshot))
	if st.Meta.Index > 0 {
		if err := sm.Restore(st.Snapshot); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
	}
	r.walCond, r.durableCond = sync.NewCond(&r.mu), sync.NewCond(&r.mu)
	r.applyCond, r.readCond = sync.NewCond(&r.mu), sync.NewCond(&r.mu)
	r.stopCtx, r.stopFn = context.WithCancel(context.Background())
	r.resetElectionDeadline()

	r.wg.Add(3)
	go r.walLoop()
	go r.applyLoop()
	go r.tickLoop()
	return r, nil
}

// Stop halts the node as abruptly as a crash would: queued but unacknowledged
// writes are dropped.
func (r *Raft) Stop() {
	r.shutdown()
	r.wg.Wait()
	r.storage.Close()
}

func (r *Raft) shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	if r.role == Leader {
		r.leaveLeadership(ErrLeadershipLost) // the outcome of pending proposals is unknown
	}
	r.role = Follower
	r.stopFn()
	for _, c := range []*sync.Cond{r.walCond, r.durableCond, r.applyCond, r.readCond} {
		c.Broadcast()
	}
}

// fail stops a node that can no longer keep its promises (disk error, corrupt
// state, violated invariant). Carrying on would risk acknowledged data.
func (r *Raft) fail(err error) {
	r.failOnce.Do(func() {
		r.logger.Error("fatal: stopping node", "err", err)
		go func() {
			r.shutdown()
			if r.cfg.OnFatal != nil {
				r.cfg.OnFatal(err)
			}
		}()
	})
}

// --- persistence queue (callers hold mu) ---

func (r *Raft) enqueue(op walOp) {
	r.walSeq++
	op.seq, op.lastIndex = r.walSeq, r.log.lastIndex()
	r.walQueue = append(r.walQueue, op)
	r.walCond.Signal()
}

func (r *Raft) persistHardState() {
	r.enqueue(walOp{save: SaveOp{HardState: &HardState{Term: r.term, VotedFor: r.votedFor}}})
}

func (r *Raft) persistEntries(entries []LogEntry) {
	r.enqueue(walOp{save: SaveOp{Entries: entries}})
}

func (r *Raft) walLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for len(r.walQueue) == 0 && !r.stopped {
			r.walCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}
		batch := r.walQueue
		r.walQueue = nil
		r.mu.Unlock()

		if err := r.writeBatch(batch); err != nil {
			r.fail(err)
			return
		}

		last := batch[len(batch)-1]
		r.mu.Lock()
		r.durableSeq, r.durableIndex = last.seq, last.lastIndex
		if r.role == Leader {
			r.advanceCommit()
		}
		r.durableCond.Broadcast()
		r.mu.Unlock()
	}
}

func (r *Raft) writeBatch(batch []walOp) error {
	var saves []SaveOp
	for _, op := range batch {
		if op.compact == nil {
			saves = append(saves, op.save)
			continue
		}
		if err := r.storage.Save(saves); err != nil {
			return err
		}
		saves = nil
		if err := r.storage.Compact(*op.compact); err != nil {
			return err
		}
	}
	if len(saves) == 0 {
		return nil
	}
	return r.storage.Save(saves)
}

// waitDurable blocks (releasing mu) until everything queued up to seq is on disk.
func (r *Raft) waitDurable(seq uint64) error {
	for r.durableSeq < seq && !r.stopped {
		r.durableCond.Wait()
	}
	if r.durableSeq < seq {
		return ErrStopped
	}
	return nil
}

// --- configuration (callers hold mu) ---

func (r *Raft) conf() Configuration { return r.configs[len(r.configs)-1].conf }

func (r *Raft) confIndex() uint64 { return r.configs[len(r.configs)-1].index }

// noteConfig puts a configuration entry in force as soon as it is in the log.
func (r *Raft) noteConfig(e LogEntry) error {
	if e.Type != EntryConfig {
		return nil
	}
	c, err := decodeConfiguration(e.Data)
	if err != nil {
		return fmt.Errorf("%w: configuration entry %d: %v", ErrCorrupt, e.Index, err)
	}
	r.configs = append(r.configs, configAt{e.Index, c})
	return nil
}

// dropConfigsFrom rolls back configurations whose entries were truncated.
func (r *Raft) dropConfigsFrom(i uint64) {
	for len(r.configs) > 1 && r.confIndex() >= i {
		r.configs = r.configs[:len(r.configs)-1]
	}
}

func (r *Raft) configAtIndex(i uint64) Configuration {
	for j := len(r.configs) - 1; j > 0; j-- {
		if r.configs[j].index <= i {
			return r.configs[j].conf
		}
	}
	return r.configs[0].conf
}

// pruneConfigs forgets configurations superseded at or before i.
func (r *Raft) pruneConfigs(i uint64) {
	for len(r.configs) > 1 && r.configs[1].index <= i {
		r.configs = r.configs[1:]
	}
}

// --- role changes (callers hold mu) ---

func (r *Raft) resetElectionDeadline() {
	t := r.cfg.ElectionTimeout
	r.electionDeadline = time.Now().Add(t + rand.N(t))
}

func (r *Raft) becomeFollower(term uint64, leader string) {
	if term > r.term {
		r.term, r.votedFor = term, ""
		r.persistHardState()
	}
	if r.role == Leader {
		r.logger.Info("stepping down", "term", r.term)
		r.leaveLeadership(ErrLeadershipLost)
		r.resetElectionDeadline()
	}
	r.role = Follower
	r.leaderID = leader
	r.campaignToken++ // orphan any election in flight
	r.readCond.Broadcast()
}

func (r *Raft) leaveLeadership(err error) {
	r.leaderCancel()
	r.peers = nil
	r.transferring = false
	for i, p := range r.proposals {
		p.ch <- proposalResult{err: err}
		delete(r.proposals, i)
	}
	if r.leaderID == r.id {
		r.leaderID = ""
	}
	r.readCond.Broadcast()
}

func (r *Raft) becomeLeader() {
	r.logger.Info("became leader", "term", r.term, "last_index", r.log.lastIndex())
	r.role, r.leaderID = Leader, r.id
	r.leaderCtx, r.leaderCancel = context.WithCancel(r.stopCtx)
	r.peers = map[string]*peer{}
	r.uncommitted = 0
	r.transferring = false
	// Entries from earlier terms only become committable once an entry of this
	// term is (§5.4.2), so commit one immediately.
	r.termStart = r.appendLocal(EntryNoop, nil)
	r.reconcilePeers()
}

// appendLocal adds an entry to the leader's log and starts replicating it.
func (r *Raft) appendLocal(t EntryType, data []byte) uint64 {
	e := LogEntry{Index: r.log.lastIndex() + 1, Term: r.term, Type: t, Data: data}
	r.log.append(e)
	r.uncommitted += len(data)
	r.persistEntries([]LogEntry{e})
	for _, p := range r.peers {
		p.kickReplicate()
	}
	return e.Index
}

func (r *Raft) tickLoop() {
	defer r.wg.Done()
	t := time.NewTicker(max(r.cfg.HeartbeatInterval/4, time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-r.stopCtx.Done():
			return
		case <-t.C:
		}
		r.mu.Lock()
		if !r.stopped {
			r.tick(time.Now())
		}
		r.mu.Unlock()
	}
}

func (r *Raft) tick(now time.Time) {
	if r.role == Leader {
		// Check-quorum: a leader cut off from its majority must stop accepting
		// work it can never commit.
		active := 0
		for _, m := range r.conf().Members {
			if m.Learner {
				continue
			}
			if p := r.peers[m.ID]; m.ID == r.id || (p != nil && now.Sub(p.lastAck) < r.cfg.ElectionTimeout) {
				active++
			}
		}
		if active < r.conf().quorum() {
			r.logger.Warn("lost contact with quorum", "term", r.term)
			r.becomeFollower(r.term, "")
		}
		return
	}
	if now.After(r.electionDeadline) && r.conf().isVoter(r.id) {
		r.campaign(true, false)
	}
}

// --- commit and apply ---

// advanceCommit commits the highest index a majority of voters holds durably,
// if it is from the current term (§5.4.2). The leader counts itself only up to
// what its own disk holds. Caller holds mu and is leader.
func (r *Raft) advanceCommit() {
	conf := r.conf()
	var match []uint64
	for _, m := range conf.Members {
		if m.Learner {
			continue
		}
		if m.ID == r.id {
			match = append(match, r.durableIndex)
		} else if p := r.peers[m.ID]; p != nil {
			match = append(match, p.match)
		} else {
			match = append(match, 0)
		}
	}
	sort.Slice(match, func(i, j int) bool { return match[i] > match[j] })
	n := match[conf.quorum()-1]
	if n <= r.commitIndex {
		return
	}
	if t, ok := r.log.term(n); !ok || t != r.term {
		return
	}
	for _, e := range r.log.slice(r.commitIndex+1, n, math.MaxInt, math.MaxInt) {
		r.uncommitted = max(0, r.uncommitted-len(e.Data))
	}
	r.setCommit(n)
	for _, p := range r.peers {
		if p.next > r.log.lastIndex() { // idle: tell it about the commit now
			p.kickHeartbeat()
		}
	}
}

func (r *Raft) setCommit(n uint64) {
	if n > r.commitIndex {
		r.commitIndex = n
		r.applyCond.Signal()
		r.readCond.Broadcast()
	}
}

func (r *Raft) applyLoop() {
	defer r.wg.Done()
	for {
		r.mu.Lock()
		for !r.stopped && !r.restorePending && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.stopped {
			r.mu.Unlock()
			return
		}
		if r.restorePending {
			data, meta := r.pendingRestore, r.snap
			r.restorePending, r.pendingRestore = false, nil
			r.mu.Unlock()
			if err := r.sm.Restore(data); err != nil {
				r.fail(fmt.Errorf("raft: restore snapshot %d: %w", meta.Index, err))
				return
			}
			r.mu.Lock()
			r.lastApplied = max(r.lastApplied, meta.Index)
			r.readCond.Broadcast()
			r.mu.Unlock()
			continue
		}
		entries := r.log.slice(r.lastApplied+1, r.commitIndex, 512, 4<<20)
		r.mu.Unlock()
		if len(entries) == 0 {
			r.fail(fmt.Errorf("raft: committed entry %d is missing", r.lastApplied+1))
			return
		}

		results := make([]any, len(entries))
		size := uint64(0)
		for i, e := range entries {
			size += uint64(len(e.Data)) + 32
			if e.Type == EntryCommand {
				results[i] = r.sm.Apply(e)
			}
		}

		r.mu.Lock()
		last := entries[len(entries)-1]
		r.lastApplied = last.Index
		r.bytesSinceSnap += size
		for i, e := range entries {
			p := r.proposals[e.Index]
			if p == nil {
				continue
			}
			delete(r.proposals, e.Index)
			if p.term == e.Term {
				p.ch <- proposalResult{value: results[i]}
			} else {
				p.ch <- proposalResult{err: ErrLeadershipLost}
			}
		}
		if r.role == Leader && r.confIndex() <= r.lastApplied && !r.conf().isVoter(r.id) {
			r.logger.Info("removed from the cluster; stepping down")
			r.becomeFollower(r.term, "")
		}
		r.readCond.Broadcast()
		if !r.stopped && !r.restorePending {
			r.maybeSnapshot(last)
		}
		r.mu.Unlock()
	}
}

// --- client operations ---

// Propose replicates a command and returns the state machine's result.
func (r *Raft) Propose(ctx context.Context, data []byte) (any, error) {
	return r.propose(ctx, EntryCommand, func() ([]byte, error) { return data, nil })
}

func (r *Raft) propose(ctx context.Context, t EntryType, build func() ([]byte, error)) (any, error) {
	r.mu.Lock()
	switch {
	case r.stopped:
		r.mu.Unlock()
		return nil, ErrStopped
	case r.role != Leader:
		r.mu.Unlock()
		return nil, ErrNotLeader
	case r.transferring:
		r.mu.Unlock()
		return nil, ErrTransferring
	case r.uncommitted > r.cfg.MaxUncommittedBytes:
		r.mu.Unlock()
		return nil, ErrOverloaded
	}
	data, err := build()
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	p := &proposal{term: r.term, ch: make(chan proposalResult, 1)}
	idx := r.appendLocal(t, data)
	r.proposals[idx] = p
	if t == EntryConfig {
		if err := r.noteConfig(LogEntry{Index: idx, Term: r.term, Type: t, Data: data}); err != nil {
			r.mu.Unlock()
			return nil, err
		}
		r.reconcilePeers()
	}
	r.mu.Unlock()

	select {
	case res := <-p.ch:
		return res.value, res.err
	case <-ctx.Done():
		r.mu.Lock()
		if r.proposals[idx] == p {
			delete(r.proposals, idx)
		}
		r.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Status is a point-in-time view of the node.
type Status struct {
	ID            string            `json:"id"`
	Role          string            `json:"role"`
	Term          uint64            `json:"term"`
	LeaderID      string            `json:"leader"`
	CommitIndex   uint64            `json:"commit_index"`
	LastApplied   uint64            `json:"last_applied"`
	LastIndex     uint64            `json:"last_index"`
	DurableIndex  uint64            `json:"durable_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	Members       []Member          `json:"members"`
	Match         map[string]uint64 `json:"match,omitempty"`
	Elections     uint64            `json:"elections"`
	Snapshots     uint64            `json:"snapshots_taken"`
	Installed     uint64            `json:"snapshots_installed"`
}

// Status reports the node's current state.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Status{
		ID: r.id, Role: r.role.String(), Term: r.term, LeaderID: r.leaderID,
		CommitIndex: r.commitIndex, LastApplied: r.lastApplied,
		LastIndex: r.log.lastIndex(), DurableIndex: r.durableIndex,
		SnapshotIndex: r.snap.Index, Members: r.conf().clone().Members,
		Elections: r.electionsSeen.Load(), Snapshots: r.snapsTaken.Load(), Installed: r.snapsReceived.Load(),
	}
	if r.role == Leader {
		s.Match = map[string]uint64{r.id: r.durableIndex}
		for id, p := range r.peers {
			s.Match[id] = p.match
		}
	}
	return s
}

// Leader returns the member this node believes is leader.
func (r *Raft) Leader() (Member, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leaderID == "" {
		return Member{}, false
	}
	return r.conf().Find(r.leaderID)
}

// IsLeader reports whether this node currently believes it is leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}
