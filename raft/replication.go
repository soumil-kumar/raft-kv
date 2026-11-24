package raft

import (
	"context"
	"fmt"
	"time"
)

// peer is the leader's view of one other member. Two goroutines serve it: one
// replicates the log, the other sends heartbeats, so bulk data and slow disks
// never hold up leadership or reads.
type peer struct {
	member Member
	cancel context.CancelFunc

	// Guarded by Raft.mu.
	next       uint64
	match      uint64
	ackedSeq   uint64    // newest readSeq this peer has confirmed leadership for
	lastAck    time.Time // send time of the newest acknowledged RPC
	sentCommit uint64

	replicate chan struct{}
	heartbeat chan struct{}
}

func kick(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (p *peer) kickReplicate() { kick(p.replicate) }
func (p *peer) kickHeartbeat() { kick(p.heartbeat) }

// reconcilePeers starts and stops replication to match the configuration in
// force. Caller holds mu and is leader.
func (r *Raft) reconcilePeers() {
	conf := r.conf()
	for id, p := range r.peers {
		if m, ok := conf.Find(id); !ok {
			p.cancel()
			delete(r.peers, id)
		} else {
			p.member = m
		}
	}
	for _, m := range conf.Members {
		if m.ID == r.id || r.peers[m.ID] != nil {
			continue
		}
		ctx, cancel := context.WithCancel(r.leaderCtx)
		p := &peer{
			member: m, cancel: cancel,
			next:      r.log.lastIndex() + 1,
			lastAck:   time.Now(), // grace period before check-quorum applies
			replicate: make(chan struct{}, 1),
			heartbeat: make(chan struct{}, 1),
		}
		r.peers[m.ID] = p
		term := r.term
		r.wg.Add(2)
		go r.replicateLoop(ctx, p, term)
		go r.heartbeatLoop(ctx, p, term)
	}
}

// leads reports whether p still belongs to this node's leadership of term.
func (r *Raft) leads(p *peer, term uint64) bool {
	return !r.stopped && r.role == Leader && r.term == term && r.peers[p.member.ID] == p
}

// noteAck processes the part common to every reply from a peer. It returns
// false if the reply ended, or arrived after, this leadership. Caller holds mu.
func (r *Raft) noteAck(p *peer, term, replyTerm, seq uint64, sent time.Time) bool {
	if !r.leads(p, term) {
		return false
	}
	if replyTerm > r.term {
		r.becomeFollower(replyTerm, "")
		return false
	}
	if sent.After(p.lastAck) {
		p.lastAck = sent
	}
	if seq > p.ackedSeq {
		p.ackedSeq = seq
		r.readCond.Broadcast()
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (r *Raft) heartbeatLoop(ctx context.Context, p *peer, term uint64) {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		again, err := r.sendHeartbeat(ctx, p, term)
		if err != nil {
			sleepCtx(ctx, r.cfg.HeartbeatInterval)
		}
		if ctx.Err() != nil {
			return
		}
		if again {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-p.heartbeat:
		}
	}
}

func (r *Raft) sendHeartbeat(ctx context.Context, p *peer, term uint64) (again bool, err error) {
	r.mu.Lock()
	if !r.leads(p, term) {
		r.mu.Unlock()
		return false, nil
	}
	seq := r.readSeq
	// The peer is known to match us up to p.match, and only that far.
	args := &HeartbeatArgs{Term: term, LeaderID: r.id, Commit: min(r.commitIndex, p.match)}
	p.sentCommit = max(p.sentCommit, args.Commit)
	m := p.member
	r.mu.Unlock()

	sent := time.Now()
	c, cancel := context.WithTimeout(ctx, r.cfg.RPCTimeout)
	defer cancel()
	reply, err := r.transport.Heartbeat(c, m, args)
	if err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.noteAck(p, term, reply.Term, seq, sent) {
		return false, nil
	}
	return r.readSeq > seq, nil // reads arrived while this round was in flight
}

func (r *Raft) replicateLoop(ctx context.Context, p *peer, term uint64) {
	defer r.wg.Done()
	// Probe once right away: it establishes match even with nothing to send.
	for {
		more, err := r.replicateOnce(ctx, p, term)
		if err != nil {
			sleepCtx(ctx, r.cfg.HeartbeatInterval)
			more = true
		}
		if ctx.Err() != nil {
			return
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-p.replicate:
		}
	}
}

func (r *Raft) replicateOnce(ctx context.Context, p *peer, term uint64) (more bool, err error) {
	r.mu.Lock()
	if !r.leads(p, term) {
		r.mu.Unlock()
		return false, nil
	}
	prev := p.next - 1
	prevTerm, ok := r.log.term(prev)
	if !ok {
		meta := r.snap
		r.mu.Unlock()
		return true, r.sendSnapshot(ctx, p, term, meta)
	}
	seq := r.readSeq
	args := &AppendEntriesArgs{
		Term: term, LeaderID: r.id,
		PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries:      r.log.slice(p.next, r.log.lastIndex(), r.cfg.MaxBatchEntries, r.cfg.MaxBatchBytes),
		LeaderCommit: r.commitIndex,
	}
	m := p.member
	r.mu.Unlock()

	sent := time.Now()
	// Entries wait for the follower's fsync, so allow more than a heartbeat.
	c, cancel := context.WithTimeout(ctx, 4*r.cfg.RPCTimeout)
	defer cancel()
	reply, err := r.transport.AppendEntries(c, m, args)
	if err != nil {
		return false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.noteAck(p, term, reply.Term, seq, sent) {
		return false, nil
	}
	if reply.Success {
		match := prev + uint64(len(args.Entries))
		p.next = max(p.next, match+1)
		p.sentCommit = max(p.sentCommit, min(args.LeaderCommit, match))
		if match > p.match {
			p.match = match
			r.advanceCommit()
			r.readCond.Broadcast() // a leadership transfer may be waiting
		}
		if r.role != Leader {
			return false, nil
		}
		if p.next <= r.log.lastIndex() {
			return true, nil
		}
		if min(r.commitIndex, p.match) > p.sentCommit {
			p.kickHeartbeat()
		}
		return false, nil
	}

	// Skip a whole conflicting term per round trip (§5.3).
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if i, ok := r.log.lastIndexOfTerm(reply.ConflictTerm); ok {
			next = i + 1
		}
	}
	p.next = max(1, min(next, prev)) // strictly decreasing, so this terminates
	return true, nil
}

// HandleHeartbeat is the Heartbeat RPC.
func (r *Raft) HandleHeartbeat(args *HeartbeatArgs) (*HeartbeatReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if args.Term < r.term {
		return &HeartbeatReply{Term: r.term}, nil
	}
	newTerm := args.Term > r.term
	r.followLeader(args.Term, args.LeaderID)
	r.setCommit(min(args.Commit, r.log.lastIndex()))
	reply := &HeartbeatReply{Term: r.term}
	if newTerm {
		if err := r.waitDurable(r.walSeq); err != nil {
			return nil, err
		}
	}
	return reply, nil
}

// followLeader accepts a valid message from the leader of term. It keeps
// votedFor when the term is unchanged: forgetting it would allow a second vote
// in the same term. Caller holds mu and has checked term >= r.term.
func (r *Raft) followLeader(term uint64, leader string) {
	if term > r.term || r.role != Follower {
		r.becomeFollower(term, leader)
	}
	r.leaderID = leader
	r.lastLeaderContact = time.Now()
	r.resetElectionDeadline()
}

// HandleAppendEntries is the AppendEntries RPC. It replies only after the
// entries it acknowledges are durable.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if args.Term < r.term {
		return &AppendEntriesReply{Term: r.term}, nil
	}
	if args.Term == r.term && r.role == Leader {
		err := fmt.Errorf("raft: two leaders in term %d: %s and %s", r.term, r.id, args.LeaderID)
		r.fail(err)
		return nil, err
	}
	r.followLeader(args.Term, args.LeaderID)

	reply := &AppendEntriesReply{Term: r.term}
	prev, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	matched := prev + uint64(len(entries))
	if prev < r.log.base {
		// Everything up to base is committed here, so it already agrees.
		skip := min(r.log.base-prev, uint64(len(entries)))
		entries = entries[skip:]
		prev += skip
		if prev == r.log.base {
			prevTerm = r.log.baseTerm
		}
	}
	if prev >= r.log.base {
		t, ok := r.log.term(prev)
		if !ok {
			reply.ConflictIndex = r.log.lastIndex() + 1
		} else if t != prevTerm {
			reply.ConflictTerm = t
			reply.ConflictIndex = r.log.firstIndexOfTermAt(prev)
		}
		if !ok || t != prevTerm {
			return reply, r.waitDurable(r.walSeq)
		}
	}

	for i, e := range entries {
		if t, ok := r.log.term(e.Index); ok {
			if t == e.Term {
				continue
			}
			if e.Index <= r.commitIndex {
				err := fmt.Errorf("raft: leader %s would overwrite committed entry %d", args.LeaderID, e.Index)
				r.fail(err)
				return nil, err
			}
			r.log.truncateFrom(e.Index)
			r.dropConfigsFrom(e.Index)
		}
		rest := entries[i:]
		r.log.append(rest...)
		for _, ne := range rest {
			if err := r.noteConfig(ne); err != nil {
				r.fail(err)
				return nil, err
			}
		}
		r.persistEntries(rest)
		break
	}
	r.setCommit(min(args.LeaderCommit, matched))
	reply.Success = true
	if err := r.waitDurable(r.walSeq); err != nil {
		return nil, err
	}
	return reply, nil
}
