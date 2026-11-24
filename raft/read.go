package raft

import (
	"context"
)

// LinearizableRead returns once local state reflects every write acknowledged
// before the call began. It works on any node: followers ask the leader for a
// safe index and then serve the read themselves.
func (r *Raft) LinearizableRead(ctx context.Context) error {
	idx, err := r.readIndex(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stop := r.wakeOnDone(ctx)
	defer stop()
	for r.lastApplied < idx {
		if r.stopped {
			return ErrStopped
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.readCond.Wait()
	}
	return nil
}

// wakeOnDone makes waits on readCond notice that ctx ended. Taking the lock
// before broadcasting closes the gap between a waiter's check and its Wait.
func (r *Raft) wakeOnDone(ctx context.Context) func() bool {
	return context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.readCond.Broadcast()
		r.mu.Unlock()
	})
}

func (r *Raft) readIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	isLeader, leader := r.role == Leader, r.leaderID
	m, known := r.conf().Find(leader)
	stopped := r.stopped
	r.mu.Unlock()
	switch {
	case stopped:
		return 0, ErrStopped
	case isLeader:
		return r.leaderReadIndex(ctx)
	case !known:
		return 0, ErrNotLeader
	}
	reply, err := r.transport.ReadIndex(ctx, m, &ReadIndexArgs{From: r.id})
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, ErrNotLeader
	}
	return reply.Index, nil
}

// HandleReadIndex serves a follower's ReadIndex request.
func (r *Raft) HandleReadIndex(*ReadIndexArgs) (*ReadIndexReply, error) {
	ctx, cancel := context.WithTimeout(r.stopCtx, 4*r.cfg.RPCTimeout)
	defer cancel()
	idx, err := r.leaderReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	return &ReadIndexReply{Index: idx}, nil
}

// leaderReadIndex is the ReadIndex protocol (thesis §6.4): note the commit
// index, then prove this node was still leader after noting it by having a
// majority acknowledge a heartbeat sent later. Concurrent reads share rounds.
func (r *Raft) leaderReadIndex(ctx context.Context) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	stop := r.wakeOnDone(ctx)
	defer stop()
	if r.role != Leader {
		return 0, ErrNotLeader
	}
	term := r.term
	check := func() error {
		switch {
		case r.stopped:
			return ErrStopped
		case r.role != Leader || r.term != term:
			return ErrNotLeader
		}
		return ctx.Err()
	}
	// Until something from this term commits, commitIndex may lag entries an
	// earlier leader already acknowledged.
	for r.commitIndex < r.termStart {
		if err := check(); err != nil {
			return 0, err
		}
		r.readCond.Wait()
	}
	if err := check(); err != nil {
		return 0, err
	}
	idx := r.commitIndex
	r.readSeq++
	seq := r.readSeq
	for _, p := range r.peers {
		p.kickHeartbeat()
	}
	for !r.quorumAcked(seq) {
		if err := check(); err != nil {
			return 0, err
		}
		r.readCond.Wait()
	}
	return idx, check()
}

func (r *Raft) quorumAcked(seq uint64) bool {
	if r.role != Leader {
		return false
	}
	conf, n := r.conf(), 0
	for _, m := range conf.Members {
		if m.Learner {
			continue
		}
		if p := r.peers[m.ID]; m.ID == r.id || (p != nil && p.ackedSeq >= seq) {
			n++
		}
	}
	return n >= conf.quorum()
}
