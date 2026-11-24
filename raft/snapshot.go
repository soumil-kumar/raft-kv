package raft

import (
	"context"
	"errors"
	"time"
)

// maybeSnapshot starts a background snapshot once enough log has accumulated.
// Waiting for the log to outgrow the previous snapshot bounds the write
// amplification at about 2x however large the state becomes. Caller holds mu
// and runs on the apply goroutine, with last being the newest applied entry.
func (r *Raft) maybeSnapshot(last LogEntry) {
	if r.cfg.SnapshotThreshold == 0 || r.snapshotting ||
		last.Index < r.snap.Index+r.cfg.SnapshotThreshold ||
		r.bytesSinceSnap < r.lastSnapSize {
		return
	}
	meta := SnapshotMeta{Index: last.Index, Term: last.Term, Config: r.configAtIndex(last.Index).clone()}
	r.snapshotting = true
	r.bytesSinceSnap = 0
	encode := r.sm.Snapshot()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		data, err := encode()
		if err == nil {
			err = r.storage.WriteSnapshot(meta, data)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.snapshotting = false
		if r.stopped {
			return
		}
		if err != nil {
			// The log is still complete, so nothing is lost; try again later.
			r.logger.Error("snapshot failed", "index", meta.Index, "err", err)
			return
		}
		if meta.Index <= r.snap.Index {
			return // a newer one was installed meanwhile
		}
		r.snap = meta
		r.lastSnapSize = uint64(len(data))
		r.snapsTaken.Add(1)
		r.pruneConfigs(meta.Index)
		// Keep some entries so slightly lagging followers need no snapshot.
		if meta.Index > r.log.base+r.cfg.TrailingLogs {
			r.log.compactTo(meta.Index - r.cfg.TrailingLogs)
		}
		r.enqueue(walOp{compact: &meta})
	}()
}

// sendSnapshot streams the given snapshot to a peer that is too far behind.
func (r *Raft) sendSnapshot(ctx context.Context, p *peer, term uint64, meta SnapshotMeta) error {
	data, err := r.storage.ReadSnapshot(meta)
	if err != nil {
		return err // possibly superseded; the caller retries with the new one
	}
	r.mu.Lock()
	m := p.member
	r.mu.Unlock()
	r.logger.Info("sending snapshot", "to", m.ID, "index", meta.Index, "bytes", len(data))

	for off := uint64(0); ; {
		end := min(off+uint64(r.cfg.SnapshotChunk), uint64(len(data)))
		args := &InstallSnapshotArgs{
			Term: term, LeaderID: r.id, Meta: meta,
			Offset: off, Data: data[off:end], Done: end == uint64(len(data)),
		}
		// The final chunk includes the follower's fsync of the whole snapshot.
		timeout := 4 * r.cfg.RPCTimeout
		if args.Done {
			timeout = 30*time.Second + r.cfg.RPCTimeout
		}
		sent := time.Now()
		c, cancel := context.WithTimeout(ctx, timeout)
		reply, err := r.transport.InstallSnapshot(c, m, args)
		cancel()
		if err != nil {
			return err
		}
		r.mu.Lock()
		if !r.noteAck(p, term, reply.Term, 0, sent) {
			r.mu.Unlock()
			return nil
		}
		if reply.Installed {
			p.match = max(p.match, meta.Index)
			p.next = max(p.next, p.match+1)
			r.advanceCommit()
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
		if reply.Offset > uint64(len(data)) || (args.Done && reply.Offset == uint64(len(data))) {
			return errors.New("raft: follower rejected snapshot")
		}
		if reply.Offset <= off && reply.Offset != 0 {
			return errors.New("raft: snapshot transfer is not making progress")
		}
		if reply.Offset == 0 && off == 0 && len(args.Data) > 0 {
			return errors.New("raft: follower refused the first snapshot chunk")
		}
		off = reply.Offset // resumes wherever the follower actually is
	}
}

type incomingSnapshot struct {
	leader string
	term   uint64
	meta   SnapshotMeta
	buf    []byte
}

// HandleInstallSnapshot is the InstallSnapshot RPC. Nothing but the reassembly
// buffer changes until the last chunk has arrived and is safely on disk.
func (r *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if args.Term < r.term {
		return &InstallSnapshotReply{Term: r.term}, nil
	}
	r.followLeader(args.Term, args.LeaderID)
	reply := &InstallSnapshotReply{Term: r.term}
	if args.Meta.Index <= r.commitIndex {
		r.incoming = nil
		reply.Installed = true
		return reply, r.waitDurable(r.walSeq)
	}

	in := r.incoming
	if in == nil || in.leader != args.LeaderID || in.term != args.Term ||
		in.meta.Index != args.Meta.Index || in.meta.Term != args.Meta.Term || args.Offset == 0 {
		if args.Offset != 0 {
			return reply, r.waitDurable(r.walSeq) // Offset 0: start over
		}
		in = &incomingSnapshot{leader: args.LeaderID, term: args.Term, meta: args.Meta}
		r.incoming = in
	}
	inOrder := args.Offset == uint64(len(in.buf))
	if inOrder {
		in.buf = append(in.buf, args.Data...)
	}
	reply.Offset = uint64(len(in.buf))
	if !args.Done || !inOrder {
		return reply, r.waitDurable(r.walSeq)
	}
	r.incoming = nil // a retry starts from the beginning

	r.mu.Unlock()
	err := r.storage.WriteSnapshot(in.meta, in.buf)
	r.mu.Lock()
	if r.stopped {
		return nil, ErrStopped
	}
	if err != nil {
		r.fail(err)
		return nil, err
	}
	if r.term != args.Term {
		reply.Term = r.term
		reply.Offset = 0
		return reply, r.waitDurable(r.walSeq)
	}
	r.followLeader(args.Term, args.LeaderID)
	if in.meta.Index > r.commitIndex {
		if t, ok := r.log.term(in.meta.Index); ok && t == in.meta.Term {
			r.log.compactTo(in.meta.Index)
		} else {
			r.log.reset(in.meta.Index, in.meta.Term)
			r.configs = r.configs[:1]
		}
		var later []configAt
		for _, c := range r.configs {
			if c.index > in.meta.Index {
				later = append(later, c)
			}
		}
		r.configs = append([]configAt{{in.meta.Index, in.meta.Config}}, later...)
		r.snap = in.meta
		r.lastSnapSize = uint64(len(in.buf))
		r.bytesSinceSnap = 0
		r.snapsReceived.Add(1)
		r.pendingRestore, r.restorePending = in.buf, true
		meta := in.meta
		r.enqueue(walOp{compact: &meta})
		r.setCommit(in.meta.Index)
		r.applyCond.Signal()
		r.logger.Info("installed snapshot", "index", meta.Index, "bytes", len(in.buf))
	}
	reply.Installed = true
	if err := r.waitDurable(r.walSeq); err != nil {
		return nil, err
	}
	return reply, nil
}
