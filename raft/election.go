package raft

import (
	"context"
	"errors"
	"time"
)

// campaign starts a pre-vote or a real election. Caller holds mu.
func (r *Raft) campaign(pre, transfer bool) {
	r.resetElectionDeadline()
	r.campaignToken++
	token := r.campaignToken
	r.leaderID = ""
	r.votes = map[string]bool{r.id: true}

	args := RequestVoteArgs{
		Term:         r.term + 1,
		CandidateID:  r.id,
		LastLogIndex: r.log.lastIndex(),
		LastLogTerm:  r.log.lastTerm(),
		PreVote:      pre,
		Transfer:     transfer,
	}
	if pre {
		r.role = PreCandidate
	} else {
		r.role = Candidate
		r.term++
		r.votedFor = r.id
		r.persistHardState()
		r.electionsSeen.Add(1)
		r.logger.Info("starting election", "term", r.term)
	}
	seq := r.walSeq
	members := r.conf().clone().Members

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// Our own vote counts only once it is durable.
		r.mu.Lock()
		err := r.waitDurable(seq)
		if err == nil {
			r.tally(token)
		}
		r.mu.Unlock()
		if err != nil {
			return
		}
		for _, m := range members {
			if m.ID == r.id || m.Learner {
				continue
			}
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				r.solicitVote(token, m, args)
			}()
		}
	}()
}

func (r *Raft) solicitVote(token uint64, m Member, args RequestVoteArgs) {
	ctx, cancel := context.WithTimeout(r.stopCtx, r.cfg.RPCTimeout)
	defer cancel()
	reply, err := r.transport.RequestVote(ctx, m, &args)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if reply.Term > r.term && !reply.Granted {
		r.becomeFollower(reply.Term, "")
		return
	}
	if reply.Granted && token == r.campaignToken {
		r.votes[m.ID] = true
		r.tally(token)
	}
}

// tally moves a winning campaign to its next stage. Caller holds mu.
func (r *Raft) tally(token uint64) {
	if token != r.campaignToken || r.stopped {
		return
	}
	conf, n := r.conf(), 0
	for id := range r.votes {
		if conf.isVoter(id) {
			n++
		}
	}
	if n < conf.quorum() {
		return
	}
	switch r.role {
	case PreCandidate:
		r.campaign(false, false)
	case Candidate:
		r.campaignToken++
		r.becomeLeader()
	}
}

// hasLiveLeader reports whether this node has recent evidence of a working
// leader, in which case it refuses to help depose it (thesis §4.2.3).
func (r *Raft) hasLiveLeader() bool {
	return r.role == Leader ||
		(r.leaderID != "" && time.Since(r.lastLeaderContact) < r.cfg.ElectionTimeout)
}

// HandleRequestVote is the RequestVote/PreVote RPC.
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if !args.Transfer && r.hasLiveLeader() {
		return &RequestVoteReply{Term: r.term}, nil
	}
	lastTerm, lastIndex := r.log.lastTerm(), r.log.lastIndex()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if args.PreVote {
		return &RequestVoteReply{Term: r.term, Granted: args.Term > r.term && upToDate}, nil
	}
	if args.Term > r.term {
		r.becomeFollower(args.Term, "")
	}
	reply := &RequestVoteReply{Term: r.term}
	if args.Term == r.term && upToDate && (r.votedFor == "" || r.votedFor == args.CandidateID) {
		if r.votedFor == "" {
			r.votedFor = args.CandidateID
			r.persistHardState()
		}
		r.resetElectionDeadline()
		reply.Granted = true
	}
	if err := r.waitDurable(r.walSeq); err != nil {
		return nil, err
	}
	return reply, nil
}

// HandleTimeoutNow makes this node campaign immediately: the leader is handing
// over and has already made sure our log is complete.
func (r *Raft) HandleTimeoutNow(args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	if args.Term == r.term && r.role == Follower && r.conf().isVoter(r.id) {
		r.campaign(false, true)
	}
	return &TimeoutNowReply{Term: r.term}, nil
}

// TransferLeadership hands leadership to the most up-to-date voter, or to
// target if it is not empty, and waits until this node has heard from the new
// leader, so that it can forward whatever it is still serving.
func (r *Raft) TransferLeadership(ctx context.Context, target string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader {
		return ErrNotLeader
	}
	if r.transferring {
		return ErrTransferring
	}
	var best *peer
	for id, p := range r.peers {
		if !r.conf().isVoter(id) || (target != "" && id != target) {
			continue
		}
		if best == nil || p.match > best.match {
			best = p
		}
	}
	if best == nil {
		return errors.New("raft: no suitable transfer target")
	}
	term := r.term
	r.transferring = true // no new proposals, so the target can catch up
	defer func() {
		if r.term == term {
			r.transferring = false
		}
	}()
	stop := r.wakeOnDone(ctx)
	defer stop()

	sent := false
	for r.role == Leader && r.term == term {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !sent && best.match == r.log.lastIndex() && r.commitIndex >= r.termStart {
			sent = true
			m, args := best.member, &TimeoutNowArgs{Term: term, LeaderID: r.id}
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				c, cancel := context.WithTimeout(r.stopCtx, r.cfg.RPCTimeout)
				defer cancel()
				r.transport.TimeoutNow(c, m, args)
			}()
		}
		r.readCond.Wait()
	}
	// Stepping down only means the target is campaigning; wait for it to win.
	for r.leaderID == "" || r.leaderID == r.id {
		switch {
		case r.stopped:
			return ErrStopped
		case r.role == Leader:
			return errors.New("raft: transfer failed, this node was elected again")
		case ctx.Err() != nil:
			return ctx.Err()
		}
		r.readCond.Wait()
	}
	return nil
}
