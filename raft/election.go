package raft

import (
	"context"
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
