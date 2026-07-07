package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RPC handler for RequestVote
func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	reply.Term = r.currentTerm
	reply.VoteGranted = false

	if args.Term < r.currentTerm {
		return nil
	}

	if (r.votedFor == "" || r.votedFor == args.CandidateId) && r.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateId
		reply.VoteGranted = true
		r.resetElectionTimer()
		r.persist()
	}

	return nil
}

// RPC handler for AppendEntries
func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	reply.Term = r.currentTerm
	reply.Success = false

	if args.Term < r.currentTerm {
		return nil
	}

	r.resetElectionTimer()

	// If we are candidate and receive AppendEntries from a valid leader, become follower
	if r.state == Candidate {
		r.becomeFollower(args.Term)
	}

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(r.log) {
			return nil
		}
		if r.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return nil
		}
	}

	// Truncate conflicting entries and append new ones
	logInsertIndex := args.PrevLogIndex
	for i, entry := range args.Entries {
		if logInsertIndex < len(r.log) {
			if r.log[logInsertIndex].Term != entry.Term {
				r.log = r.log[:logInsertIndex]
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
		} else {
			r.log = append(r.log, entry)
		}
		logInsertIndex++
	}

	if len(args.Entries) > 0 {
		r.persist()
	}

	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, len(r.log))
		r.applyCond.Broadcast()
	}

	reply.Success = true
	return nil
}

func (r *Raft) isUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
	lastIndex := len(r.log)
	lastTerm := 0
	if lastIndex > 0 {
		lastTerm = r.log[lastIndex-1].Term
	}

	if candidateLastLogTerm != lastTerm {
		return candidateLastLogTerm > lastTerm
	}
	return candidateLastLogIndex >= lastIndex
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
