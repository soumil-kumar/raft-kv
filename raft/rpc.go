package raft

import "log"

type LogEntry struct {
	Term    int
	Command []byte
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
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
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
		r.persister.SaveMetadata(r.currentTerm, r.votedFor)
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
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0
	
	log.Printf("[%s] AppendEntries args: PrevLogIndex=%d, PrevLogTerm=%d, len(Entries)=%d | local: lastLogIndex=%d, commitIndex=%d", 
		r.me, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), r.getLastLogIndex(), r.commitIndex)

	if args.Term < r.currentTerm {
		return nil
	}

	r.resetElectionTimer()
	r.leaderId = args.LeaderId

	// If we are candidate and receive AppendEntries from a valid leader, become follower
	if r.state == Candidate {
		r.becomeFollower(args.Term)
		r.leaderId = args.LeaderId
	}

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > r.getLastLogIndex() {
			reply.ConflictIndex = r.getLastLogIndex() + 1
			reply.ConflictTerm = -1
			return nil
		}
		if args.PrevLogIndex < r.lastIncludedIndex {
			reply.ConflictIndex = r.lastIncludedIndex + 1
			reply.ConflictTerm = -1
			return nil
		}
		
		if r.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.ConflictTerm = r.getLogTerm(args.PrevLogIndex)
			for i := args.PrevLogIndex - 1; i >= r.lastIncludedIndex; i-- {
				if r.getLogTerm(i) != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
				if i == r.lastIncludedIndex {
					reply.ConflictIndex = r.lastIncludedIndex + 1
				}
			}
			return nil
		}
	}

	newEntries := []LogEntry{}
	for i, entry := range args.Entries {
		absIndex := args.PrevLogIndex + 1 + i
		if absIndex <= r.getLastLogIndex() {
			if r.getLogTerm(absIndex) != entry.Term {
				r.log = r.log[:absIndex-r.lastIncludedIndex]
				r.persister.TruncateLog(absIndex)
				// Append all remaining entries from this point
				remaining := args.Entries[i:]
				r.log = append(r.log, remaining...)
				newEntries = append(newEntries, remaining...)
				break
			}
		} else {
			r.log = append(r.log, entry)
			newEntries = append(newEntries, entry)
		}
	}
	r.persister.AppendLogs(newEntries)
	r.persister.Sync() // Synchronous fsync before acknowledging success

	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, r.getLastLogIndex())
		r.applyCond.Broadcast()
	}

	reply.Success = true
	return nil
}

func (r *Raft) isUpToDate(candidateLastLogIndex, candidateLastLogTerm int) bool {
	lastIndex := r.getLastLogIndex()
	lastTerm := r.getLastLogTerm()

	if candidateLastLogTerm != lastTerm {
		return candidateLastLogTerm > lastTerm
	}
	return candidateLastLogIndex >= lastIndex
}

// RPC handler for InstallSnapshot
func (r *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return nil
	}

	r.resetElectionTimer()
	r.leaderId = args.LeaderId

	if args.LastIncludedIndex <= r.commitIndex {
		return nil
	}

	// Truncate or replace log
	if args.LastIncludedIndex <= r.getLastLogIndex() && r.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Keep tail
		tail := r.log[args.LastIncludedIndex-r.lastIncludedIndex+1:]
		newLog := make([]LogEntry, 1)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		newLog = append(newLog, tail...)
		r.log = newLog
	} else {
		// Discard entire log
		r.log = make([]LogEntry, 1)
		r.log[0] = LogEntry{Term: args.LastIncludedTerm}
	}

	r.lastIncludedIndex = args.LastIncludedIndex
	r.lastIncludedTerm = args.LastIncludedTerm
	r.commitIndex = args.LastIncludedIndex
	r.lastApplied = args.LastIncludedIndex

	if args.Offset == 0 {
		r.snapshotBuffer = nil
	}
	r.snapshotBuffer = append(r.snapshotBuffer, args.Data...)

	if args.Done {
		tailCopy := make([]LogEntry, len(r.log)-1)
		copy(tailCopy, r.log[1:])
		
		// Take a copy of the buffer to pass to applyCh and persister
		snapshotData := make([]byte, len(r.snapshotBuffer))
		copy(snapshotData, r.snapshotBuffer)
		
		r.persister.SaveStateAndSnapshot(r.currentTerm, r.votedFor, r.lastIncludedIndex, r.lastIncludedTerm, snapshotData, tailCopy)

		go func() {
			r.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshotData,
				SnapshotIndex: args.LastIncludedIndex,
				SnapshotTerm:  args.LastIncludedTerm,
			}
		}()
		
		// Clear buffer after successful application
		r.snapshotBuffer = nil
	}

	return nil
}

