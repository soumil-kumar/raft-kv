package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// ApplyMsg is sent to the application (KV store) when a command is committed
type ApplyMsg struct {
	CommandValid  bool
	Command       []byte
	CommandIndex  int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

type Raft struct {
	mu        sync.Mutex
	me        string
	peers     []string // Addresses of peers
	server    *Server
	persister *Persister

	// Persistent state
	currentTerm       int
	votedFor          string
	log               []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state
	state       State
	leaderId    string
	commitIndex int
	lastApplied int

	// Volatile state for leaders
	nextIndex  map[string]int
	matchIndex map[string]int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	resetTimerCh chan struct{}
	diskCond     *sync.Cond
	unpersisted  int
	inFlight     map[string]bool
	
	// Buffer for receiving chunked snapshots
	snapshotBuffer []byte
}

func New(me string, peers []string, server *Server, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{
		me:        me,
		peers:     peers,
		server:    server,
		persister: persister,
		applyCh:      applyCh,
		state:        Follower,
		resetTimerCh: make(chan struct{}, 1),
		inFlight:     make(map[string]bool),
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.diskCond = sync.NewCond(&r.mu)
	
	term, votedFor, lastIndex, lastTerm, snapshot, logEntries := persister.ReadState()
	r.currentTerm = term
	r.votedFor = votedFor
	r.lastIncludedIndex = lastIndex
	r.lastIncludedTerm = lastTerm
	
	if len(logEntries) == 0 {
		r.log = make([]LogEntry, 1)
		r.log[0] = LogEntry{Term: lastTerm}
	} else {
		r.log = make([]LogEntry, 1)
		r.log[0] = LogEntry{Term: lastTerm}
		r.log = append(r.log, logEntries...)
	}

	r.commitIndex = lastIndex
	r.lastApplied = lastIndex

	if snapshot != nil {
		go func() {
			applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotIndex: lastIndex,
				SnapshotTerm:  lastTerm,
			}
		}()
	}

	go r.ticker()
	go r.applier()
	go r.diskLoop()

	return r
}

func (r *Raft) Start(command []byte) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return -1, -1, false
	}

	index := r.lastIncludedIndex + len(r.log)
	term := r.currentTerm
	entry := LogEntry{Term: term, Command: command}
	r.log = append(r.log, entry)
	r.unpersisted++
	r.diskCond.Broadcast()

	return index, term, true
}

// ReadIndex implements the Raft ReadIndex protocol for linearizable reads
// without appending a new entry to the log.
func (r *Raft) ReadIndex() (int, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, fmt.Errorf("not leader")
	}
	readIndex := r.commitIndex
	term := r.currentTerm
	
	// Create a copy of peers to avoid holding lock during loop
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	me := r.me
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLogTerm(lastLogIndex)
	server := r.server
	r.mu.Unlock()

	var wg sync.WaitGroup
	var successCount int32 = 1 // Vote for self

	for _, peer := range peers {
		if peer == me {
			continue
		}
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				PrevLogIndex: lastLogIndex,
				PrevLogTerm:  lastLogTerm,
				Entries:      nil,
				LeaderCommit: readIndex, // Send current commit index
			}
			var reply AppendEntriesReply
			if server.Call(p, "Raft.AppendEntries", args, &reply) {
				if reply.Success {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(peer)
	}

	// Wait with a small timeout for heartbeats
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	if atomic.LoadInt32(&successCount) <= int32(len(peers)/2) {
		return 0, fmt.Errorf("lost quorum")
	}

	// Wait for state machine to apply up to readIndex
	r.mu.Lock()
	for r.lastApplied < readIndex {
		r.applyCond.Wait()
	}
	r.mu.Unlock()

	return readIndex, nil
}

func (r *Raft) diskLoop() {
	for {
		r.mu.Lock()
		for r.unpersisted == 0 {
			r.diskCond.Wait()
		}
		
		unpersisted := r.unpersisted
		entriesToPersist := make([]LogEntry, unpersisted)
		copy(entriesToPersist, r.log[len(r.log)-unpersisted:])
		r.unpersisted = 0
		r.mu.Unlock()
		
		r.persister.AppendLogs(entriesToPersist)
		r.persister.Sync()
		
		r.mu.Lock()
		if r.state == Leader {
			r.broadcastAppendEntries()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == Leader
}

func (r *Raft) GetLeaderID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderId
}

func (r *Raft) getLastLogIndex() int {
	return r.lastIncludedIndex + len(r.log) - 1
}

func (r *Raft) getLastLogTerm() int {
	return r.log[len(r.log)-1].Term
}

func (r *Raft) getLogTerm(index int) int {
	if index < r.lastIncludedIndex || index > r.getLastLogIndex() {
		return 0
	}
	return r.log[index-r.lastIncludedIndex].Term
}

// Snapshot is called by the state machine to truncate the log.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.lastIncludedIndex || index > r.commitIndex {
		return
	}

	// Truncate in-memory log
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: r.getLogTerm(index)}
	
	tail := r.log[index-r.lastIncludedIndex+1:]
	newLog = append(newLog, tail...)

	r.lastIncludedTerm = newLog[0].Term
	r.lastIncludedIndex = index
	r.log = newLog

	// Persist state and snapshot
	tailCopy := make([]LogEntry, len(tail))
	copy(tailCopy, tail)
	r.persister.SaveStateAndSnapshot(r.currentTerm, r.votedFor, r.lastIncludedIndex, r.lastIncludedTerm, snapshot, tailCopy)
}

// GetCommittedLog returns all log entries that were persisted and committed.
// On startup after a crash, this is used by the store to rebuild its in-memory state.
func (r *Raft) GetCommittedLog() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]LogEntry, r.commitIndex)
	copy(entries, r.log[:r.commitIndex])
	return entries
}

func (r *Raft) ticker() {
	electionTimeout := func() time.Duration {
		return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
	}
	timer := time.NewTimer(electionTimeout())
	heartbeat := time.NewTicker(300 * time.Millisecond)
	
	for {
		select {
		case <-r.resetTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(electionTimeout())
		case <-timer.C:
			r.mu.Lock()
			if r.state != Leader {
				r.startElection()
			}
			r.mu.Unlock()
			timer.Reset(electionTimeout())
		case <-heartbeat.C:
			r.mu.Lock()
			if r.state == Leader {
				r.broadcastAppendEntries()
			}
			r.mu.Unlock()
		}
	}
}

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.me
	r.leaderId = ""
	r.persister.SaveMetadata(r.currentTerm, r.votedFor)
	r.resetElectionTimer()

	log.Printf("[%s] Starting election for term %d", r.me, r.currentTerm)

	votes := 1
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.me,
		LastLogIndex: len(r.log),
		LastLogTerm:  0,
	}
	if len(r.log) > 0 {
		args.LastLogTerm = r.log[len(r.log)-1].Term
	}

	for _, peer := range r.peers {
		if peer == r.me {
			continue
		}
		go func(peer string) {
			reply := RequestVoteReply{}
			ok := r.server.Call(peer, "Raft.RequestVote", args, &reply)
			if !ok {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Candidate || r.currentTerm != args.Term {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				if votes == len(r.peers)/2 + 1 {
					r.becomeLeader()
				}
			}
		}(peer)
	}
}

func (r *Raft) becomeFollower(term int) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.leaderId = ""
	r.persister.SaveMetadata(r.currentTerm, r.votedFor)
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderId = r.me
	log.Printf("[%s] Became leader for term %d", r.me, r.currentTerm)
	r.nextIndex = make(map[string]int)
	r.matchIndex = make(map[string]int)
	r.inFlight = make(map[string]bool)
	for _, peer := range r.peers {
		if peer != r.me {
			r.nextIndex[peer] = r.getLastLogIndex() + 1
			r.matchIndex[peer] = 0
			r.inFlight[peer] = false
		}
	}
	
	// Commit a no-op entry to establish the true commitIndex for this term
	entry := LogEntry{Term: r.currentTerm, Command: []byte{}}
	r.log = append(r.log, entry)
	r.unpersisted++
	r.diskCond.Broadcast()
}

func (r *Raft) resetElectionTimer() {
	select {
	case r.resetTimerCh <- struct{}{}:
	default:
	}
}

func (r *Raft) broadcastAppendEntries() {
	for _, peer := range r.peers {
		if peer == r.me {
			continue
		}
		if !r.inFlight[peer] {
			r.inFlight[peer] = true
			go r.sendAppendEntriesToPeer(peer)
		}
	}
}

func (r *Raft) sendAppendEntriesToPeer(peer string) {
	defer func() {
		r.mu.Lock()
		r.inFlight[peer] = false
		r.mu.Unlock()
	}()

	for {
		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return
		}

		prevLogIndex := r.nextIndex[peer] - 1

		if prevLogIndex < r.lastIncludedIndex {
			// Send InstallSnapshot in chunks
			snapshot := r.persister.ReadSnapshot()
			offset := 0
			chunkSize := 32768 // 32KB

			for {
				end := offset + chunkSize
				if end > len(snapshot) {
					end = len(snapshot)
				}
				
				args := InstallSnapshotArgs{
					Term:              r.currentTerm,
					LeaderId:          r.me,
					LastIncludedIndex: r.lastIncludedIndex,
					LastIncludedTerm:  r.lastIncludedTerm,
					Data:              snapshot[offset:end],
					Offset:            offset,
					Done:              end == len(snapshot),
				}
				r.mu.Unlock()

				reply := InstallSnapshotReply{}
				ok := r.server.Call(peer, "Raft.InstallSnapshot", args, &reply)
				if !ok {
					return
				}

				r.mu.Lock()
				if r.state != Leader || args.Term != r.currentTerm {
					r.mu.Unlock()
					return
				}

				if reply.Term > r.currentTerm {
					r.becomeFollower(reply.Term)
					r.mu.Unlock()
					return
				}
				
				offset = end
				if args.Done {
					r.nextIndex[peer] = args.LastIncludedIndex + 1
					r.matchIndex[peer] = args.LastIncludedIndex
					r.mu.Unlock()
					break
				}
			}
			continue
		}

		prevLogTerm := r.getLogTerm(prevLogIndex)

		entriesCount := r.getLastLogIndex() - prevLogIndex
		entries := make([]LogEntry, entriesCount)
		if entriesCount > 0 {
			copy(entries, r.log[prevLogIndex-r.lastIncludedIndex+1:])
		}

		args := AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderId:     r.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		r.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := r.server.Call(peer, "Raft.AppendEntries", args, &reply)
		if !ok {
			return
		}

		r.mu.Lock()
		if r.state != Leader || args.Term != r.currentTerm {
			r.mu.Unlock()
			return
		}

		if reply.Term > r.currentTerm {
			r.becomeFollower(reply.Term)
			r.mu.Unlock()
			return
		}

		if reply.Success {
			r.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			r.matchIndex[peer] = r.nextIndex[peer] - 1

			// Check if we can advance commitIndex
			for i := r.getLastLogIndex(); i > r.commitIndex && i > r.lastIncludedIndex; i-- {
				if r.getLogTerm(i) == r.currentTerm {
					matchCount := 1 // self
					for p, match := range r.matchIndex {
						if p != r.me && match >= i {
							matchCount++
						}
					}
					if matchCount > len(r.peers)/2 {
						r.commitIndex = i
						r.applyCond.Broadcast()
						break
					}
				}
			}

			if r.nextIndex[peer] > r.getLastLogIndex() {
				r.mu.Unlock()
				return
			}
		} else {
			// Back off nextIndex using fast backtracking
			if reply.ConflictTerm == -1 {
				r.nextIndex[peer] = reply.ConflictIndex
			} else {
				lastIndexForTerm := -1
				for i := r.getLastLogIndex(); i > r.lastIncludedIndex; i-- {
					if r.getLogTerm(i) == reply.ConflictTerm {
						lastIndexForTerm = i
						break
					}
				}
				if lastIndexForTerm != -1 {
					r.nextIndex[peer] = lastIndexForTerm + 1
				} else {
					r.nextIndex[peer] = reply.ConflictIndex
				}
			}

			if r.nextIndex[peer] < 1 {
				r.nextIndex[peer] = 1
			}
		}
		r.mu.Unlock()
	}
}

func (r *Raft) applier() {
	for {
		r.mu.Lock()
		for r.commitIndex <= r.lastApplied {
			r.applyCond.Wait()
		}
		commitIndex := r.commitIndex
		lastApplied := r.lastApplied
		
		if lastApplied < r.lastIncludedIndex {
			lastApplied = r.lastIncludedIndex
		}

		var entries []LogEntry
		if commitIndex > lastApplied {
			entries = make([]LogEntry, commitIndex-lastApplied)
			copy(entries, r.log[lastApplied-r.lastIncludedIndex+1 : commitIndex-r.lastIncludedIndex+1])
		}
		r.mu.Unlock()

		for i, entry := range entries {
			r.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + i + 1,
			}
		}

		r.mu.Lock()
		r.lastApplied = max(r.lastApplied, commitIndex)
		r.mu.Unlock()
	}
}

