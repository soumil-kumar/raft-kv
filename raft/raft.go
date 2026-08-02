package raft

import (
	"log"
	"math/rand"
	"sync"
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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex
	me        string
	peers     []string // Addresses of peers
	server    *Server
	persister *Persister

	// Persistent state
	currentTerm int
	votedFor    string
	log         []LogEntry

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

	lastContact time.Time
}

func New(me string, peers []string, server *Server, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{
		me:        me,
		peers:     peers,
		server:    server,
		persister: persister,
		applyCh:   applyCh,
		state:     Follower,
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.readPersist(persister.ReadRaftState())

	go r.ticker()
	go r.applier()

	return r
}

func (r *Raft) Start(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return -1, -1, false
	}

	index := len(r.log) + 1
	term := r.currentTerm
	r.log = append(r.log, LogEntry{Term: term, Command: command})
	r.persist()

	r.broadcastAppendEntries()

	return index, term, true
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

// GetCommittedLog returns all log entries that were persisted.
// On startup after a crash, ALL entries in the persisted log are treated as
// committed because they were replicated to a majority before the crash.
// The store uses this to rebuild its in-memory state.
func (r *Raft) GetCommittedLog() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]LogEntry, len(r.log))
	copy(entries, r.log)
	return entries
}

func (r *Raft) ticker() {
	for {
		r.mu.Lock()
		state := r.state
		r.mu.Unlock()

		if state == Leader {
			r.mu.Lock()
			r.broadcastAppendEntries()
			r.mu.Unlock()
			time.Sleep(50 * time.Millisecond) // Heartbeat interval
		} else {
			r.mu.Lock()
			timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
			if time.Since(r.lastContact) > timeout {
				r.startElection()
			}
			r.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.me
	r.leaderId = ""
	r.persist()
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
				if votes > len(r.peers)/2 {
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
	r.persist()
	r.resetElectionTimer()
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.leaderId = r.me
	log.Printf("[%s] Became leader for term %d", r.me, r.currentTerm)
	r.nextIndex = make(map[string]int)
	r.matchIndex = make(map[string]int)
	for _, peer := range r.peers {
		if peer != r.me {
			r.nextIndex[peer] = len(r.log) + 1
			r.matchIndex[peer] = 0
		}
	}
	r.broadcastAppendEntries()
}

func (r *Raft) resetElectionTimer() {
	r.lastContact = time.Now()
}

func (r *Raft) broadcastAppendEntries() {
	for _, peer := range r.peers {
		if peer == r.me {
			continue
		}
		go r.sendAppendEntriesToPeer(peer)
	}
}

func (r *Raft) sendAppendEntriesToPeer(peer string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	prevLogIndex := r.nextIndex[peer] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = r.log[prevLogIndex-1].Term
	}

	entries := r.log[prevLogIndex:]

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
	defer r.mu.Unlock()

	if r.state != Leader || args.Term != r.currentTerm {
		return
	}

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		r.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		r.matchIndex[peer] = r.nextIndex[peer] - 1

		// Check if we can advance commitIndex
		for i := len(r.log); i > r.commitIndex; i-- {
			if r.log[i-1].Term == r.currentTerm {
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
	} else {
		// Back off nextIndex
		r.nextIndex[peer]--
		if r.nextIndex[peer] < 1 {
			r.nextIndex[peer] = 1
		}
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
		entries := r.log[lastApplied:commitIndex]
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
