package raft

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

type Persister struct {
	mu       sync.Mutex
	filename string
}

func NewPersister(nodeId string) *Persister {
	return &Persister{
		filename: "raft-data-" + nodeId + ".json",
	}
}

type persistState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

func (p *Persister) SaveRaftState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := ioutil.WriteFile(p.filename, state, 0644)
	if err != nil {
		log.Printf("Failed to write raft state: %v", err)
	}
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := ioutil.ReadFile(p.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		log.Printf("Failed to read raft state: %v", err)
		return nil
	}
	return data
}

func (r *Raft) persist() {
	state := persistState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	}
	data, err := json.Marshal(state)
	if err == nil {
		r.persister.SaveRaftState(data)
	}
}

func (r *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	var state persistState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("[%s] failed to unmarshal persistent state", r.me)
		return
	}
	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.log = state.Log
}
