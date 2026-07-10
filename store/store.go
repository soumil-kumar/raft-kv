package store

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raft-kv/raft"
)

var globalReqID int64

type command struct {
	Action      string `json:"action"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	ReqID       string `json:"req_id"`
	ClientID    string `json:"client_id,omitempty"`
	SequenceNum int    `json:"sequence_num,omitempty"`
}

const (
	actionSet    = "set"
	actionDelete = "delete"
	actionRead   = "read"
)

// Store is the core distributed key-value store.
// It wraps a custom Raft consensus instance and an in-memory map.
type Store struct {
	nodeID   string
	raftAddr string
	peers    []string

	raft   *raft.Raft
	server *raft.Server

	mu      sync.RWMutex
	kvStore map[string]string
	pending map[string]chan error

	peerHttpAddrs map[string]string
	lastApplied   map[string]int

	applyCh chan raft.ApplyMsg
}

// New creates a new Store instance.
func New(nodeID, raftAddr string, peers []string, peerHttpAddrs map[string]string) *Store {
	return &Store{
		nodeID:        nodeID,
		raftAddr:      raftAddr,
		peers:         peers,
		kvStore:       make(map[string]string),
		pending:       make(map[string]chan error),
		peerHttpAddrs: peerHttpAddrs,
		lastApplied:   make(map[string]int),
		applyCh:       make(chan raft.ApplyMsg, 100),
	}
}

// Open initializes the Raft subsystem and starts this node.
func (s *Store) Open() error {
	s.server = raft.NewServer(s.raftAddr, nil)
	persister := raft.NewPersister(s.nodeID)

	s.raft = raft.New(s.raftAddr, s.peers, s.server, persister, s.applyCh)

	// The KV state machine will now be naturally rebuilt through the Raft protocol.
	// When the node joins the cluster (or becomes leader), its commitIndex will advance,
	// and the Raft applier will push all committed log entries through applyCh.
	
	go s.applyLoop()

	if err := s.server.Start(s.raft); err != nil {
		return fmt.Errorf("failed to start raft server: %w", err)
	}

	return nil
}

func (s *Store) applyLoop() {
	for msg := range s.applyCh {
		if msg.CommandValid {
			s.applyCommand(msg.Command, msg.CommandIndex)
			
			// Snapshot every 1000 entries
			if msg.CommandIndex > 0 && msg.CommandIndex%1000 == 0 {
				s.mu.RLock()
				snapshotData := s.serializeState()
				s.mu.RUnlock()
				s.raft.Snapshot(msg.CommandIndex, snapshotData)
			}
		} else if msg.SnapshotValid {
			s.mu.Lock()
			s.deserializeState(msg.Snapshot)
			s.mu.Unlock()
			log.Printf("Loaded snapshot up to index %d", msg.SnapshotIndex)
		}
	}
}

type SnapshotState struct {
	KVStore     map[string]string
	LastApplied map[string]int
}

func (s *Store) serializeState() []byte {
	state := SnapshotState{
		KVStore:     s.kvStore,
		LastApplied: s.lastApplied,
	}
	data, _ := json.Marshal(state)
	return data
}

func (s *Store) deserializeState(data []byte) {
	if len(data) == 0 {
		return
	}
	var state SnapshotState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("Failed to unmarshal snapshot: %v", err)
		return
	}
	s.kvStore = state.KVStore
	s.lastApplied = state.LastApplied
}

// applyCommand applies a single command to the KV store.
func (s *Store) applyCommand(rawCmd []byte, index int) {
	if len(rawCmd) == 0 {
		return // Ignore Raft no-op entries
	}

	var cmd command
	if err := json.Unmarshal(rawCmd, &cmd); err != nil {
		log.Printf("applyCommand: failed to unmarshal at index %d: %v", index, err)
		return
	}

	s.mu.Lock()
	if cmd.ClientID != "" && cmd.SequenceNum > 0 {
		if s.lastApplied[cmd.ClientID] >= cmd.SequenceNum {
			log.Printf("Ignored duplicate cmd seq %d for client %s", cmd.SequenceNum, cmd.ClientID)
			if cmd.ReqID != "" {
				if ch, ok := s.pending[cmd.ReqID]; ok {
					ch <- nil
					delete(s.pending, cmd.ReqID)
				}
			}
			s.mu.Unlock()
			return
		}
		s.lastApplied[cmd.ClientID] = cmd.SequenceNum
	}

	switch cmd.Action {
	case actionSet:
		s.kvStore[cmd.Key] = cmd.Value
	case actionDelete:
		delete(s.kvStore, cmd.Key)
	case actionRead:
		// No-op for state machine
	}

	if cmd.ReqID != "" {
		log.Printf("APPLIED ReqID=%s", cmd.ReqID)
		if ch, ok := s.pending[cmd.ReqID]; ok {
			ch <- nil
			delete(s.pending, cmd.ReqID)
		}
	}
	s.mu.Unlock()
}

func (s *Store) proposeAndWait(cmd command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	ch := make(chan error, 1)
	s.mu.Lock()
	s.pending[cmd.ReqID] = ch
	s.mu.Unlock()

	_, _, isLeader := s.raft.Start(b)
	if !isLeader {
		s.mu.Lock()
		delete(s.pending, cmd.ReqID)
		s.mu.Unlock()
		return ErrNotLeader
	}

	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		s.mu.Lock()
		delete(s.pending, cmd.ReqID)
		s.mu.Unlock()
		log.Printf("TIMEOUT waiting for command ReqID=%s", cmd.ReqID)
		return fmt.Errorf("timeout waiting for command to be applied")
	}
}

// Set stores a key-value pair in the distributed store.
func (s *Store) Set(key, value, clientID string, seqNum int) error {
	if !s.raft.IsLeader() {
		return ErrNotLeader
	}
	cmd := command{
		Action:      actionSet,
		Key:         key,
		Value:       value,
		ClientID:    clientID,
		SequenceNum: seqNum,
		ReqID:       fmt.Sprintf("%s-%d", s.nodeID, atomic.AddInt64(&globalReqID, 1)),
	}
	return s.proposeAndWait(cmd)
}

// Get retrieves the value for a key from the local state.
func (s *Store) Get(key string, consistent bool) (string, bool, error) {
	if consistent {
		if _, err := s.raft.ReadIndex(); err != nil {
			if err.Error() == "not leader" {
				return "", false, ErrNotLeader
			}
			return "", false, err
		}
	}

	s.mu.RLock()
	val, ok := s.kvStore[key]
	s.mu.RUnlock()
	
	return val, ok, nil
}

// Delete removes a key from the distributed store.
func (s *Store) Delete(key, clientID string, seqNum int) error {
	if !s.raft.IsLeader() {
		return ErrNotLeader
	}
	cmd := command{
		Action:      actionDelete,
		Key:         key,
		ClientID:    clientID,
		SequenceNum: seqNum,
		ReqID:       fmt.Sprintf("%s-%d", s.nodeID, atomic.AddInt64(&globalReqID, 1)),
	}
	return s.proposeAndWait(cmd)
}

// LeaderAddr returns the address of the current Raft leader.
func (s *Store) LeaderAddr() string {
	return s.raft.GetLeaderID()
}

// LeaderHTTPAddr returns the HTTP address of the current Raft leader.
func (s *Store) LeaderHTTPAddr() string {
	leaderAddr := s.LeaderAddr()
	if leaderAddr == "" {
		return ""
	}
	if httpAddr, ok := s.peerHttpAddrs[leaderAddr]; ok {
		return httpAddr
	}
	return ""
}

// IsLeader returns true if this node is the current Raft leader.
func (s *Store) IsLeader() bool {
	return s.raft.IsLeader()
}

// NodeID returns this node's unique identifier.
func (s *Store) NodeID() string {
	return s.nodeID
}

// GetAll returns all key-value pairs in the store.
func (s *Store) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copy := make(map[string]string)
	for k, v := range s.kvStore {
		copy[k] = v
	}
	return copy
}

// ErrNotLeader is returned when a write operation is attempted on a non-leader node.
var ErrNotLeader = fmt.Errorf("not the leader")

func (s *Store) Stats() map[string]string {
	return map[string]string{"type": "custom from-scratch raft"}
}