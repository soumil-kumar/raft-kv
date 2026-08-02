package store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/raft-kv/raft"
)

type command struct {
	Action string `json:"action"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

const (
	actionSet    = "set"
	actionDelete = "delete"
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

	applyCh chan raft.ApplyMsg
}

// New creates a new Store instance.
func New(nodeID, raftAddr string, peers []string) *Store {
	return &Store{
		nodeID:   nodeID,
		raftAddr: raftAddr,
		peers:    peers,
		kvStore:  make(map[string]string),
		applyCh:  make(chan raft.ApplyMsg, 100),
	}
}

// Open initializes the Raft subsystem and starts this node.
func (s *Store) Open() error {
	s.server = raft.NewServer(s.raftAddr)
	persister := raft.NewPersister(s.nodeID)

	s.raft = raft.New(s.raftAddr, s.peers, s.server, persister, s.applyCh)

	// Replay persisted log entries to rebuild in-memory KV state.
	// On a restart, the Raft log is restored from disk but the KV map is empty.
	// We replay all entries to reconstruct the store's state.
	entries := s.raft.GetCommittedLog()
	for i, entry := range entries {
		s.applyCommand(entry.Command, i+1)
	}
	log.Printf("[store] replayed %d entries from persisted log", len(entries))

	if err := s.server.Start(s.raft); err != nil {
		return fmt.Errorf("failed to start raft server: %w", err)
	}

	go s.applyLoop()

	return nil
}

func (s *Store) applyLoop() {
	for msg := range s.applyCh {
		if msg.CommandValid {
			s.applyCommand(msg.Command, msg.CommandIndex)
		}
	}
}

// applyCommand applies a single command to the KV store.
// The command may arrive as []byte (during normal operation) or as a string
// (after JSON deserialization from persistence). When Go JSON-marshals a []byte,
// it produces a base64-encoded string. On unmarshal into interface{}, this comes
// back as a plain string containing the base64 text, which we must decode.
func (s *Store) applyCommand(rawCmd interface{}, index int) {
	var b []byte
	switch v := rawCmd.(type) {
	case []byte:
		b = v
	case string:
		// JSON roundtrip: []byte -> base64 string -> interface{} gives us a base64 string.
		decoded, err := base64Decode(v)
		if err != nil {
			// Not base64, try as raw JSON string
			b = []byte(v)
		} else {
			b = decoded
		}
	default:
		var err error
		b, err = json.Marshal(rawCmd)
		if err != nil {
			log.Printf("applyCommand: unsupported command type %T at index %d", rawCmd, index)
			return
		}
	}

	var cmd command
	if err := json.Unmarshal(b, &cmd); err != nil {
		log.Printf("applyCommand: failed to unmarshal at index %d: %v", index, err)
		return
	}

	s.mu.Lock()
	switch cmd.Action {
	case actionSet:
		s.kvStore[cmd.Key] = cmd.Value
		log.Printf("Applied SET %s=%s (index %d)", cmd.Key, cmd.Value, index)
	case actionDelete:
		delete(s.kvStore, cmd.Key)
		log.Printf("Applied DELETE %s (index %d)", cmd.Key, index)
	}
	s.mu.Unlock()
}

// Set stores a key-value pair in the distributed store.
func (s *Store) Set(key, value string) error {
	if !s.raft.IsLeader() {
		return ErrNotLeader
	}

	cmd := command{
		Action: actionSet,
		Key:    key,
		Value:  value,
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	_, _, isLeader := s.raft.Start(b)
	if !isLeader {
		return ErrNotLeader
	}

	// Wait a bit for the command to be applied.
	// In a real system, we'd wait for the specific index to be applied.
	time.Sleep(200 * time.Millisecond)

	return nil
}

// Get retrieves the value for a key from the local state.
func (s *Store) Get(key string, consistent bool) (string, bool, error) {
	if consistent {
		if !s.raft.IsLeader() {
			return "", false, ErrNotLeader
		}
		// A real consistent read would send a no-op through the log.
		// For simplicity, we just check if we think we're the leader.
	}

	s.mu.RLock()
	val, ok := s.kvStore[key]
	s.mu.RUnlock()
	
	return val, ok, nil
}

// Delete removes a key from the distributed store.
func (s *Store) Delete(key string) error {
	if !s.raft.IsLeader() {
		return ErrNotLeader
	}

	cmd := command{
		Action: actionDelete,
		Key:    key,
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	_, _, isLeader := s.raft.Start(b)
	if !isLeader {
		return ErrNotLeader
	}

	time.Sleep(200 * time.Millisecond)

	return nil
}

// LeaderAddr returns the address of the current Raft leader.
func (s *Store) LeaderAddr() string {
	return s.raft.GetLeaderID()
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

// base64Decode attempts to decode a base64-encoded string.
func base64Decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}