// Package store is a replicated key-value map built on the raft package.
package store

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/soumil-kumar/raft-kv/raft"
)

// Store exposes the map through consensus.
type Store struct {
	fsm  *fsm
	raft *raft.Raft
}

// Open recovers the state machine and starts a Raft node for it.
func Open(cfg raft.Config, storage raft.Storage, transport raft.Transport) (*Store, error) {
	f := newFSM()
	r, err := raft.New(cfg, storage, transport, f)
	if err != nil {
		return nil, err
	}
	return &Store{fsm: f, raft: r}, nil
}

// Raft returns the underlying consensus node.
func (s *Store) Raft() *raft.Raft { return s.raft }

func (s *Store) write(ctx context.Context, c command) (Result, error) {
	v, err := s.raft.Propose(ctx, c.encode())
	if err != nil {
		return Result{}, err
	}
	out, ok := v.(outcome)
	if !ok {
		return Result{}, fmt.Errorf("store: unexpected result %T", v)
	}
	return out.Result, out.err()
}

// Set stores value under key.
func (s *Store) Set(ctx context.Context, key, value string, o WriteOptions) (Result, error) {
	return s.write(ctx, newCommand(opSet, key, value, o))
}

// Delete removes key. Deleting a missing key succeeds with Existed == false.
func (s *Store) Delete(ctx context.Context, key string, o WriteOptions) (Result, error) {
	return s.write(ctx, newCommand(opDelete, key, "", o))
}

// Get reads key. Unless stale is set the read is linearizable, on any node.
func (s *Store) Get(ctx context.Context, key string, stale bool) (Item, bool, error) {
	if !stale {
		if err := s.raft.LinearizableRead(ctx); err != nil {
			return Item{}, false, err
		}
	}
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	it, ok := s.fsm.items[key]
	return it, ok, nil
}

// KV is one entry of a scan.
type KV struct {
	Key string
	Item
}

// Scan returns up to limit keys with the given prefix, in key order.
func (s *Store) Scan(ctx context.Context, prefix string, limit int, stale bool) ([]KV, error) {
	if !stale {
		if err := s.raft.LinearizableRead(ctx); err != nil {
			return nil, err
		}
	}
	s.fsm.mu.RLock()
	var out []KV
	for k, it := range s.fsm.items {
		if strings.HasPrefix(k, prefix) {
			out = append(out, KV{k, it})
		}
	}
	s.fsm.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Len returns the number of keys held locally.
func (s *Store) Len() int {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return len(s.fsm.items)
}

// Close stops the node.
func (s *Store) Close() { s.raft.Stop() }
