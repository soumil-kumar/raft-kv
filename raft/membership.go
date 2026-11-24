package raft

import (
	"context"
	"fmt"
)

// promoteLag is how close a learner's log must be before it may vote.
const promoteLag = 256

// AddLearner adds a non-voting member. It replicates the log but counts towards
// no quorum, so a new node can catch up without hurting availability.
func (r *Raft) AddLearner(ctx context.Context, m Member) error {
	m.Learner = true
	return r.changeConfig(ctx, func(c Configuration) (Configuration, error) {
		if _, ok := c.Find(m.ID); ok {
			return c, fmt.Errorf("raft: member %q already exists", m.ID)
		}
		c.Members = append(c.Members, m)
		return c, nil
	})
}

// Promote turns a caught-up learner into a voter.
func (r *Raft) Promote(ctx context.Context, id string) error {
	return r.changeConfig(ctx, func(c Configuration) (Configuration, error) {
		for i := range c.Members {
			if c.Members[i].ID != id {
				continue
			}
			if !c.Members[i].Learner {
				return c, fmt.Errorf("raft: member %q is already a voter", id)
			}
			p := r.peers[id]
			if p == nil || p.match+promoteLag < r.log.lastIndex() {
				return c, fmt.Errorf("raft: member %q has not caught up yet", id)
			}
			c.Members[i].Learner = false
			return c, nil
		}
		return c, fmt.Errorf("raft: no member %q", id)
	})
}

// RemoveMember removes a voter or learner. A leader that removes itself steps
// down once the change commits.
func (r *Raft) RemoveMember(ctx context.Context, id string) error {
	return r.changeConfig(ctx, func(c Configuration) (Configuration, error) {
		for i, m := range c.Members {
			if m.ID == id {
				c.Members = append(c.Members[:i], c.Members[i+1:]...)
				return c, nil
			}
		}
		return c, fmt.Errorf("raft: no member %q", id)
	})
}

// changeConfig commits a single-server change. Changing one voter at a time
// guarantees old and new majorities overlap, provided the previous change has
// committed and the leader has committed something in its own term (the latter
// fixes the known flaw in the originally published single-server algorithm).
func (r *Raft) changeConfig(ctx context.Context, edit func(Configuration) (Configuration, error)) error {
	_, err := r.propose(ctx, EntryConfig, func() ([]byte, error) {
		if r.confIndex() > r.commitIndex || r.commitIndex < r.termStart {
			return nil, ErrConfigPending
		}
		next, err := edit(r.conf().clone())
		if err != nil {
			return nil, err
		}
		if err := next.Validate(); err != nil {
			return nil, fmt.Errorf("raft: %w", err)
		}
		return next.encode(), nil
	})
	return err
}
