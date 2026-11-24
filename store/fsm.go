package store

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/soumil-kumar/raft-kv/raft"
)

const (
	opSet    = 1
	opDelete = 2

	flagConditional = 1

	// maxSessions bounds the deduplication table. The least recently used client
	// is forgotten first; its retries are then no longer recognized.
	maxSessions = 100_000
)

type command struct {
	op        byte
	key       string
	value     string
	clientID  string
	seq       uint64
	ifVersion *uint64
}

func newCommand(op byte, key, value string, o WriteOptions) command {
	return command{op: op, key: key, value: value, clientID: o.ClientID, seq: o.Seq, ifVersion: o.IfVersion}
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func (c command) encode() []byte {
	b := make([]byte, 0, 24+len(c.key)+len(c.value)+len(c.clientID))
	var flags byte
	if c.ifVersion != nil {
		flags |= flagConditional
	}
	b = append(b, c.op, flags)
	b = appendString(b, c.key)
	b = appendString(b, c.value)
	b = appendString(b, c.clientID)
	b = binary.AppendUvarint(b, c.seq)
	if c.ifVersion != nil {
		b = binary.AppendUvarint(b, *c.ifVersion)
	}
	return b
}

var errBadEncoding = errors.New("store: malformed encoding")

// reader decodes; after the first failure every method returns zero values.
type reader struct {
	b   []byte
	err error
}

func (r *reader) byte() byte {
	if len(r.b) == 0 {
		r.err = errBadEncoding
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err, r.b = errBadEncoding, nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) string() string {
	n := r.uvarint()
	if n > uint64(len(r.b)) {
		r.err, r.b = errBadEncoding, nil
		return ""
	}
	v := string(r.b[:n])
	r.b = r.b[n:]
	return v
}

func decodeCommand(b []byte) (command, error) {
	r := &reader{b: b}
	c := command{op: r.byte()}
	flags := r.byte()
	c.key, c.value, c.clientID, c.seq = r.string(), r.string(), r.string(), r.uvarint()
	if flags&flagConditional != 0 {
		v := r.uvarint()
		c.ifVersion = &v
	}
	return c, r.err
}

// outcome is the deterministic result of a command; it is remembered per client
// so a retry gets the original answer.
type outcome struct {
	Result
	code byte
}

const (
	codeOK byte = iota
	codeVersionMismatch
	codeStaleSequence
	codeBadCommand
)

func (o outcome) err() error {
	switch o.code {
	case codeOK:
		return nil
	case codeVersionMismatch:
		return ErrVersionMismatch
	case codeStaleSequence:
		return ErrStaleSequence
	}
	return errBadEncoding
}

type session struct {
	client string
	seq    uint64
	last   outcome
}

// fsm is the deterministic state machine. Apply runs on one goroutine; mu only
// protects readers.
type fsm struct {
	mu    sync.RWMutex
	items map[string]Item

	// Sessions are kept in least-recently-applied order, which is the same on
	// every replica, so eviction is deterministic too.
	sessions map[string]*list.Element
	lru      *list.List // of *session; front is oldest
	cap      int
}

func newFSM() *fsm {
	return &fsm{items: map[string]Item{}, sessions: map[string]*list.Element{}, lru: list.New(), cap: maxSessions}
}

// Apply implements raft.StateMachine.
func (f *fsm) Apply(e raft.LogEntry) any {
	c, err := decodeCommand(e.Data)
	if err != nil {
		return outcome{code: codeBadCommand}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.clientID == "" {
		return f.execute(c, e.Index)
	}
	if el := f.sessions[c.clientID]; el != nil {
		s := el.Value.(*session)
		switch {
		case c.seq == s.seq:
			f.lru.MoveToBack(el)
			return s.last
		case c.seq < s.seq:
			return outcome{code: codeStaleSequence}
		}
		f.lru.MoveToBack(el)
		s.seq, s.last = c.seq, f.execute(c, e.Index)
		return s.last
	}
	s := &session{client: c.clientID, seq: c.seq, last: f.execute(c, e.Index)}
	f.sessions[c.clientID] = f.lru.PushBack(s)
	if f.lru.Len() > f.cap {
		oldest := f.lru.Remove(f.lru.Front()).(*session)
		delete(f.sessions, oldest.client)
	}
	return s.last
}

func (f *fsm) execute(c command, index uint64) outcome {
	cur, existed := f.items[c.key]
	res := Result{Version: cur.Version, Existed: existed}
	if c.ifVersion != nil && *c.ifVersion != cur.Version {
		return outcome{Result: res, code: codeVersionMismatch}
	}
	switch c.op {
	case opSet:
		f.items[c.key] = Item{Value: c.value, Version: index}
		res.Version = index
	case opDelete:
		delete(f.items, c.key)
		res.Version = 0
	default:
		return outcome{code: codeBadCommand}
	}
	return outcome{Result: res}
}

const snapshotVersion = 1

// Snapshot implements raft.StateMachine. The maps are copied immediately and
// encoded later, so Apply is paused only for the copy.
func (f *fsm) Snapshot() func() ([]byte, error) {
	f.mu.RLock()
	items := maps.Clone(f.items)
	sessions := make([]session, 0, f.lru.Len())
	for el := f.lru.Front(); el != nil; el = el.Next() {
		sessions = append(sessions, *el.Value.(*session))
	}
	f.mu.RUnlock()
	return func() ([]byte, error) {
		b := []byte{snapshotVersion}
		b = binary.AppendUvarint(b, uint64(len(items)))
		for k, it := range items {
			b = appendString(b, k)
			b = appendString(b, it.Value)
			b = binary.AppendUvarint(b, it.Version)
		}
		b = binary.AppendUvarint(b, uint64(len(sessions)))
		for _, s := range sessions {
			b = appendString(b, s.client)
			b = binary.AppendUvarint(b, s.seq)
			b = append(b, s.last.code)
			b = binary.AppendUvarint(b, s.last.Version)
			var existed byte
			if s.last.Existed {
				existed = 1
			}
			b = append(b, existed)
		}
		return b, nil
	}
}

// Restore implements raft.StateMachine.
func (f *fsm) Restore(data []byte) error {
	r := &reader{b: data}
	if v := r.byte(); r.err == nil && v != snapshotVersion {
		return fmt.Errorf("store: unknown snapshot version %d", v)
	}
	items := map[string]Item{}
	for n := r.uvarint(); n > 0 && r.err == nil; n-- {
		k, v := r.string(), r.string()
		items[k] = Item{Value: v, Version: r.uvarint()}
	}
	sessions, lru := map[string]*list.Element{}, list.New()
	for n := r.uvarint(); n > 0 && r.err == nil; n-- {
		s := &session{client: r.string(), seq: r.uvarint()}
		s.last.code = r.byte()
		s.last.Version = r.uvarint()
		s.last.Existed = r.byte() == 1
		sessions[s.client] = lru.PushBack(s)
	}
	if r.err != nil {
		return r.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items, f.sessions, f.lru = items, sessions, lru
	return nil
}
