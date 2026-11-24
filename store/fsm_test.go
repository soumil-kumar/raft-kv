package store

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/soumil-kumar/raft-kv/raft"
)

type machine struct {
	*fsm
	index uint64
}

func newMachine() *machine { return &machine{fsm: newFSM()} }

func (m *machine) do(c command) outcome {
	m.index++
	return m.Apply(raft.LogEntry{Index: m.index, Data: c.encode()}).(outcome)
}

func u64(v uint64) *uint64 { return &v }

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []command{
		{op: opSet, key: "k", value: "v"},
		{op: opDelete, key: "a/b c", clientID: "cli", seq: 1 << 40},
		{op: opSet, key: "\xff\x00", value: "not utf8 \xfe", ifVersion: u64(0)},
		{op: opSet, ifVersion: u64(99)},
	} {
		got, err := decodeCommand(c.encode())
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("got %+v, %v; want %+v", got, err, c)
		}
	}
}

func TestMalformedCommandsAreHarmless(t *testing.T) {
	full := command{op: opSet, key: "key", value: "value", clientID: "c", seq: 5, ifVersion: u64(300)}.encode()
	m := newMachine()
	for i := range full {
		if out := m.Apply(raft.LogEntry{Index: 1, Data: full[:i]}).(outcome); out.err() == nil {
			t.Fatalf("prefix of %d bytes was executed", i)
		}
	}
	if len(m.items) != 0 {
		t.Fatal("malformed command changed state")
	}
}

func TestSetDelete(t *testing.T) {
	m := newMachine()
	if out := m.do(command{op: opSet, key: "a", value: "1"}); out.Version != 1 || out.Existed {
		t.Fatalf("%+v", out)
	}
	if out := m.do(command{op: opSet, key: "a", value: "2"}); out.Version != 2 || !out.Existed {
		t.Fatalf("%+v", out)
	}
	if out := m.do(command{op: opDelete, key: "a"}); out.Version != 0 || !out.Existed {
		t.Fatalf("%+v", out)
	}
	if out := m.do(command{op: opDelete, key: "a"}); out.Existed || out.err() != nil {
		t.Fatalf("%+v", out)
	}
}

func TestCompareAndSet(t *testing.T) {
	m := newMachine()
	if out := m.do(command{op: opSet, key: "lock", value: "me", ifVersion: u64(0)}); out.err() != nil {
		t.Fatal(out.err())
	}
	out := m.do(command{op: opSet, key: "lock", value: "you", ifVersion: u64(0)})
	if !errors.Is(out.err(), ErrVersionMismatch) || out.Version != 1 {
		t.Fatalf("%+v", out)
	}
	if m.items["lock"].Value != "me" {
		t.Fatal("failed compare-and-set changed the value")
	}
	if out := m.do(command{op: opDelete, key: "lock", ifVersion: u64(1)}); out.err() != nil {
		t.Fatal(out.err())
	}
}

func TestRetriesExecuteOnce(t *testing.T) {
	m := newMachine()
	c := command{op: opSet, key: "a", value: "1", clientID: "c", seq: 1}
	first := m.do(c)
	m.do(command{op: opSet, key: "a", value: "other client"})
	if again := m.do(c); again != first {
		t.Fatalf("retry returned %+v, original %+v", again, first)
	}
	if m.items["a"].Value != "other client" {
		t.Fatal("retry was executed a second time")
	}
	// A failed conditional write must keep failing when retried.
	bad := command{op: opSet, key: "a", value: "x", clientID: "c", seq: 2, ifVersion: u64(77)}
	m.do(bad)
	if out := m.do(bad); !errors.Is(out.err(), ErrVersionMismatch) {
		t.Fatalf("%+v", out)
	}
}

// Regression: an out-of-order request was dropped but reported as successful.
func TestStaleSequenceIsReported(t *testing.T) {
	m := newMachine()
	m.do(command{op: opSet, key: "k5", value: "v", clientID: "c", seq: 5})
	out := m.do(command{op: opSet, key: "k4", value: "v", clientID: "c", seq: 4})
	if !errors.Is(out.err(), ErrStaleSequence) {
		t.Fatalf("dropped write reported %v", out.err())
	}
}

func TestSessionEvictionIsDeterministic(t *testing.T) {
	a, b := newMachine(), newMachine()
	a.cap, b.cap = 10, 10
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 500 {
		c := command{op: opSet, key: fmt.Sprint(i), clientID: fmt.Sprint("c", rng.IntN(30)), seq: uint64(i + 1)}
		if x, y := a.do(c), b.do(c); x != y {
			t.Fatalf("step %d: %+v vs %+v", i, x, y)
		}
		if i == 250 { // a replica restored from a snapshot must evict the same way
			data, _ := b.Snapshot()()
			fresh := newMachine()
			fresh.cap, fresh.index = 10, b.index
			if err := fresh.Restore(data); err != nil {
				t.Fatal(err)
			}
			b = fresh
		}
	}
	if len(a.sessions) != 10 || a.lru.Len() != 10 {
		t.Fatalf("%d sessions kept", len(a.sessions))
	}
	for k := range a.sessions {
		if b.sessions[k] == nil {
			t.Fatalf("replicas kept different sessions")
		}
	}
}

func TestSnapshotRoundTripAndIsolation(t *testing.T) {
	m := newMachine()
	for i := range 1000 {
		m.do(command{op: opSet, key: fmt.Sprint(i), value: "\xff" + fmt.Sprint(i), clientID: "c", seq: uint64(i + 1)})
	}
	encode := m.Snapshot()
	m.do(command{op: opSet, key: "later", value: "x"}) // must not leak in
	data, err := encode()
	if err != nil {
		t.Fatal(err)
	}
	n := newMachine()
	if err := n.Restore(data); err != nil {
		t.Fatal(err)
	}
	if len(n.items) != 1000 || n.items["7"].Value != "\xff7" {
		t.Fatalf("restored %d items", len(n.items))
	}
	n.index = m.index
	if out := n.do(command{op: opSet, key: "9", value: "dup", clientID: "c", seq: 1000}); out.Version != 1000 {
		t.Fatalf("session lost across snapshot: %+v", out)
	}
	for i := 0; i < len(data); i += 97 {
		if err := newMachine().Restore(data[:i]); err == nil {
			t.Fatalf("truncated snapshot of %d bytes accepted", i)
		}
	}
}

func FuzzDecodeCommand(f *testing.F) {
	f.Add(command{op: opSet, key: "k", value: "v", clientID: "c", seq: 3, ifVersion: u64(4)}.encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		newMachine().Apply(raft.LogEntry{Index: 1, Data: b})
		newMachine().Restore(b)
	})
}
