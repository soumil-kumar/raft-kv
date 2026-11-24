package e2e

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type opKind int

const (
	opGet opKind = iota
	opPut
	opDelete
	opCAS
)

type kvInput struct {
	kind   opKind
	key    string
	value  string // written value; unique per operation
	expect string // CAS: value the client had read at the version it named
}

type kvOutput struct {
	value   string // Get; "" when absent
	ok      bool   // CAS
	unknown bool   // no definite answer ever arrived
}

// model is a register per key supporting get, put, delete and compare-and-set.
// Values are unique, so a version names exactly one value.
var model = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range h {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in, out := state.(string), input.(kvInput), output.(kvOutput)
		switch in.kind {
		case opGet:
			return out.unknown || out.value == s, s
		case opPut:
			return true, in.value
		case opDelete:
			return true, ""
		default:
			match := s == in.expect
			if !out.unknown && out.ok != match {
				return false, s
			}
			if match {
				return true, in.value
			}
			return true, s
		}
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		return fmt.Sprintf("%s(%s %q->%q) = %+v", [...]string{"get", "put", "del", "cas"}[in.kind], in.key, in.expect, in.value, out)
	},
}

type history struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
}

func (h *history) now() int64 { return time.Since(h.start).Nanoseconds() }

func (h *history) add(client int, in kvInput, call int64, out kvOutput) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: h.now()})
}

// seconds is how long the chaos test runs; raise it with E2E_SECONDS.
func seconds() time.Duration {
	if n, err := strconv.Atoi(os.Getenv("E2E_SECONDS")); err == nil {
		return time.Duration(n) * time.Second
	}
	return 20 * time.Second
}

// TestLinearizability runs clients against a cluster whose processes are being
// killed, cut off and stalled, and checks the whole history with Porcupine.
// Clients retry writes with the same sequence number until they get a definite
// answer, so an operation that executed twice is also a violation.
func TestLinearizability(t *testing.T) {
	c := newCluster(t, 3, "--snapshot-threshold", "400")
	c.leader()
	keys := []string{"x", "y", "z", "w", "v"}
	h := &history{start: time.Now()}
	stop := make(chan struct{})
	done := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	var wg sync.WaitGroup
	for cl := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(cl), uint64(time.Now().UnixNano())))
			id := fmt.Sprintf("client-%d", cl)
			seq := 0
			seen := map[string]response{} // last value and version read, per key
			pick := func() string { return c.procs[rng.IntN(len(c.procs))].base() }

			for !done() {
				key := keys[rng.IntN(len(keys))]
				in := kvInput{key: key}
				call := h.now()
				if rng.IntN(2) == 0 {
					code, out := do("GET", pick()+"/store/"+key, "")
					switch code {
					case 200:
						seen[key] = out
						h.add(cl, in, call, kvOutput{value: out.Value})
					case 404:
						seen[key] = response{}
						h.add(cl, in, call, kvOutput{})
					}
					continue // a read without an answer constrains nothing
				}

				seq++
				in.value = fmt.Sprintf("%d-%d", cl, seq)
				method, url, body := "PUT", "/store/"+key, fmt.Sprintf(`{"value":%q}`, in.value)
				switch rng.IntN(6) {
				case 0:
					method, body, in.kind = "DELETE", "", opDelete
				case 1, 2:
					last, ok := seen[key]
					if !ok {
						continue
					}
					in.kind, in.expect = opCAS, last.Value
					url += fmt.Sprintf("?if_version=%d", last.Version)
				default:
					in.kind = opPut
				}
				out := kvOutput{unknown: true}
				for !done() {
					code, resp := do(method, pick()+url, body, "X-Client-Id", id, "X-Sequence-Num", fmt.Sprint(seq))
					if code == 200 || code == 412 {
						out = kvOutput{ok: code == 200}
						break
					}
					if code != 0 && code != 503 && code != 504 {
						t.Errorf("%s %s: unexpected %d %+v", method, url, code, resp)
						break
					}
					time.Sleep(time.Duration(5+rng.IntN(20)) * time.Millisecond)
				}
				h.add(cl, in, call, out)
			}
		}()
	}

	faults := map[string]int{}
	for end := time.Now().Add(seconds()); time.Now().Before(end); {
		time.Sleep(time.Duration(200+rand.IntN(600)) * time.Millisecond)
		p := c.procs[rand.IntN(len(c.procs))]
		switch rand.IntN(7) {
		case 0, 1:
			faults["kill"]++
			c.kill(p)
			time.Sleep(time.Duration(rand.IntN(800)) * time.Millisecond)
			c.start(p)
		case 2:
			faults["kill all"]++
			for _, q := range c.procs {
				c.kill(q)
			}
			for _, q := range c.procs {
				c.start(q)
			}
		case 3:
			faults["cut"]++
			p.proxy.set(true, false)
		case 4:
			faults["stall"]++
			p.proxy.set(false, true)
		default:
			for _, q := range c.procs {
				q.proxy.set(false, false)
			}
		}
	}
	for _, q := range c.procs {
		q.proxy.set(false, false)
	}
	close(stop)
	wg.Wait()

	// Final reads pin down what the cluster ended up with, on every node.
	for _, p := range c.procs {
		eventually(t, 30*time.Second, p.id+" to recover", func() bool { code, _ := do("GET", p.base()+"/readyz", ""); return code == 200 })
		for _, k := range keys {
			call := h.now()
			code, out := do("GET", p.base()+"/store/"+k, "")
			if code != 200 && code != 404 {
				t.Fatalf("final read: %d %+v", code, out)
			}
			h.add(100, kvInput{key: k}, call, kvOutput{value: out.Value})
		}
	}
	// An operation with no definite answer may take effect at any later time.
	unknown, kinds := 0, map[opKind]int{}
	for i := range h.ops {
		kinds[h.ops[i].Input.(kvInput).kind]++
		if h.ops[i].Output.(kvOutput).unknown {
			unknown++
			h.ops[i].Return = h.now() + int64(time.Hour)
		}
	}
	res, info := porcupine.CheckOperationsVerbose(model, h.ops, 2*time.Minute)
	t.Logf("%d operations (%d get, %d put, %d delete, %d cas; %d unresolved), faults %v: %s",
		len(h.ops), kinds[opGet], kinds[opPut], kinds[opDelete], kinds[opCAS], unknown, faults, res)
	if res != porcupine.Ok {
		path := filepath.Join(os.TempDir(), "raft-kv-linearizability.html")
		porcupine.VisualizePath(model, info, path)
		t.Fatalf("history is not linearizable (%s); see %s", res, path)
	}
	if len(h.ops) < 200 {
		t.Fatalf("only %d operations completed; the test proved little", len(h.ops))
	}
}

// The checker must be able to fail, or a pass means nothing.
func TestCheckerRejectsBadHistories(t *testing.T) {
	op := func(cl int, in kvInput, call, ret int64, out kvOutput) porcupine.Operation {
		return porcupine.Operation{ClientId: cl, Input: in, Call: call, Output: out, Return: ret}
	}
	put := func(v string) kvInput { return kvInput{kind: opPut, key: "k", value: v} }
	get := kvInput{key: "k"}
	cas := func(e, v string) kvInput { return kvInput{kind: opCAS, key: "k", expect: e, value: v} }
	for _, tc := range []struct {
		name string
		ops  []porcupine.Operation
		ok   bool
	}{
		{"fresh read", []porcupine.Operation{op(1, put("a"), 0, 1, kvOutput{}), op(2, get, 2, 3, kvOutput{value: "a"})}, true},
		{"lost write", []porcupine.Operation{op(1, put("a"), 0, 1, kvOutput{}), op(2, get, 2, 3, kvOutput{})}, false},
		{"stale read", []porcupine.Operation{op(1, put("a"), 0, 1, kvOutput{}), op(1, put("b"), 2, 3, kvOutput{}), op(2, get, 4, 5, kvOutput{value: "a"})}, false},
		{"write executed twice", []porcupine.Operation{op(1, put("a"), 0, 1, kvOutput{}), op(2, put("b"), 2, 3, kvOutput{}),
			op(3, get, 4, 5, kvOutput{value: "b"}), op(3, get, 6, 7, kvOutput{value: "a"})}, false},
		{"two CAS winners", []porcupine.Operation{op(1, put("a"), 0, 1, kvOutput{}), op(1, cas("a", "b"), 2, 5, kvOutput{ok: true}), op(2, cas("a", "c"), 2, 5, kvOutput{ok: true})}, false},
		{"unknown write seen late", []porcupine.Operation{op(1, put("a"), 0, 1<<40, kvOutput{unknown: true}), op(2, get, 2, 3, kvOutput{}), op(2, get, 4, 5, kvOutput{value: "a"})}, true},
		{"flip-flop", []porcupine.Operation{op(1, put("a"), 0, 1<<40, kvOutput{unknown: true}), op(2, get, 2, 3, kvOutput{value: "a"}), op(2, get, 4, 5, kvOutput{})}, false},
	} {
		if got := porcupine.CheckOperations(model, tc.ops); got != tc.ok {
			t.Errorf("%s: linearizable=%v, want %v", tc.name, got, tc.ok)
		}
	}
}
