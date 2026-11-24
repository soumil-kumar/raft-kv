package httpd

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/soumil-kumar/raft-kv/raft"
	"github.com/soumil-kumar/raft-kv/store"
)

type testNode struct {
	id    string
	base  string
	store *store.Store
	close func()
}

// startCluster runs real nodes over loopback TCP and HTTP.
func startCluster(t *testing.T, n int, maxBody int64) []*testNode {
	t.Helper()
	var conf raft.Configuration
	var raftLns, httpLns []net.Listener
	listen := func() net.Listener {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return ln
	}
	for i := range n {
		raftLns, httpLns = append(raftLns, listen()), append(httpLns, listen())
		conf.Members = append(conf.Members, raft.Member{
			ID: fmt.Sprint("n", i+1), RaftAddr: raftLns[i].Addr().String(), HTTPAddr: httpLns[i].Addr().String(),
		})
	}
	var nodes []*testNode
	for i, m := range conf.Members {
		fs, err := raft.OpenFileStorage(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		tr := raft.NewTCPTransport(200 * time.Millisecond)
		st, err := store.Open(raft.Config{ID: m.ID, Bootstrap: conf, ElectionTimeout: 300 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond}, fs, tr)
		if err != nil {
			t.Fatal(err)
		}
		srv, err := raft.Serve(raftLns[i], st.Raft())
		if err != nil {
			t.Fatal(err)
		}
		api := New(st, Options{Storage: fs, Timeout: 2 * time.Second, MaxBodyBytes: maxBody})
		go api.Serve(httpLns[i])
		closed := false
		node := &testNode{id: m.ID, base: "http://" + m.HTTPAddr, store: st, close: func() {
			if !closed {
				closed = true
				api.server.Close()
				srv.Close()
				st.Close()
				tr.Close()
			}
		}}
		t.Cleanup(node.close)
		nodes = append(nodes, node)
	}
	if n > 0 {
		waitReady(t, nodes[0])
	}
	return nodes
}

func waitReady(t *testing.T, n *testNode) {
	t.Helper()
	for range 100 {
		if code, _ := call(t, "GET", n.base+"/readyz", "", nil); code == 200 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cluster never became ready")
}

func call(t *testing.T, method, url, body string, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, map[string]any{"error": err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func leaderAndFollower(t *testing.T, nodes []*testNode) (l, f *testNode) {
	t.Helper()
	for range 100 {
		for i, n := range nodes {
			if n.store.Raft().IsLeader() {
				return n, nodes[(i+1)%len(nodes)]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil, nil
}

func TestEveryNodeServesReadsAndWrites(t *testing.T) {
	nodes := startCluster(t, 3, 0)
	for i, n := range nodes {
		val := fmt.Sprint("v", i)
		if code, out := call(t, "PUT", n.base+"/store/k", `{"value":"`+val+`"}`, nil); code != 200 {
			t.Fatalf("PUT via %s: %d %v", n.id, code, out)
		}
		for _, m := range nodes { // linearizable from everyone, immediately
			if code, out := call(t, "GET", m.base+"/store/k", "", nil); code != 200 || out["value"] != val {
				t.Fatalf("GET via %s: %d %v", m.id, code, out)
			}
		}
	}
	for _, n := range nodes {
		call(t, "PUT", n.base+"/store/gone", `{"value":"x"}`, nil)
		if code, out := call(t, "DELETE", n.base+"/store/gone", "", nil); code != 200 || out["existed"] != true {
			t.Fatalf("DELETE via %s: %d %v", n.id, code, out)
		}
		if code, _ := call(t, "GET", n.base+"/store/gone", "", nil); code != 404 {
			t.Fatalf("deleted key: %d", code)
		}
	}
}

func TestKeysAndValuesSurviveEncoding(t *testing.T) {
	nodes := startCluster(t, 1, 0)
	for _, key := range []string{"a/b/c", "sp%20ace", "q%3Fmark", "%E2%9C%93", "a%2Fb"} {
		if code, out := call(t, "PUT", nodes[0].base+"/store/"+key, `{"value":""}`, nil); code != 200 {
			t.Fatalf("%s: %d %v", key, code, out)
		}
		if code, out := call(t, "GET", nodes[0].base+"/store/"+key, "", nil); code != 200 || out["value"] != "" {
			t.Fatalf("%s: %d %v", key, code, out)
		}
	}
}

func TestRequestValidation(t *testing.T) {
	nodes := startCluster(t, 1, 100)
	base := nodes[0].base
	cases := []struct {
		name, method, path, body string
		hdr                      map[string]string
		want                     int
	}{
		{"empty key", "PUT", "/store/", `{"value":"x"}`, nil, 400},
		{"huge key", "GET", "/store/" + strings.Repeat("k", 5000), "", nil, 400},
		{"not json", "PUT", "/store/k", "hello", nil, 400},
		{"misspelled field", "PUT", "/store/k", `{"valu":"x"}`, nil, 400},
		{"missing value", "PUT", "/store/k", `{}`, nil, 400},
		{"wrong type", "PUT", "/store/k", `{"value":5}`, nil, 400},
		{"trailing data", "PUT", "/store/k", `{"value":"x"} {}`, nil, 400},
		{"too large", "PUT", "/store/k", `{"value":"` + strings.Repeat("x", 200) + `"}`, nil, 413},
		{"bad method", "POST", "/store/k", `{"value":"x"}`, nil, 405},
		{"seq without client", "PUT", "/store/k", `{"value":"x"}`, map[string]string{"X-Sequence-Num": "1"}, 400},
		{"client without seq", "PUT", "/store/k", `{"value":"x"}`, map[string]string{"X-Client-Id": "c"}, 400},
		{"bad seq", "PUT", "/store/k", `{"value":"x"}`, map[string]string{"X-Client-Id": "c", "X-Sequence-Num": "-1"}, 400},
		{"zero seq", "PUT", "/store/k", `{"value":"x"}`, map[string]string{"X-Client-Id": "c", "X-Sequence-Num": "0"}, 400},
		{"bad if_version", "PUT", "/store/k?if_version=x", `{"value":"x"}`, nil, 400},
		{"bad limit", "GET", "/store?limit=0", "", nil, 400},
		{"unknown path", "GET", "/nope", "", nil, 404},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code, out := call(t, tc.method, base+tc.path, tc.body, tc.hdr); code != tc.want {
				t.Fatalf("got %d %v, want %d", code, out, tc.want)
			}
		})
	}
	if code, _ := call(t, "GET", base+"/store/k", "", nil); code != 404 {
		t.Fatal("a rejected request changed state")
	}
}

func TestConditionalWritesAndDeduplication(t *testing.T) {
	nodes := startCluster(t, 3, 0)
	_, f := leaderAndFollower(t, nodes) // everything through forwarding
	code, out := call(t, "PUT", f.base+"/store/lock?if_version=0", `{"value":"me"}`, nil)
	if code != 200 {
		t.Fatalf("%d %v", code, out)
	}
	if code, out := call(t, "PUT", f.base+"/store/lock?if_version=0", `{"value":"you"}`, nil); code != 412 || out["code"] != "version_mismatch" {
		t.Fatalf("%d %v", code, out)
	}
	if code, _ := call(t, "DELETE", fmt.Sprintf("%s/store/lock?if_version=%v", f.base, out["version"]), "", nil); code != 200 {
		t.Fatalf("conditional delete: %d", code)
	}

	h := func(seq string) map[string]string {
		return map[string]string{"X-Client-Id": "cli", "X-Sequence-Num": seq}
	}
	_, first := call(t, "PUT", f.base+"/store/d", `{"value":"1"}`, h("5"))
	call(t, "PUT", f.base+"/store/d", `{"value":"2"}`, nil)
	if code, again := call(t, "PUT", f.base+"/store/d", `{"value":"1"}`, h("5")); code != 200 || again["version"] != first["version"] {
		t.Fatalf("retry: %d %v, original %v", code, again, first)
	}
	if _, out := call(t, "GET", f.base+"/store/d", "", nil); out["value"] != "2" {
		t.Fatalf("retry re-executed: %v", out)
	}
	if code, out := call(t, "PUT", f.base+"/store/e", `{"value":"x"}`, h("4")); code != 409 {
		t.Fatalf("stale sequence: %d %v", code, out)
	}
}

func TestLeaderFailureIsSurvivable(t *testing.T) {
	nodes := startCluster(t, 3, 0)
	l, f := leaderAndFollower(t, nodes)
	call(t, "PUT", l.base+"/store/a", `{"value":"1"}`, nil)
	l.close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, _ := call(t, "PUT", f.base+"/store/b", `{"value":"2"}`, nil)
		if code == 200 {
			break
		}
		if code == 0 || time.Now().After(deadline) {
			t.Fatalf("no recovery, last status %d", code)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, out := call(t, "GET", f.base+"/store/a", "", nil); code != 200 || out["value"] != "1" {
		t.Fatalf("%d %v", code, out)
	}
}

// Regression: a write without quorum used to end in a dropped connection.
func TestNoQuorumGetsAProperError(t *testing.T) {
	nodes := startCluster(t, 3, 0)
	l, _ := leaderAndFollower(t, nodes)
	for _, n := range nodes {
		if n != l {
			n.close()
		}
	}
	code, out := call(t, "PUT", l.base+"/store/q", `{"value":"x"}`, nil)
	if code != 504 && code != 503 {
		t.Fatalf("write: %d %v", code, out)
	}
	if code, out := call(t, "GET", l.base+"/store/q", "", nil); code != 503 {
		t.Fatalf("read: %d %v", code, out)
	}
	if code, _ := call(t, "GET", l.base+"/store/q?stale=true", "", nil); code != 404 {
		t.Fatalf("stale read should still work: %d", code)
	}
	if code, _ := call(t, "GET", l.base+"/readyz", "", nil); code != 503 {
		t.Fatalf("readyz: %d", code)
	}
	if code, _ := call(t, "GET", l.base+"/healthz", "", nil); code != 200 {
		t.Fatalf("healthz: %d", code)
	}
}

func TestForwardLoopIsRefused(t *testing.T) {
	nodes := startCluster(t, 3, 0)
	_, f := leaderAndFollower(t, nodes)
	if code, out := call(t, "PUT", f.base+"/store/k", `{"value":"x"}`, map[string]string{forwardedHeader: "n9"}); code != 503 || out["code"] != "not_leader" {
		t.Fatalf("%d %v", code, out)
	}
}
