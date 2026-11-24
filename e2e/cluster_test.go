// Package e2e tests real raft-kv processes over real sockets and disks.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "raft-kv-e2e")
	if err != nil {
		panic(err)
	}
	binary = filepath.Join(dir, "raft-kv")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	args := []string{"build", "-o", binary}
	if os.Getenv("E2E_RACE") != "" {
		args = append(args, "-race")
	}
	if out, err := exec.Command("go", append(args, "..")...).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// proxy fronts one node's Raft port so a test can cut or stall its inbound
// traffic while the process keeps running.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	down  bool // refuse and drop connections
	stall bool // keep connections open but move no bytes
	conns map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() { ln.Close(); p.set(true, false) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.down {
		c.Close()
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *proxy) serve(in net.Conn) {
	out, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		in.Close()
		return
	}
	if !p.track(in) || !p.track(out) {
		in.Close()
		out.Close()
		return
	}
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			for p.stalled() {
				time.Sleep(5 * time.Millisecond)
			}
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		in.Close()
		out.Close()
		p.mu.Lock()
		delete(p.conns, in)
		delete(p.conns, out)
		p.mu.Unlock()
	}
	go pipe(out, in)
	go pipe(in, out)
}

func (p *proxy) stalled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stall && !p.down
}

func (p *proxy) set(down, stall bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down, p.stall = down, stall
	if down {
		for c := range p.conns {
			c.Close()
		}
	}
}

type proc struct {
	id, dir            string
	raftAddr, httpAddr string
	proxy              *proxy
	extra              []string
	cmd                *exec.Cmd
	log                *os.File
}

func (p *proc) base() string { return "http://" + p.httpAddr }

type cluster struct {
	t     *testing.T
	procs []*proc
	mu    sync.Mutex
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func (c *cluster) newProc(id string) *proc {
	p := &proc{id: id, dir: c.t.TempDir(), raftAddr: freeAddr(c.t), httpAddr: freeAddr(c.t)}
	p.proxy = newProxy(c.t, p.raftAddr)
	c.procs = append(c.procs, p)
	return p
}

func newCluster(t *testing.T, n int, extra ...string) *cluster {
	c := &cluster{t: t}
	var peers, web []string
	for i := range n {
		p := c.newProc(fmt.Sprint("n", i+1))
		peers = append(peers, p.id+"="+p.proxy.ln.Addr().String())
		web = append(web, p.id+"="+p.httpAddr)
	}
	for _, p := range c.procs {
		p.extra = append([]string{"--peers", strings.Join(peers, ","), "--peer-http", strings.Join(web, ",")}, extra...)
		c.start(p)
	}
	t.Cleanup(func() {
		for _, p := range c.procs {
			c.kill(p)
		}
		if t.Failed() {
			for _, p := range c.procs {
				b, _ := os.ReadFile(filepath.Join(p.dir, "log.txt"))
				if len(b) > 3000 {
					b = b[len(b)-3000:]
				}
				t.Logf("--- %s\n%s", p.id, b)
			}
		}
		for _, p := range c.procs {
			b, _ := os.ReadFile(filepath.Join(p.dir, "log.txt"))
			for _, bad := range []string{"panic:", "fatal:", "DATA RACE", "level=ERROR"} {
				if bytes.Contains(b, []byte(bad)) {
					t.Errorf("%s logged %q", p.id, bad)
				}
			}
		}
	})
	return c
}

func (c *cluster) start(p *proc) {
	c.t.Helper()
	f, err := os.OpenFile(filepath.Join(p.dir, "log.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	args := append([]string{"--node-id", p.id, "--raft-addr", p.raftAddr, "--http-addr", p.httpAddr,
		"--data-dir", filepath.Join(p.dir, "data")}, p.extra...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	p.cmd, p.log = cmd, f
}

// kill is kill -9: no shutdown code runs.
func (c *cluster) kill(p *proc) {
	if p.cmd != nil {
		p.cmd.Process.Kill()
		p.cmd.Wait()
		p.log.Close()
		p.cmd = nil
	}
}

var client = &http.Client{
	Timeout:   6 * time.Second,
	Transport: &http.Transport{MaxIdleConnsPerHost: 64, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
}

type response struct {
	Value    string `json:"value"`
	Version  uint64 `json:"version"`
	Code     string `json:"code"`
	Error    string `json:"error"`
	IsLeader bool   `json:"is_leader"`
	Term     uint64 `json:"term"`
	Keys     int    `json:"keys"`
	Snapshot uint64 `json:"snapshot_index"`
	Install  uint64 `json:"snapshots_installed"`
	Members  []struct {
		ID      string `json:"id"`
		Learner bool   `json:"learner"`
	} `json:"members"`
}

// do returns status 0 if no response arrived.
func do(method, url, body string, hdr ...string) (int, response) {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return 0, response{Error: err.Error()}
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, response{Error: err.Error()}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, response{Error: err.Error()}
	}
	var out response
	json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func eventually(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (c *cluster) leader() *proc {
	c.t.Helper()
	var found *proc
	eventually(c.t, 20*time.Second, "a leader", func() bool {
		for _, p := range c.procs {
			if p.cmd == nil {
				continue
			}
			if code, _ := do("GET", p.base()+"/readyz", ""); code != 200 {
				continue
			}
			if _, st := do("GET", p.base()+"/status", ""); st.IsLeader {
				found = p
				return true
			}
		}
		return false
	})
	return found
}

func (c *cluster) put(key, value string) {
	c.t.Helper()
	eventually(c.t, 20*time.Second, "PUT "+key, func() bool {
		for _, p := range c.procs {
			if p.cmd != nil {
				if code, _ := do("PUT", p.base()+"/store/"+key, fmt.Sprintf(`{"value":%q}`, value)); code == 200 {
					return true
				}
			}
		}
		return false
	})
}

// load writes n keys through url with the given concurrency.
func (c *cluster) load(url, prefix string, n, workers int) {
	c.t.Helper()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < n; i += workers {
				for try := 0; ; try++ {
					if code, out := do("PUT", fmt.Sprintf("%s/store/%s%d", url, prefix, i), fmt.Sprintf(`{"value":"v%d"}`, i)); code == 200 {
						break
					} else if try > 50 {
						c.t.Errorf("write %d: %d %s", i, code, out.Error)
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
}

func (c *cluster) others(p *proc) []*proc {
	var out []*proc
	for _, q := range c.procs {
		if q != p {
			out = append(out, q)
		}
	}
	return out
}

// verify checks that every live node serves all n keys, linearizably.
func (c *cluster) verify(prefix string, n int) {
	c.t.Helper()
	for _, p := range c.procs {
		if p.cmd == nil {
			continue
		}
		eventually(c.t, 20*time.Second, p.id+" ready", func() bool { code, _ := do("GET", p.base()+"/readyz", ""); return code == 200 })
		for i := range n {
			code, out := do("GET", fmt.Sprintf("%s/store/%s%d", p.base(), prefix, i), "")
			if code != 200 || out.Value != fmt.Sprint("v", i) {
				c.t.Fatalf("%s: key %s%d: %d %+v", p.id, prefix, i, code, out)
			}
		}
	}
}

// --- scenarios ---

// Regression: the original cluster never elected a leader again after its first
// snapshot, not even after a full restart.
func TestFailoverAndRestartAfterSnapshots(t *testing.T) {
	c := newCluster(t, 3, "--snapshot-threshold", "300")
	c.load(c.leader().base(), "k", 1500, 16)
	for _, p := range c.procs {
		eventually(t, 10*time.Second, p.id+" snapshot", func() bool { _, st := do("GET", p.base()+"/status", ""); return st.Snapshot > 0 })
	}
	l := c.leader()
	c.kill(l)
	c.put("after-failover", "x")
	c.start(l)
	for _, p := range c.procs {
		c.kill(p)
	}
	for _, p := range c.procs {
		c.start(p)
	}
	c.put("after-restart", "x")
	c.verify("k", 1500)
}

// Regression: a rejoining follower got only the first 32 KiB of a snapshot.
func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	c := newCluster(t, 3, "--snapshot-threshold", "300")
	l := c.leader()
	f := c.others(l)[0]
	c.kill(f)
	// Outrun the in-memory trailing log so that only a snapshot can help.
	c.load(l.base(), "k", 6000, 32)
	c.start(f)
	c.verify("k", 6000)
	if _, st := do("GET", f.base()+"/status", ""); st.Install == 0 {
		t.Fatal("follower did not need a snapshot; test is not exercising it")
	}
}

// Regression: appends after a torn record were unreadable at the next restart.
func TestTornWriteAheadLogs(t *testing.T) {
	c := newCluster(t, 3)
	c.load(c.leader().base(), "a", 50, 4)
	for _, p := range c.procs {
		c.kill(p)
	}
	for i, p := range c.procs { // junk after the last acknowledged byte
		wals, _ := filepath.Glob(filepath.Join(p.dir, "data", "wal-*.log"))
		if len(wals) != 1 {
			t.Fatalf("%v", wals)
		}
		b, _ := os.ReadFile(wals[0])
		f, _ := os.OpenFile(wals[0], os.O_APPEND|os.O_WRONLY, 0o644)
		f.Write(b[len(b)-20 : len(b)-20+3+i*3]) // shorter than a frame header
		f.Close()
	}
	for _, p := range c.procs {
		c.start(p)
	}
	c.load(c.leader().base(), "b", 50, 4)
	for _, p := range c.procs {
		c.kill(p)
	}
	for _, p := range c.procs {
		c.start(p)
	}
	c.verify("a", 50)
	c.verify("b", 50)
}

func TestRollingRestartsUnderLoad(t *testing.T) {
	c := newCluster(t, 3, "--snapshot-threshold", "500")
	c.leader()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var acked []int
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; ; i += 4 {
				select {
				case <-stop:
					return
				default:
				}
				p := c.procs[i%3]
				if code, _ := do("PUT", fmt.Sprintf("%s/store/r%d", p.base(), i), fmt.Sprintf(`{"value":"v%d"}`, i)); code == 200 {
					mu.Lock()
					acked = append(acked, i)
					mu.Unlock()
				}
			}
		}()
	}
	for range 2 {
		for _, p := range c.procs {
			c.mu.Lock()
			c.kill(p)
			time.Sleep(300 * time.Millisecond)
			c.start(p)
			c.mu.Unlock()
			eventually(t, 15*time.Second, p.id+" ready", func() bool { code, _ := do("GET", p.base()+"/readyz", ""); return code == 200 })
		}
	}
	close(stop)
	wg.Wait()
	l := c.leader()
	for _, i := range acked {
		if code, out := do("GET", fmt.Sprintf("%s/store/r%d", l.base(), i), ""); code != 200 || out.Value != fmt.Sprint("v", i) {
			t.Fatalf("acknowledged write %d: %d %+v", i, code, out)
		}
	}
	t.Logf("%d acknowledged writes survived 6 kill -9 restarts", len(acked))
	if len(acked) < 20 { // enough to mean something, low enough for a slow machine
		t.Fatalf("only %d writes succeeded", len(acked))
	}
}

func TestAddAndRemoveNodes(t *testing.T) {
	c := newCluster(t, 3, "--snapshot-threshold", "300")
	c.load(c.leader().base(), "k", 1000, 16)
	n4 := c.newProc("n4")
	n4.extra = []string{"--join", "--snapshot-threshold", "300"}
	c.start(n4)
	body := fmt.Sprintf(`{"id":"n4","raft_addr":%q,"http_addr":%q}`, n4.proxy.ln.Addr(), n4.httpAddr)
	if code, out := do("POST", c.others(c.leader())[0].base()+"/cluster/members", body); code != 200 {
		t.Fatalf("add: %d %+v", code, out)
	}
	eventually(t, 20*time.Second, "promotion", func() bool {
		code, _ := do("POST", c.leader().base()+"/cluster/members/n4/promote", "")
		return code == 200
	})
	c.verify("k", 1000)

	old := c.leader()
	if code, out := do("DELETE", old.base()+"/cluster/members/"+old.id, ""); code != 200 {
		t.Fatalf("remove leader: %d %+v", code, out)
	}
	c.kill(old)
	var rest []*proc
	for _, p := range c.procs {
		if p != old {
			rest = append(rest, p)
		}
	}
	c.procs = rest
	c.put("after-removal", "x")
	// Three voters remain, so one more failure is fine.
	c.kill(c.leader())
	c.put("one-down", "x")
	for _, p := range c.procs {
		c.kill(p)
	}
	for _, p := range c.procs {
		c.start(p)
	}
	c.verify("k", 1000)
	if _, st := do("GET", c.leader().base()+"/status", ""); len(st.Members) != 3 {
		t.Fatalf("members after restart: %+v", st.Members)
	}
}

func TestStalledFollowerDoesNotHurtLatency(t *testing.T) {
	c := newCluster(t, 3)
	l := c.leader()
	c.put("k", "v")
	c.others(l)[0].proxy.set(false, true)
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	for range 20 {
		if code, out := do("GET", l.base()+"/store/k", ""); code != 200 {
			t.Fatalf("read: %d %+v", code, out)
		}
		if code, out := do("PUT", l.base()+"/store/k", `{"value":"v"}`); code != 200 {
			t.Fatalf("write: %d %+v", code, out)
		}
	}
	if avg := time.Since(start) / 40; avg > 100*time.Millisecond {
		t.Fatalf("average operation took %v with one stalled follower", avg)
	}
}
