// Command raft-bench is a closed-loop load generator for raft-kv.
//
// Each worker holds one keep-alive connection and sends its next request when
// the previous one finishes, so mean latency is about workers/throughput. Only
// successful requests count towards throughput.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	Round    int            `json:"round"`
	Op       string         `json:"op"`
	OK       int            `json:"ok"`
	Errors   int            `json:"errors"`
	PerSec   float64        `json:"per_sec"`
	MeanMs   float64        `json:"mean_ms"`
	P50Ms    float64        `json:"p50_ms"`
	P95Ms    float64        `json:"p95_ms"`
	P99Ms    float64        `json:"p99_ms"`
	MaxMs    float64        `json:"max_ms"`
	ErrKinds map[string]int `json:"error_kinds,omitempty"`
}

type sample struct {
	read bool
	d    time.Duration
	err  string
}

func main() {
	urls := flag.String("urls", "http://127.0.0.1:8001", "comma-separated node URLs; workers are spread across them")
	workers := flag.Int("c", 50, "concurrent workers")
	dur := flag.Duration("d", 10*time.Second, "measured time per round")
	warmup := flag.Duration("warmup", 2*time.Second, "unmeasured time before the first round")
	rounds := flag.Int("rounds", 3, "rounds")
	reads := flag.Float64("reads", 0, "fraction of requests that are reads, 0 to 1")
	stale := flag.Bool("stale", false, "use stale reads instead of linearizable ones")
	keys := flag.Int("keys", 10000, "key space")
	size := flag.Int("size", 100, "value bytes")
	leader := flag.Bool("leader", false, "send everything to the current leader instead of spreading it")
	asJSON := flag.Bool("json", false, "print JSON lines")
	fsyncDir := flag.String("fsync", "", "instead of load, measure sequential write+fsync in this directory")
	flag.Parse()

	if *fsyncDir != "" {
		fsyncBaseline(*fsyncDir, *size, *dur)
		return
	}
	nodes := strings.Split(*urls, ",")
	if *leader {
		nodes = []string{findLeader(nodes)}
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 0, MaxIdleConnsPerHost: *workers},
	}
	value := strings.Repeat("x", *size)
	body, _ := json.Marshal(map[string]string{"value": value})

	run := func(d time.Duration) ([]sample, time.Duration) {
		per := make([][]sample, *workers)
		var wg sync.WaitGroup
		start := time.Now()
		for w := range *workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(w), uint64(start.UnixNano())))
				base := nodes[w%len(nodes)]
				for time.Since(start) < d {
					url := fmt.Sprintf("%s/store/bench-%d", base, rng.IntN(*keys))
					read := rng.Float64() < *reads
					var req *http.Request
					if read {
						if *stale {
							url += "?stale=true"
						}
						req, _ = http.NewRequest("GET", url, nil)
					} else {
						req, _ = http.NewRequest("PUT", url, bytes.NewReader(body))
					}
					t0 := time.Now()
					s := sample{read: read}
					resp, err := client.Do(req)
					if err == nil {
						_, err = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
					s.d = time.Since(t0)
					switch {
					case err != nil:
						s.err = "transport"
					case resp.StatusCode != 200 && !(read && resp.StatusCode == 404):
						s.err = fmt.Sprint(resp.StatusCode)
					}
					per[w] = append(per[w], s)
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		var all []sample
		for _, p := range per {
			all = append(all, p...)
		}
		return all, elapsed
	}

	if *warmup > 0 {
		run(*warmup)
	}
	if !*asJSON {
		fmt.Printf("%-5s %-5s %9s %7s %10s %9s %9s %9s %9s %9s\n", "round", "op", "ok", "errors", "ok/s", "mean ms", "p50 ms", "p95 ms", "p99 ms", "max ms")
	}
	for round := 1; round <= *rounds; round++ {
		all, elapsed := run(*dur)
		for _, op := range []string{"write", "read"} {
			r := summarize(round, op, all, elapsed)
			if r.OK+r.Errors == 0 {
				continue
			}
			if *asJSON {
				json.NewEncoder(os.Stdout).Encode(r)
				continue
			}
			fmt.Printf("%-5d %-5s %9d %7d %10.0f %9.2f %9.2f %9.2f %9.2f %9.2f", round, op, r.OK, r.Errors, r.PerSec, r.MeanMs, r.P50Ms, r.P95Ms, r.P99Ms, r.MaxMs)
			if len(r.ErrKinds) > 0 {
				fmt.Printf("  %v", r.ErrKinds)
			}
			fmt.Println()
		}
	}
}

func summarize(round int, op string, all []sample, elapsed time.Duration) result {
	r := result{Round: round, Op: op, ErrKinds: map[string]int{}}
	var lat []time.Duration
	var sum time.Duration
	for _, s := range all {
		if s.read != (op == "read") {
			continue
		}
		if s.err != "" {
			r.Errors++
			r.ErrKinds[s.err]++
			continue
		}
		lat = append(lat, s.d)
		sum += s.d
	}
	r.OK = len(lat)
	if r.OK == 0 {
		return r
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	pct := func(p float64) float64 { return ms(lat[min(len(lat)-1, int(p*float64(len(lat))))]) }
	r.PerSec = float64(r.OK) / elapsed.Seconds()
	r.MeanMs = ms(sum / time.Duration(r.OK))
	r.P50Ms, r.P95Ms, r.P99Ms, r.MaxMs = pct(.5), pct(.95), pct(.99), ms(lat[len(lat)-1])
	return r
}

func findLeader(nodes []string) string {
	for range 50 {
		for _, n := range nodes {
			resp, err := http.Get(n + "/status")
			if err != nil {
				continue
			}
			var st struct {
				IsLeader bool `json:"is_leader"`
			}
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.IsLeader {
				return n
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "no leader found")
	os.Exit(1)
	return ""
}

// fsyncBaseline measures what the disk allows a single unbatched writer, which
// is the ceiling for an implementation without group commit.
func fsyncBaseline(dir string, size int, d time.Duration) {
	f, err := os.CreateTemp(dir, "fsync-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.Remove(filepath.Join(f.Name()))
	defer f.Close()
	buf := bytes.Repeat([]byte("x"), size)
	n := 0
	start := time.Now()
	for time.Since(start) < d {
		if _, err = f.Write(buf); err == nil {
			err = f.Sync()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		n++
	}
	el := time.Since(start)
	fmt.Printf("%d sequential %d-byte write+fsync: %.0f/s, mean %.2f ms\n", n, size, float64(n)/el.Seconds(), el.Seconds()*1000/float64(n))
}
