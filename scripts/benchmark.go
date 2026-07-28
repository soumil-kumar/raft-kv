package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

type command struct {
	Value string `json:"value"`
}

func main() {
	url := flag.String("url", "http://localhost:8001/store", "Base URL of the KV endpoint")
	concurrency := flag.Int("c", 10, "Number of concurrent workers")
	requests := flag.Int("n", 100, "Total number of requests")
	flag.Parse()

	log.Printf("Starting benchmark: %d requests, %d concurrency, targeting %s", *requests, *concurrency, *url)

	start := time.Now()
	var wg sync.WaitGroup
	
	reqsPerWorker := *requests / *concurrency
	errors := 0
	
	latencies := make([]time.Duration, *requests)
	var latMu sync.Mutex
	latIndex := 0

	var errMu sync.Mutex

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for j := 0; j < reqsPerWorker; j++ {
				key := fmt.Sprintf("bench-key-%d-%d", workerID, j)
				cmd := command{
					Value: fmt.Sprintf("bench-val-%d", time.Now().UnixNano()),
				}
				body, _ := json.Marshal(cmd)
				
				targetUrl := fmt.Sprintf("%s/%s", *url, key)
				req, err := http.NewRequest("PUT", targetUrl, bytes.NewBuffer(body))
				if err != nil {
					errMu.Lock()
					errors++
					errMu.Unlock()
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				
				reqStart := time.Now()
				resp, err := client.Do(req)
				reqDur := time.Since(reqStart)

				if err != nil || resp.StatusCode != http.StatusOK {
					errMu.Lock()
					errors++
					if errors <= 5 {
						if err != nil {
							fmt.Printf("Error: %v\n", err)
						} else {
							bodyBytes, _ := io.ReadAll(resp.Body)
							fmt.Printf("Status %d: %s\n", resp.StatusCode, string(bodyBytes))
						}
					}
					errMu.Unlock()
				} else {
					latMu.Lock()
					latencies[latIndex] = reqDur
					latIndex++
					latMu.Unlock()
				}
				
				if resp != nil && resp.Body != nil {
					io.ReadAll(resp.Body)
					resp.Body.Close()
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	rps := float64(*requests) / duration.Seconds()

	var totalLatency time.Duration
	for i := 0; i < latIndex; i++ {
		totalLatency += latencies[i]
	}
	avgLatency := time.Duration(0)
	var p50, p95, p99 time.Duration
	if latIndex > 0 {
		avgLatency = totalLatency / time.Duration(latIndex)
		
		validLats := latencies[:latIndex]
		sort.Slice(validLats, func(i, j int) bool {
			return validLats[i] < validLats[j]
		})
		
		p50 = validLats[int(float64(latIndex)*0.50)]
		p95 = validLats[int(float64(latIndex)*0.95)]
		p99 = validLats[int(float64(latIndex)*0.99)]
	}

	fmt.Printf("Benchmark completed in %v\n", duration)
	fmt.Printf("Total Requests: %d\n", *requests)
	fmt.Printf("Errors:         %d\n", errors)
	fmt.Printf("Requests/sec:   %.2f\n", rps)
	fmt.Printf("Avg Latency:    %v\n", avgLatency)
	fmt.Printf("p50 Latency:    %v\n", p50)
	fmt.Printf("p95 Latency:    %v\n", p95)
	fmt.Printf("p99 Latency:    %v\n", p99)
}
