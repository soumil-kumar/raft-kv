package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"raft-kv/httpd"
	"raft-kv/store"
)

func main() {
	// --- Command-line flags ---
	nodeID := flag.String("node-id", "", "Unique node identifier (required)")
	httpAddr := flag.String("http-addr", ":8001", "HTTP API bind address")
	raftAddr := flag.String("raft-addr", ":9001", "Raft inter-node communication address")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer Raft addresses (e.g. localhost:9002,localhost:9003)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
╔═══════════════════════════════════════════════════════════════╗
║              raft-kv: Distributed Key-Value Store             ║
║          Built with a custom from-scratch Raft Engine         ║
╚═══════════════════════════════════════════════════════════════╝

A fault-tolerant, distributed key-value store that replicates data
across a cluster of nodes using the Raft consensus protocol.

Usage:
  raft-kv [flags]

Flags:
`)
		flag.PrintDefaults()
	}

	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Error: --node-id is required")
	}

	peers := []string{}
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}
	peers = append(peers, *raftAddr) // Include self in peers for simplicity

	log.Printf("=========================================")
	log.Printf("  raft-kv starting up")
	log.Printf("  Node ID:    %s", *nodeID)
	log.Printf("  HTTP:       %s", *httpAddr)
	log.Printf("  Raft:       %s", *raftAddr)
	log.Printf("  Peers:      %v", peers)
	log.Printf("=========================================")

	// --- Initialize the store ---
	s := store.New(*nodeID, *raftAddr, peers)

	if err := s.Open(); err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}

	// --- Start HTTP server ---
	h := httpd.New(*httpAddr, s)

	go func() {
		if err := h.Start(); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	log.Printf("raft-kv is ready! Node %s is up.", *nodeID)

	// --- Wait for shutdown signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("Shutting down node %s...", *nodeID)
}
