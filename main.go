// Command raft-kv runs one node of a replicated key-value store.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/soumil-kumar/raft-kv/httpd"
	"github.com/soumil-kumar/raft-kv/raft"
	"github.com/soumil-kumar/raft-kv/store"
)

type options struct {
	nodeID, httpAddr, raftAddr, peers, peerHTTP, dataDir, logLevel string
	join                                                           bool
	election, heartbeat                                            time.Duration
	snapshotEvery                                                  uint64
	maxBody                                                        int64
}

func main() {
	var o options
	flag.StringVar(&o.nodeID, "node-id", "", "unique node ID (required)")
	flag.StringVar(&o.httpAddr, "http-addr", ":8001", "HTTP bind address")
	flag.StringVar(&o.raftAddr, "raft-addr", ":9001", "Raft bind address")
	flag.StringVar(&o.peers, "peers", "", "initial members' Raft addresses, this node included: n1=host:port,n2=host:port")
	flag.StringVar(&o.peerHTTP, "peer-http", "", "the same members' HTTP addresses: n1=host:port,n2=host:port")
	flag.BoolVar(&o.join, "join", false, "start empty and wait to be added to an existing cluster")
	flag.StringVar(&o.dataDir, "data-dir", "", "data directory (default data/<node-id>)")
	flag.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	flag.DurationVar(&o.election, "election-timeout", 500*time.Millisecond, "minimum election timeout")
	flag.DurationVar(&o.heartbeat, "heartbeat", 50*time.Millisecond, "heartbeat interval")
	flag.Uint64Var(&o.snapshotEvery, "snapshot-threshold", 20_000, "log entries between snapshots")
	flag.Int64Var(&o.maxBody, "max-body-bytes", 1<<20, "largest accepted request body")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "raft-kv:", err)
		os.Exit(1)
	}
}

// parsePeers builds the initial configuration from "id=addr" lists.
func parsePeers(raftList, httpList string) (raft.Configuration, error) {
	parse := func(name, list string) ([]string, map[string]string, error) {
		var order []string
		m := map[string]string{}
		for _, part := range strings.Split(list, ",") {
			id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
			if !ok || id == "" || addr == "" {
				return nil, nil, fmt.Errorf("--%s: %q is not id=host:port", name, part)
			}
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, nil, fmt.Errorf("--%s: %w", name, err)
			}
			if _, dup := m[id]; dup {
				return nil, nil, fmt.Errorf("--%s: duplicate id %q", name, id)
			}
			m[id] = addr
			order = append(order, id)
		}
		return order, m, nil
	}
	var conf raft.Configuration
	order, raftAddrs, err := parse("peers", raftList)
	if err != nil {
		return conf, err
	}
	_, httpAddrs, err := parse("peer-http", httpList)
	if err != nil {
		return conf, err
	}
	if len(httpAddrs) != len(raftAddrs) {
		return conf, errors.New("--peers and --peer-http must list the same nodes")
	}
	for _, id := range order {
		h, ok := httpAddrs[id]
		if !ok {
			return conf, fmt.Errorf("--peer-http has no address for %q", id)
		}
		conf.Members = append(conf.Members, raft.Member{ID: id, RaftAddr: raftAddrs[id], HTTPAddr: h})
	}
	return conf, conf.Validate()
}

func run(o options) error {
	if o.nodeID == "" {
		return errors.New("--node-id is required")
	}
	if o.heartbeat <= 0 || o.election < 4*o.heartbeat {
		return errors.New("--election-timeout must be at least 4x --heartbeat")
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(o.logLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	var conf raft.Configuration
	switch {
	case o.join && o.peers != "":
		return errors.New("--join and --peers are mutually exclusive")
	case !o.join:
		var err error
		if conf, err = parsePeers(o.peers, o.peerHTTP); err != nil {
			return err
		}
		if _, ok := conf.Find(o.nodeID); !ok {
			return fmt.Errorf("--peers does not include this node (%s)", o.nodeID)
		}
	}
	if o.dataDir == "" {
		o.dataDir = filepath.Join("data", o.nodeID)
	}

	// Binding first also stops a second process from opening the same data.
	raftLn, err := net.Listen("tcp", o.raftAddr)
	if err != nil {
		return err
	}
	defer raftLn.Close()
	httpLn, err := net.Listen("tcp", o.httpAddr)
	if err != nil {
		return err
	}
	defer httpLn.Close()

	storage, err := raft.OpenFileStorage(o.dataDir)
	if err != nil {
		return err
	}
	transport := raft.NewTCPTransport(o.election)
	defer transport.Close()
	fatal := make(chan error, 1)
	st, err := store.Open(raft.Config{
		ID: o.nodeID, Bootstrap: conf,
		ElectionTimeout: o.election, HeartbeatInterval: o.heartbeat,
		SnapshotThreshold: o.snapshotEvery,
		Logger:            logger,
		OnFatal:           func(err error) { fatal <- err },
	}, storage, transport)
	if err != nil {
		return err
	}
	defer st.Close()
	rpcServer, err := raft.Serve(raftLn, st.Raft())
	if err != nil {
		return err
	}
	defer rpcServer.Close()

	api := httpd.New(st, httpd.Options{MaxBodyBytes: o.maxBody, Storage: storage, Logger: logger})
	go func() {
		if err := api.Serve(httpLn); err != nil {
			fatal <- err
		}
	}()
	logger.Info("ready", "node", o.nodeID, "http", httpLn.Addr().String(), "raft", raftLn.Addr().String(), "data", o.dataDir)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-fatal:
		return err
	case <-quit:
	}
	logger.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Hand over first so that clients see no election, then drain.
	if st.Raft().IsLeader() {
		tctx, tcancel := context.WithTimeout(ctx, 2*time.Second)
		if err := st.Raft().TransferLeadership(tctx, ""); err != nil {
			logger.Warn("leadership transfer failed", "err", err)
		}
		tcancel()
	}
	return api.Shutdown(ctx)
}
