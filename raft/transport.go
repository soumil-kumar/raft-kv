package raft

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Transport carries RPCs to other members. Implementations must be safe for
// concurrent use and must return promptly once ctx is done.
type Transport interface {
	RequestVote(ctx context.Context, to Member, args *RequestVoteArgs) (*RequestVoteReply, error)
	Heartbeat(ctx context.Context, to Member, args *HeartbeatArgs) (*HeartbeatReply, error)
	AppendEntries(ctx context.Context, to Member, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to Member, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
	TimeoutNow(ctx context.Context, to Member, args *TimeoutNowArgs) (*TimeoutNowReply, error)
	ReadIndex(ctx context.Context, to Member, args *ReadIndexArgs) (*ReadIndexReply, error)
}

// TCPTransport implements Transport over net/rpc. Each peer gets two
// connections: log data and snapshots must never queue ahead of heartbeats.
type TCPTransport struct {
	dialTimeout time.Duration

	mu     sync.Mutex
	conns  map[connKey]*rpc.Client
	closed bool
}

type connKey struct {
	addr string
	bulk bool
}

// NewTCPTransport returns a transport that dials lazily.
func NewTCPTransport(dialTimeout time.Duration) *TCPTransport {
	return &TCPTransport{dialTimeout: dialTimeout, conns: map[connKey]*rpc.Client{}}
}

func (t *TCPTransport) client(ctx context.Context, k connKey) (*rpc.Client, error) {
	t.mu.Lock()
	c := t.conns[k]
	t.mu.Unlock()
	if c != nil {
		return c, nil
	}
	d := net.Dialer{Timeout: t.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", k.addr)
	if err != nil {
		return nil, err
	}
	fresh := rpc.NewClient(conn)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		fresh.Close()
		return nil, ErrStopped
	}
	if c := t.conns[k]; c != nil { // lost a dial race
		fresh.Close()
		return c, nil
	}
	t.conns[k] = fresh
	return fresh, nil
}

func (t *TCPTransport) drop(k connKey, c *rpc.Client) {
	t.mu.Lock()
	if t.conns[k] == c {
		delete(t.conns, k)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *TCPTransport) call(ctx context.Context, to Member, bulk bool, method string, args, reply any) error {
	k := connKey{to.RaftAddr, bulk}
	c, err := t.client(ctx, k)
	if err != nil {
		return err
	}
	// client.Go encodes synchronously and can block on a full socket, so it must
	// not run on the caller's goroutine.
	done := make(chan error, 1)
	go func() { done <- c.Call("Raft."+method, args, reply) }()
	select {
	case err := <-done:
		if err != nil {
			if _, remote := err.(rpc.ServerError); !remote {
				t.drop(k, c)
			}
		}
		return err
	case <-ctx.Done():
		// The connection may be wedged; start over on a new one.
		if ctx.Err() == context.DeadlineExceeded {
			t.drop(k, c)
		}
		return ctx.Err()
	}
}

func (t *TCPTransport) RequestVote(ctx context.Context, to Member, a *RequestVoteArgs) (*RequestVoteReply, error) {
	var r RequestVoteReply
	if err := t.call(ctx, to, false, "RequestVote", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) Heartbeat(ctx context.Context, to Member, a *HeartbeatArgs) (*HeartbeatReply, error) {
	var r HeartbeatReply
	if err := t.call(ctx, to, false, "Heartbeat", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) AppendEntries(ctx context.Context, to Member, a *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var r AppendEntriesReply
	if err := t.call(ctx, to, true, "AppendEntries", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) InstallSnapshot(ctx context.Context, to Member, a *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	var r InstallSnapshotReply
	if err := t.call(ctx, to, true, "InstallSnapshot", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) TimeoutNow(ctx context.Context, to Member, a *TimeoutNowArgs) (*TimeoutNowReply, error) {
	var r TimeoutNowReply
	if err := t.call(ctx, to, false, "TimeoutNow", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) ReadIndex(ctx context.Context, to Member, a *ReadIndexArgs) (*ReadIndexReply, error) {
	var r ReadIndexReply
	if err := t.call(ctx, to, false, "ReadIndex", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Close drops all connections.
func (t *TCPTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for k, c := range t.conns {
		c.Close()
		delete(t.conns, k)
	}
}

// Server exposes a node's RPC handlers on a listener.
type Server struct {
	ln net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

type rpcService struct{ r *Raft }

func (s *rpcService) RequestVote(a *RequestVoteArgs, out *RequestVoteReply) error {
	return put(out)(s.r.HandleRequestVote(a))
}
func (s *rpcService) Heartbeat(a *HeartbeatArgs, out *HeartbeatReply) error {
	return put(out)(s.r.HandleHeartbeat(a))
}
func (s *rpcService) AppendEntries(a *AppendEntriesArgs, out *AppendEntriesReply) error {
	return put(out)(s.r.HandleAppendEntries(a))
}
func (s *rpcService) InstallSnapshot(a *InstallSnapshotArgs, out *InstallSnapshotReply) error {
	return put(out)(s.r.HandleInstallSnapshot(a))
}
func (s *rpcService) TimeoutNow(a *TimeoutNowArgs, out *TimeoutNowReply) error {
	return put(out)(s.r.HandleTimeoutNow(a))
}
func (s *rpcService) ReadIndex(a *ReadIndexArgs, out *ReadIndexReply) error {
	return put(out)(s.r.HandleReadIndex(a))
}

func put[T any](out *T) func(*T, error) error {
	return func(reply *T, err error) error {
		if err == nil {
			*out = *reply
		}
		return err
	}
}

// Serve starts answering RPCs for r on ln.
func Serve(ln net.Listener, r *Raft) (*Server, error) {
	rs := rpc.NewServer()
	if err := rs.RegisterName("Raft", &rpcService{r}); err != nil {
		return nil, err
	}
	s := &Server{ln: ln, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			go func() {
				rs.ServeConn(conn)
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
			}()
		}
	}()
	return s, nil
}

// Close stops the listener and every open connection.
func (s *Server) Close() {
	s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
}
