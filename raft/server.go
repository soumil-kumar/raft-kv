package raft

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

type NetworkTransport interface {
	Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool
}

type RealTransport struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client
}

func NewRealTransport() *RealTransport {
	return &RealTransport{
		clients: make(map[string]*rpc.Client),
	}
}

func (t *RealTransport) getClient(peerAddr string) (*rpc.Client, error) {
	t.mu.Lock()
	client, ok := t.clients[peerAddr]
	t.mu.Unlock()

	if ok && client != nil {
		return client, nil
	}

	conn, err := net.DialTimeout("tcp", peerAddr, 1000*time.Millisecond)
	if err != nil {
		return nil, err
	}
	
	newClient := rpc.NewClient(conn)
	t.mu.Lock()
	t.clients[peerAddr] = newClient
	t.mu.Unlock()
	return newClient, nil
}

func (t *RealTransport) removeClient(peerAddr string, client *rpc.Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.clients[peerAddr] == client {
		delete(t.clients, peerAddr)
		client.Close()
	}
}

func (t *RealTransport) Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	client, err := t.getClient(peerAddr)
	if err != nil {
		return false
	}
	
	call := client.Go(rpcname, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			t.removeClient(peerAddr, client)
			return false
		}
		return true
	case <-time.After(1000 * time.Millisecond): // RPC timeout
		t.removeClient(peerAddr, client)
		return false
	}
}

type Server struct {
	addr      string
	rpcSrv    *rpc.Server
	listener  net.Listener
	transport NetworkTransport
}

func NewServer(addr string, transport NetworkTransport) *Server {
	if transport == nil {
		transport = NewRealTransport()
	}
	return &Server{
		addr:      addr,
		rpcSrv:    rpc.NewServer(),
		transport: transport,
	}
}

func (s *Server) Start(raft *Raft) error {
	s.rpcSrv.Register(raft)
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = l

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				// Listener closed
				return
			}
			go s.rpcSrv.ServeConn(conn)
		}
	}()
	return nil
}

func (s *Server) Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	return s.transport.Call(peerAddr, rpcname, args, reply)
}
