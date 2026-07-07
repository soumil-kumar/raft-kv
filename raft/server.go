package raft

import (
	"log"
	"net"
	"net/rpc"
	"time"
)

type Server struct {
	addr     string
	rpcSrv   *rpc.Server
	listener net.Listener
}

func NewServer(addr string) *Server {
	return &Server{
		addr:   addr,
		rpcSrv: rpc.NewServer(),
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
				log.Printf("RPC accept error: %v", err)
				return
			}
			go s.rpcSrv.ServeConn(conn)
		}
	}()
	return nil
}

func (s *Server) Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	// Setup connection with a timeout
	conn, err := net.DialTimeout("tcp", peerAddr, 50*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	
	client := rpc.NewClient(conn)
	defer client.Close()
	
	call := client.Go(rpcname, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			return false
		}
		return true
	case <-time.After(100 * time.Millisecond): // RPC timeout
		return false
	}
}
