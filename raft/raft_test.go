package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	// log.SetOutput(new(mockWriter)) // Disable logging during tests to prevent noise
}

type mockWriter struct{}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

type MockNetwork struct {
	mu           sync.Mutex
	rafts        map[string]*Raft
	disconnected map[string]bool
}

func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		rafts:        make(map[string]*Raft),
		disconnected: make(map[string]bool),
	}
}

func (n *MockNetwork) Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	n.mu.Lock()
	target, ok := n.rafts[peerAddr]
	disc := n.disconnected[peerAddr]
	n.mu.Unlock()

	if !ok || disc {
		return false
	}

	time.Sleep(5 * time.Millisecond) // Simulate minor network delay

	switch rpcname {
	case "Raft.AppendEntries":
		a := args.(AppendEntriesArgs)
		r := reply.(*AppendEntriesReply)
		return target.AppendEntries(a, r) == nil
	case "Raft.RequestVote":
		a := args.(RequestVoteArgs)
		r := reply.(*RequestVoteReply)
		return target.RequestVote(a, r) == nil
	case "Raft.InstallSnapshot":
		a := args.(InstallSnapshotArgs)
		r := reply.(*InstallSnapshotReply)
		return target.InstallSnapshot(a, r) == nil
	}
	return false
}

type NodeNetwork struct {
	me      string
	network *MockNetwork
}

func (n *NodeNetwork) Call(peerAddr string, rpcname string, args interface{}, reply interface{}) bool {
	n.network.mu.Lock()
	senderDisc := n.network.disconnected[n.me]
	n.network.mu.Unlock()
	
	if senderDisc {
		log.Printf("[NodeNetwork] %s is disconnected, dropping outgoing %s to %s", n.me, rpcname, peerAddr)
		return false
	}
	
	return n.network.Call(peerAddr, rpcname, args, reply)
}

func (n *MockNetwork) Disconnect(node string) {
	n.mu.Lock()
	n.disconnected[node] = true
	n.mu.Unlock()
}

func (n *MockNetwork) Connect(node string) {
	n.mu.Lock()
	n.disconnected[node] = false
	n.mu.Unlock()
}

type TestCluster struct {
	network *MockNetwork
	nodes   map[string]*Raft
	peers   []string
}

func NewTestCluster(n int) *TestCluster {
	peers := make([]string, n)
	for i := 0; i < n; i++ {
		peers[i] = string(rune('A' + i))
	}

	network := NewMockNetwork()
	cluster := &TestCluster{
		network: network,
		nodes:   make(map[string]*Raft),
		peers:   peers,
	}

	for _, peer := range peers {
		nodeNet := &NodeNetwork{me: peer, network: network}
		server := NewServer(peer, nodeNet)
		persister := NewPersister("") // In-memory
		applyCh := make(chan ApplyMsg, 100)
		
		raft := New(peer, peers, server, persister, applyCh)
		network.rafts[peer] = raft
		cluster.nodes[peer] = raft
		server.Start(raft) // Start server processing
	}

	return cluster
}

func (c *TestCluster) checkOneLeader() string {
	for i := 0; i < 15; i++ {
		time.Sleep(300 * time.Millisecond)
		leaderCount := 0
		var leader string
		for _, r := range c.nodes {
			if r.IsLeader() {
				leaderCount++
				leader = r.me
			}
		}
		if leaderCount == 1 {
			return leader
		}
		if leaderCount > 1 {
			panic("Multiple leaders detected!")
		}
	}
	panic("No leader elected in time")
}

func TestInitialElection(t *testing.T) {
	c := NewTestCluster(3)
	leader := c.checkOneLeader()
	if leader == "" {
		t.Fatalf("Expected a leader to be elected")
	}
}

func TestReElection(t *testing.T) {
	c := NewTestCluster(3)
	leader1 := c.checkOneLeader()

	// Disconnect leader
	c.network.Disconnect(leader1)

	// Check new leader is elected
	leader2 := ""
	for i := 0; i < 30; i++ {
		time.Sleep(300 * time.Millisecond)
		for _, r := range c.nodes {
			if r.IsLeader() && r.me != leader1 {
				leader2 = r.me
			}
		}
		if leader2 != "" {
			break
		}
	}
	t.Logf("leader1=%s, leader2=%s", leader1, leader2)
	if leader1 == leader2 || leader2 == "" {
		t.Fatalf("Leader should have changed")
	}

	// Reconnect old leader
	c.network.Connect(leader1)
	
	leader3 := c.checkOneLeader()
	if leader3 != leader2 {
		t.Fatalf("Leader should be stable after old leader reconnects")
	}
}

func TestLogMatchingInvariant(t *testing.T) {
	c := NewTestCluster(3)
	leader := c.checkOneLeader()

	// Append an entry
	c.nodes[leader].Start([]byte("command1"))
	time.Sleep(1 * time.Second) // wait for replication

	// Verify all nodes have the command
	for id, r := range c.nodes {
		logLen := r.getLastLogIndex()
		if logLen != 2 {
			t.Fatalf("Node %s has log length %d, expected 2", id, logLen)
		}
	}
}

func TestSplitBrain(t *testing.T) {
	c := NewTestCluster(5)
	leader := c.checkOneLeader()

	// Find followers
	var followers []string
	for p := range c.nodes {
		if p != leader {
			followers = append(followers, p)
		}
	}

	// Partition: Leader + 1 follower in minority, 3 followers in majority
	c.network.Disconnect(leader)
	c.network.Disconnect(followers[0])
	
	// The 3 followers should elect a new leader
	newLeader := ""
	for i := 0; i < 15; i++ {
		time.Sleep(300 * time.Millisecond)
		for _, f := range followers[1:] {
			if c.nodes[f].IsLeader() {
				newLeader = f
			}
		}
		if newLeader != "" {
			break
		}
	}
	
	if newLeader == "" {
		t.Fatalf("Majority partition failed to elect a leader")
	}
	if newLeader == leader || newLeader == followers[0] {
		t.Fatalf("Minority node elected as leader!")
	}
	
	// Minority leader shouldn't be able to commit
	idx, _, isLeader := c.nodes[leader].Start([]byte("cmd2"))
	if !isLeader {
		t.Fatalf("Minority leader should still think it is leader")
	}
	
	time.Sleep(1 * time.Second)
	
	// Check commit index
	c.nodes[leader].mu.Lock()
	if c.nodes[leader].commitIndex >= idx {
		c.nodes[leader].mu.Unlock()
		t.Fatalf("Minority leader committed a log entry!")
	}
	c.nodes[leader].mu.Unlock()
}

func TestOldTerms(t *testing.T) {
	c := NewTestCluster(3)
	leader := c.checkOneLeader()

	// Send an old RPC to the leader
	args := AppendEntriesArgs{
		Term:         c.nodes[leader].currentTerm - 1,
		LeaderId:     "oldLeader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	reply := AppendEntriesReply{}
	
	c.nodes[leader].AppendEntries(args, &reply)
	
	if reply.Success {
		t.Fatalf("Leader accepted AppendEntries from older term!")
	}
}
