package main

import (
	"strings"
	"testing"
	"time"
)

func TestParsePeers(t *testing.T) {
	c, err := parsePeers("a=h1:1, b=h2:2", "b=h2:8,a=h1:8")
	if err != nil {
		t.Fatal(err)
	}
	if m, _ := c.Find("a"); m.RaftAddr != "h1:1" || m.HTTPAddr != "h1:8" {
		t.Fatalf("%+v", m)
	}
	for _, bad := range [][2]string{
		{"", ""},
		{":9001,:9002", ":8001,:8002"}, // the old positional format
		{"a=h:1,a=h:2", "a=h:8"},
		{"a=h:1", "b=h:8"},
		{"a=h:1,b=h:2", "a=h:8"},
		{"a=h:1,b=h:1", "a=h:8,b=h:9"},
		{"a=noport", "a=h:8"},
	} {
		if _, err := parsePeers(bad[0], bad[1]); err == nil {
			t.Errorf("accepted %q %q", bad[0], bad[1])
		}
	}
}

// Regression: a node missing from its own peer list used to add itself under a
// second address and count itself twice.
func TestNodeMustBeInItsPeerList(t *testing.T) {
	base := options{nodeID: "n9", peers: "n1=h:1", peerHTTP: "n1=h:2", logLevel: "info",
		election: time.Second, heartbeat: 10 * time.Millisecond, dataDir: t.TempDir()}
	if err := run(base); err == nil || !strings.Contains(err.Error(), "does not include this node") {
		t.Fatalf("got %v", err)
	}
	o := base
	o.join = true
	if err := run(o); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("got %v", err)
	}
	o = base
	o.election = 20 * time.Millisecond
	if err := run(o); err == nil || !strings.Contains(err.Error(), "election-timeout") {
		t.Fatalf("got %v", err)
	}
}
