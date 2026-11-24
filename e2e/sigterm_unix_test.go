//go:build unix

package e2e

import (
	"syscall"
	"testing"
	"time"
)

// SIGTERM must hand leadership over before the process exits. Without the
// handover nobody would lead until an election timeout later, so a leader that
// already exists the moment the old process is gone proves it happened.
func TestSIGTERMHandsOverLeadership(t *testing.T) {
	c := newCluster(t, 3)
	c.put("k", "v")
	for range 3 {
		old := c.leader()
		rest := c.others(old)
		if err := old.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := old.cmd.Wait(); err != nil {
			t.Fatalf("exit: %v", err)
		}
		took := time.Since(start)
		old.log.Close()
		old.cmd = nil
		handed := false
		for _, p := range rest {
			if _, st := do("GET", p.base()+"/status", ""); st.IsLeader {
				handed = true
			}
		}
		if !handed {
			t.Fatal("process exited without handing over leadership")
		}
		t.Logf("handed over and exited in %v", took)
		c.put("k", "v")
		c.start(old)
		eventually(t, 15*time.Second, old.id+" ready", func() bool { code, _ := do("GET", old.base()+"/readyz", ""); return code == 200 })
	}
}
