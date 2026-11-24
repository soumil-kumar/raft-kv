package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func openStore(t *testing.T, dir string) (*FileStorage, *State) {
	t.Helper()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.noSync = true
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, st
}

func ent(i, term uint64) LogEntry {
	return LogEntry{Index: i, Term: term, Data: []byte(fmt.Sprintf("cmd-%d-%d", i, term))}
}

func save(t *testing.T, s *FileStorage, ops ...SaveOp) {
	t.Helper()
	if err := s.Save(ops); err != nil {
		t.Fatal(err)
	}
}

func walFiles(dir string) []string {
	m, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	return m
}

func TestStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, st := openStore(t, dir)
	if !st.Empty {
		t.Fatal("new directory should be empty")
	}
	save(t, s, SaveOp{HardState: &HardState{3, "n2"}, Entries: []LogEntry{ent(1, 1), ent(2, 3)}})
	save(t, s, SaveOp{Entries: []LogEntry{{Index: 3, Term: 3, Type: EntryConfig, Data: []byte{}}}})
	s.Close()

	_, st = openStore(t, dir)
	if st.Empty || st.HardState != (HardState{3, "n2"}) || len(st.Entries) != 3 {
		t.Fatalf("recovered %+v", st)
	}
	if string(st.Entries[1].Data) != "cmd-2-3" || st.Entries[2].Type != EntryConfig {
		t.Fatalf("recovered %+v", st.Entries)
	}
}

// Regression: replaying a truncation used to be off by one and ignored the
// snapshot offset, so restarted nodes kept entries that had been overwritten.
func TestStorageOverwriteTruncates(t *testing.T) {
	for _, base := range []uint64{0, 1000} {
		t.Run(fmt.Sprint("base=", base), func(t *testing.T) {
			dir := t.TempDir()
			s, _ := openStore(t, dir)
			if base > 0 {
				meta := SnapshotMeta{Index: base, Term: 1}
				if err := s.WriteSnapshot(meta, []byte("state")); err != nil {
					t.Fatal(err)
				}
				if err := s.Compact(meta); err != nil {
					t.Fatal(err)
				}
			}
			save(t, s, SaveOp{Entries: []LogEntry{ent(base+1, 1), ent(base+2, 1), ent(base+3, 1)}})
			save(t, s, SaveOp{Entries: []LogEntry{ent(base+2, 2)}})
			want := []LogEntry{ent(base+1, 1), ent(base+2, 2)}
			s.Close()
			s, st := openStore(t, dir)
			if !reflect.DeepEqual(st.Entries, want) {
				t.Fatalf("got %v want %v", st.Entries, want)
			}
			save(t, s, SaveOp{Entries: []LogEntry{ent(base+3, 2)}})
			s.Close()
			if _, st = openStore(t, dir); len(st.Entries) != 3 || st.Entries[2].Term != 2 {
				t.Fatalf("got %v", st.Entries)
			}
		})
	}
}

// Regression: a torn record used to make everything appended after it
// unreadable. Cut the WAL at every byte of the last frame, then keep writing.
func TestStorageTornTailAtEveryOffset(t *testing.T) {
	src := t.TempDir()
	s, _ := openStore(t, src)
	save(t, s, SaveOp{HardState: &HardState{1, "n1"}, Entries: []LogEntry{ent(1, 1), ent(2, 1)}})
	acked := s.size
	save(t, s, SaveOp{Entries: []LogEntry{ent(3, 1), ent(4, 1), ent(5, 1), ent(6, 1), ent(7, 1), ent(8, 1)}})
	path := s.walPath(s.gen)
	s.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cut := acked; cut < int64(len(full)); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(path)), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		s, st := openStore(t, dir)
		if len(st.Entries) != 2 || st.HardState.VotedFor != "n1" {
			t.Fatalf("cut %d: recovered %+v", cut, st)
		}
		save(t, s, SaveOp{Entries: []LogEntry{ent(3, 2)}})
		s.Close()
		if s, st = openStore(t, dir); len(st.Entries) != 3 || st.Entries[2].Term != 2 {
			t.Fatalf("cut %d: after append recovered %+v", cut, st.Entries)
		}
		s.Close()
	}
}

func TestStorageZeroFilledTailIsTorn(t *testing.T) {
	dir := t.TempDir()
	s, _ := openStore(t, dir)
	save(t, s, SaveOp{Entries: []LogEntry{ent(1, 1)}})
	path := s.walPath(s.gen)
	s.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write(make([]byte, 500))
	f.Close()
	if _, st := openStore(t, dir); len(st.Entries) != 1 {
		t.Fatalf("recovered %v", st.Entries)
	}
}

// Damage to acknowledged data must be reported, never silently dropped.
func TestStorageDetectsCorruption(t *testing.T) {
	src := t.TempDir()
	s, _ := openStore(t, src)
	save(t, s, SaveOp{Entries: []LogEntry{ent(1, 1)}})
	mid := s.size
	save(t, s, SaveOp{Entries: []LogEntry{ent(2, 1)}})
	path := s.walPath(s.gen)
	s.Close()
	full, _ := os.ReadFile(path)
	for off := int64(0); off < mid; off++ {
		dir := t.TempDir()
		bad := append([]byte(nil), full...)
		bad[off] ^= 0x40
		os.WriteFile(filepath.Join(dir, filepath.Base(path)), bad, 0o644)
		s, _ := OpenFileStorage(dir)
		if _, err := s.Load(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flip at %d: got %v, want ErrCorrupt", off, err)
		}
		s.Close()
	}
}

func TestStorageCompact(t *testing.T) {
	dir := t.TempDir()
	s, _ := openStore(t, dir)
	conf := Configuration{Members: []Member{{ID: "a", RaftAddr: "a"}}}
	save(t, s, SaveOp{HardState: &HardState{2, "x"}})
	for i := uint64(1); i <= 10; i++ {
		save(t, s, SaveOp{Entries: []LogEntry{ent(i, 2)}})
	}
	meta := SnapshotMeta{Index: 7, Term: 2, Config: conf}
	if err := s.WriteSnapshot(meta, []byte("seven")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(meta); err != nil {
		t.Fatal(err)
	}
	save(t, s, SaveOp{Entries: []LogEntry{ent(11, 2)}})
	s.Close()
	s, st := openStore(t, dir)
	if string(st.Snapshot) != "seven" || st.Meta.Index != 7 || !st.Meta.Config.isVoter("a") ||
		len(st.Entries) != 4 || st.Entries[0].Index != 8 || st.HardState.VotedFor != "x" {
		t.Fatalf("recovered %+v", st)
	}
	// A snapshot that disagrees with the log discards all of it.
	meta = SnapshotMeta{Index: 9, Term: 5, Config: conf}
	if err := s.WriteSnapshot(meta, []byte("nine")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(meta); err != nil {
		t.Fatal(err)
	}
	s.Close()
	_, st = openStore(t, dir)
	if len(st.Entries) != 0 || st.Meta.Index != 9 || string(st.Snapshot) != "nine" {
		t.Fatalf("recovered %+v", st)
	}
	if snaps, _ := filepath.Glob(filepath.Join(dir, "snap-*")); len(snaps) != 1 || len(walFiles(dir)) != 1 {
		t.Fatalf("old files left behind: %v %v", snaps, walFiles(dir))
	}
}

// A crash at any point of a compaction leaves either the old or the new state.
func TestStorageCompactCrashWindows(t *testing.T) {
	build := func(t *testing.T) (string, *FileStorage) {
		dir := t.TempDir()
		s, _ := openStore(t, dir)
		save(t, s, SaveOp{Entries: []LogEntry{ent(1, 1), ent(2, 1), ent(3, 1)}})
		return dir, s
	}
	meta := SnapshotMeta{Index: 2, Term: 1}

	t.Run("snapshot published, WAL not rotated", func(t *testing.T) {
		dir, s := build(t)
		s.WriteSnapshot(meta, []byte("two"))
		s.Close()
		if _, st := openStore(t, dir); st.Meta.Index != 0 || len(st.Entries) != 3 {
			t.Fatalf("recovered %+v", st)
		}
	})
	t.Run("half-written temp files", func(t *testing.T) {
		dir, s := build(t)
		s.Close()
		os.WriteFile(s.walPath(s.gen+1)+".tmp", []byte("partial"), 0o644)
		os.WriteFile(filepath.Join(dir, "snap-123.tmp"), []byte("partial"), 0o644)
		if _, st := openStore(t, dir); len(st.Entries) != 3 {
			t.Fatalf("recovered %+v", st)
		}
		if tmp, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(tmp) != 0 {
			t.Fatalf("temp files left: %v", tmp)
		}
	})
	t.Run("new WAL in place, old not yet removed", func(t *testing.T) {
		dir, s := build(t)
		oldPath := s.walPath(s.gen)
		old, _ := os.ReadFile(oldPath)
		s.WriteSnapshot(meta, []byte("two"))
		if err := s.Compact(meta); err != nil {
			t.Fatal(err)
		}
		s.Close()
		os.WriteFile(oldPath, old, 0o644)
		if _, st := openStore(t, dir); st.Meta.Index != 2 || len(st.Entries) != 1 {
			t.Fatalf("recovered %+v", st)
		}
	})
	t.Run("corrupt snapshot is refused", func(t *testing.T) {
		dir, s := build(t)
		s.WriteSnapshot(meta, []byte("two"))
		s.Compact(meta)
		s.Close()
		b, _ := os.ReadFile(s.snapPath(meta))
		b[len(b)-1] ^= 1
		os.WriteFile(s.snapPath(meta), b, 0o644)
		s2, _ := OpenFileStorage(dir)
		if _, err := s2.Load(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("got %v", err)
		}
		s2.Close()
	})
}

func FuzzReadFrame(f *testing.F) {
	f.Add(appendFrame(nil, []byte("hello")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if p, n, err := readFrame(b); err == nil {
			if n > len(b) || n != frameHeader+len(p) {
				t.Fatalf("n=%d len=%d", n, len(b))
			}
			s := &FileStorage{}
			s.applyPayload(p) // must not panic
		}
	})
}
