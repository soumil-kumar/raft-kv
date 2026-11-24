package raft

// raftLog is the in-memory suffix of the replicated log. Every method takes
// absolute indices; nothing outside this file does offset arithmetic.
//
// Invariant: entries[i].Index == base+1+i.
type raftLog struct {
	base     uint64 // index of the entry just before entries[0]
	baseTerm uint64
	entries  []LogEntry
}

func (l *raftLog) lastIndex() uint64 { return l.base + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Term
	}
	return l.baseTerm
}

// term returns the term at index i, or false if i is compacted or absent.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.base:
		return l.baseTerm, true
	case i < l.base || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.base-1].Term, true
}

// slice copies entries lo..hi inclusive, stopping early once maxEntries or
// maxBytes is reached (always returning at least one entry if any exists).
// It copies because append after truncate reuses the backing array.
func (l *raftLog) slice(lo, hi uint64, maxEntries, maxBytes int) []LogEntry {
	hi = min(hi, l.lastIndex())
	if lo <= l.base || lo > hi {
		return nil
	}
	src := l.entries[lo-l.base-1 : hi-l.base]
	n, bytes := 0, 0
	for n < len(src) && n < maxEntries {
		bytes += len(src[n].Data)
		if n > 0 && bytes > maxBytes {
			break
		}
		n++
	}
	return append([]LogEntry(nil), src[:n]...)
}

func (l *raftLog) append(e ...LogEntry) { l.entries = append(l.entries, e...) }

// truncateFrom drops index i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i > l.base && i <= l.lastIndex() {
		l.entries = l.entries[:i-l.base-1]
	}
}

// compactTo drops entries up to and including i, which must be present.
func (l *raftLog) compactTo(i uint64) {
	t, ok := l.term(i)
	if !ok || i <= l.base {
		return
	}
	l.entries = append([]LogEntry(nil), l.entries[i-l.base:]...) // free the prefix
	l.base, l.baseTerm = i, t
}

func (l *raftLog) reset(base, term uint64) {
	l.base, l.baseTerm, l.entries = base, term, nil
}

// lastIndexOfTerm finds the last in-memory entry with the given term.
func (l *raftLog) lastIndexOfTerm(t uint64) (uint64, bool) {
	for i := len(l.entries) - 1; i >= 0 && l.entries[i].Term >= t; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index, true
		}
	}
	return 0, false
}

// firstIndexOfTermAt walks back from i to the first entry sharing its term.
func (l *raftLog) firstIndexOfTermAt(i uint64) uint64 {
	t, ok := l.term(i)
	for ok && i > l.base+1 {
		if pt, pok := l.term(i - 1); !pok || pt != t {
			break
		}
		i--
	}
	return i
}
