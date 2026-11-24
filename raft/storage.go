package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrCorrupt means durable state failed its checksum somewhere other than the
// tail. Continuing could silently drop acknowledged writes, so the node refuses
// to start and must be rebuilt from its peers.
var ErrCorrupt = errors.New("raft: storage corrupt")

// SaveOp is one ordered mutation of durable state. Either field may be empty.
// An entry at an index that already exists replaces it and truncates the rest.
type SaveOp struct {
	HardState *HardState
	Entries   []LogEntry
}

// State is everything recovered from disk at startup.
type State struct {
	Empty     bool // nothing was ever stored
	HardState HardState
	Meta      SnapshotMeta
	Snapshot  []byte // nil when Meta.Index == 0
	Entries   []LogEntry
}

// Storage is the durable half of a Raft node. Save and Compact are only called
// from one goroutine, in log order. WriteSnapshot and ReadSnapshot may run
// concurrently with them.
type Storage interface {
	Load() (*State, error)
	// Save makes every op durable (one fsync) before returning.
	Save(ops []SaveOp) error
	// WriteSnapshot durably writes a snapshot without making it current.
	WriteSnapshot(meta SnapshotMeta, data []byte) error
	// Compact makes meta the log's base and drops entries it covers.
	Compact(meta SnapshotMeta) error
	ReadSnapshot(meta SnapshotMeta) ([]byte, error)
	Close() error
}

// FileStorage keeps a checksummed write-ahead log plus snapshot files:
//
//	wal-<generation>.log     the current log
//	snap-<index>-<term>.snap the snapshot that log starts from
//
// The WAL is a sequence of frames; each Save is exactly one frame, so a crash
// can tear only the final, never-acknowledged frame:
//
//	len u32 | crc32c(len) u32 | crc32c(payload) u32 | payload
//
// Compaction writes a complete new generation to a temp file, fsyncs it, and
// renames it into place, so there is always one intact WAL on disk.
type FileStorage struct {
	dir    string
	noSync bool // tests only

	file *os.File
	gen  uint64
	size int64
	buf  []byte

	// Shadow of what the WAL currently encodes; the next generation is built
	// from it.
	hs      HardState
	meta    SnapshotMeta
	entries []LogEntry

	snapMu sync.RWMutex // snapshot file rename/delete vs read

	// SyncCount and SavedEntries expose the group-commit ratio.
	SyncCount    atomic.Uint64
	SavedEntries atomic.Uint64

	// Test hooks.
	beforeSync func() error
	syncedSize atomic.Int64
}

const (
	frameHeader  = 12
	maxFrame     = 1 << 30
	recHeader    = 1
	recHardState = 2
	recEntry     = 3
	snapMagic    = "RKVSNAP1"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFileStorage opens (creating if needed) a node's data directory.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (s *FileStorage) walPath(gen uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("wal-%016x.log", gen))
}

func (s *FileStorage) snapPath(m SnapshotMeta) string {
	return filepath.Join(s.dir, fmt.Sprintf("snap-%016x-%016x.snap", m.Index, m.Term))
}

// Load recovers state, repairs a torn tail, and leaves the WAL open for appends.
func (s *FileStorage) Load() (*State, error) {
	names, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	found := false
	for _, n := range names {
		var g uint64
		if c, _ := fmt.Sscanf(n.Name(), "wal-%016x.log", &g); c == 1 && strings.HasSuffix(n.Name(), ".log") {
			found = true
			s.gen = max(s.gen, g)
		}
	}
	if !found {
		for _, n := range names {
			if strings.HasSuffix(n.Name(), ".snap") {
				return nil, fmt.Errorf("%w: snapshot %s without a WAL", ErrCorrupt, n.Name())
			}
		}
		if err := s.rotate(); err != nil {
			return nil, err
		}
		return &State{Empty: true}, nil
	}

	if err := s.replay(s.walPath(s.gen)); err != nil {
		return nil, err
	}
	st := &State{HardState: s.hs, Meta: s.meta, Entries: append([]LogEntry(nil), s.entries...)}
	st.Empty = s.hs == (HardState{}) && s.meta.Index == 0 && len(s.meta.Config.Members) == 0 && len(s.entries) == 0
	if s.meta.Index > 0 {
		if st.Snapshot, err = s.ReadSnapshot(s.meta); err != nil {
			return nil, err
		}
	}

	// Leftovers of an interrupted compaction are safe to drop only now.
	keepSnap, keepWAL := filepath.Base(s.snapPath(s.meta)), filepath.Base(s.walPath(s.gen))
	for _, n := range names {
		name := n.Name()
		stale := strings.HasSuffix(name, ".tmp") ||
			(strings.HasPrefix(name, "snap-") && name != keepSnap) ||
			(strings.HasPrefix(name, "wal-") && name != keepWAL)
		if stale {
			os.Remove(filepath.Join(s.dir, name))
		}
	}
	return st, nil
}

func (s *FileStorage) replay(path string) (err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	good := 0
	for good < len(data) {
		payload, n, err := readFrame(data[good:])
		if errors.Is(err, errTorn) {
			break
		}
		if err == nil {
			err = s.applyPayload(payload)
		}
		if err != nil {
			return fmt.Errorf("%s at offset %d: %w", filepath.Base(path), good, err)
		}
		good += n
	}
	if good == 0 {
		// The header frame is fsynced before the file gets its final name.
		return fmt.Errorf("%w: %s has no header", ErrCorrupt, filepath.Base(path))
	}
	if good < len(data) {
		// Cut the torn frame off so later appends cannot land after garbage.
		if err := f.Truncate(int64(good)); err != nil {
			return err
		}
		if err := s.sync(f); err != nil {
			return err
		}
	}
	if _, err := f.Seek(int64(good), io.SeekStart); err != nil {
		return err
	}
	s.file, s.size = f, int64(good)
	s.syncedSize.Store(s.size)
	return nil
}

var errTorn = errors.New("torn frame")

// readFrame separates a torn tail (an unacknowledged write that a crash cut
// short) from corruption of data that had already been acknowledged.
func readFrame(b []byte) (payload []byte, n int, err error) {
	if len(b) < frameHeader {
		return nil, 0, errTorn
	}
	size := binary.LittleEndian.Uint32(b)
	if crc32.Checksum(b[:4], crcTable) != binary.LittleEndian.Uint32(b[4:]) || size > maxFrame || size == 0 {
		if len(bytes.Trim(b, "\x00")) == 0 {
			return nil, 0, errTorn // file was extended but never written
		}
		return nil, 0, fmt.Errorf("%w: bad frame header", ErrCorrupt)
	}
	end := frameHeader + int(size)
	if len(b) < end {
		return nil, 0, errTorn
	}
	payload = b[frameHeader:end]
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(b[8:]) {
		if len(b) == end {
			return nil, 0, errTorn
		}
		return nil, 0, fmt.Errorf("%w: bad frame checksum", ErrCorrupt)
	}
	return payload, end, nil
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func encodeHardState(b []byte, hs HardState) []byte {
	b = append(b, recHardState)
	b = binary.AppendUvarint(b, hs.Term)
	return appendBytes(b, []byte(hs.VotedFor))
}

func encodeEntry(b []byte, e LogEntry) []byte {
	b = append(b, recEntry)
	b = binary.AppendUvarint(b, e.Index)
	b = binary.AppendUvarint(b, e.Term)
	b = append(b, byte(e.Type))
	return appendBytes(b, e.Data)
}

type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.bad, d.b = true, nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) byte() byte {
	if len(d.b) == 0 {
		d.bad = true
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.bad, d.b = true, nil
		return nil
	}
	v := append([]byte(nil), d.b[:n]...)
	d.b = d.b[n:]
	return v
}

func (s *FileStorage) applyPayload(p []byte) error {
	d := &decoder{b: p}
	for len(d.b) > 0 && !d.bad {
		switch t := d.byte(); t {
		case recHeader:
			if err := json.Unmarshal(d.bytes(), &s.meta); err != nil {
				return fmt.Errorf("%w: header: %v", ErrCorrupt, err)
			}
		case recHardState:
			term := d.uvarint()
			s.hs = HardState{Term: term, VotedFor: string(d.bytes())}
		case recEntry:
			var e LogEntry
			e.Index = d.uvarint()
			e.Term = d.uvarint()
			e.Type = EntryType(d.byte())
			e.Data = d.bytes()
			if d.bad {
				break
			}
			if err := s.shadowAppend(e); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, t)
		}
	}
	if d.bad {
		return fmt.Errorf("%w: short record", ErrCorrupt)
	}
	return nil
}

// shadowAppend applies "an entry replaces its index and truncates the rest".
func (s *FileStorage) shadowAppend(e LogEntry) error {
	next := s.meta.Index + uint64(len(s.entries)) + 1
	if e.Index <= s.meta.Index || e.Index > next {
		return fmt.Errorf("%w: entry %d does not follow log (%d, %d)", ErrCorrupt, e.Index, s.meta.Index, next)
	}
	s.entries = append(s.entries[:e.Index-s.meta.Index-1], e)
	return nil
}

func putFrameHeader(dst, payload []byte) {
	binary.LittleEndian.PutUint32(dst[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(dst[4:], crc32.Checksum(dst[:4], crcTable))
	binary.LittleEndian.PutUint32(dst[8:], crc32.Checksum(payload, crcTable))
}

func appendFrame(dst, payload []byte) []byte {
	var h [frameHeader]byte
	putFrameHeader(h[:], payload)
	return append(append(dst, h[:]...), payload...)
}

// Save appends all ops as one frame and fsyncs once (group commit).
func (s *FileStorage) Save(ops []SaveOp) error {
	b := append(s.buf[:0], make([]byte, frameHeader)...)
	n := 0
	for _, op := range ops {
		if op.HardState != nil {
			b = encodeHardState(b, *op.HardState)
			s.hs = *op.HardState
		}
		for _, e := range op.Entries {
			b = encodeEntry(b, e)
			if err := s.shadowAppend(e); err != nil {
				return err
			}
			n++
		}
	}
	payload := b[frameHeader:]
	if len(payload) == 0 {
		return nil
	}
	if len(payload) > maxFrame {
		return fmt.Errorf("raft: WAL frame of %d bytes is too large", len(payload))
	}
	putFrameHeader(b, payload)

	if _, err := s.file.Write(b); err != nil {
		return fmt.Errorf("raft: WAL write: %w", err)
	}
	s.size += int64(len(b))
	if s.buf = b; cap(b) > 4<<20 {
		s.buf = nil // do not pin one huge batch forever
	}
	if s.beforeSync != nil {
		if err := s.beforeSync(); err != nil {
			return err
		}
	}
	// A failed fsync may have dropped the dirty pages, so it is never retried;
	// the caller stops the node.
	if err := s.sync(s.file); err != nil {
		return fmt.Errorf("raft: WAL fsync: %w", err)
	}
	s.syncedSize.Store(s.size)
	s.SyncCount.Add(1)
	s.SavedEntries.Add(uint64(n))
	return nil
}

func (s *FileStorage) sync(f *os.File) error {
	if s.noSync {
		return nil
	}
	return f.Sync()
}

// Compact rebases the log on meta. Entries after meta.Index survive only if the
// log agrees with the snapshot at that point (Raft Fig. 13, steps 6-7).
func (s *FileStorage) Compact(meta SnapshotMeta) error {
	var tail []LogEntry
	if meta.Index == s.meta.Index && meta.Term == s.meta.Term {
		tail = s.entries
	} else if meta.Index > s.meta.Index && meta.Index <= s.meta.Index+uint64(len(s.entries)) {
		if at := s.entries[meta.Index-s.meta.Index-1]; at.Term == meta.Term {
			tail = s.entries[meta.Index-s.meta.Index:]
		}
	}
	s.entries = append([]LogEntry(nil), tail...)
	s.meta = meta
	if err := s.rotate(); err != nil {
		return err
	}
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	old, _ := filepath.Glob(filepath.Join(s.dir, "snap-*.snap"))
	for _, p := range old {
		var idx, term uint64
		fmt.Sscanf(filepath.Base(p), "snap-%016x-%016x.snap", &idx, &term)
		if idx < meta.Index { // never touch a newer one still being published
			os.Remove(p)
		}
	}
	return nil
}

// rotate writes the shadow state as a fresh WAL generation, atomically.
func (s *FileStorage) rotate() error {
	hdr, err := json.Marshal(s.meta)
	if err != nil {
		return err
	}
	out := appendFrame(nil, encodeHardState(appendBytes([]byte{recHeader}, hdr), s.hs))
	var payload []byte
	for i, e := range s.entries {
		payload = encodeEntry(payload, e)
		if len(payload) >= 4<<20 || i == len(s.entries)-1 {
			out = appendFrame(out, payload)
			payload = payload[:0]
		}
	}

	old, oldGen := s.file, s.gen
	path := s.walPath(s.gen + 1)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := s.publish(f, out, path); err != nil {
		return fmt.Errorf("raft: WAL rotate: %w", err)
	}
	s.gen++
	if f, err = os.OpenFile(path, os.O_RDWR, 0o644); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	s.file, s.size = f, int64(len(out))
	s.syncedSize.Store(s.size)
	if old != nil {
		old.Close()
		os.Remove(s.walPath(oldGen))
	}
	return nil
}

// publish writes data to a temp file and atomically renames it to path.
func (s *FileStorage) publish(f *os.File, data []byte, path string) error {
	_, err := f.Write(data)
	if err == nil {
		err = s.sync(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return s.syncDir()
}

func (s *FileStorage) syncDir() error {
	if s.noSync || runtime.GOOS == "windows" {
		return nil // NTFS journals renames; directories cannot be fsynced
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriteSnapshot implements Storage.
func (s *FileStorage) WriteSnapshot(meta SnapshotMeta, data []byte) error {
	hdr, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	body := appendBytes(nil, hdr)
	crc := crc32.Update(crc32.Checksum(body, crcTable), crcTable, data)
	out := make([]byte, 0, len(snapMagic)+4+len(body)+len(data))
	out = append(out, snapMagic...)
	out = binary.LittleEndian.AppendUint32(out, crc)
	out = append(append(out, body...), data...)

	f, err := os.CreateTemp(s.dir, "snap-*.tmp")
	if err != nil {
		return err
	}
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.publish(f, out, s.snapPath(meta))
}

// ReadSnapshot implements Storage.
func (s *FileStorage) ReadSnapshot(meta SnapshotMeta) ([]byte, error) {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	path := s.snapPath(meta)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	n := len(snapMagic)
	if len(b) < n+4 || string(b[:n]) != snapMagic ||
		crc32.Checksum(b[n+4:], crcTable) != binary.LittleEndian.Uint32(b[n:]) {
		return nil, fmt.Errorf("%w: %s", ErrCorrupt, filepath.Base(path))
	}
	d := &decoder{b: b[n+4:]}
	hl := d.uvarint()
	if d.bad || hl > uint64(len(d.b)) {
		return nil, fmt.Errorf("%w: %s", ErrCorrupt, filepath.Base(path))
	}
	return d.b[hl:], nil
}

// Close releases the WAL. It deliberately does not flush: anything not yet
// synced was never acknowledged.
func (s *FileStorage) Close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
