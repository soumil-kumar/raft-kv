package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"os"
	"sync"
)

type Persister struct {
	mu       sync.Mutex
	filename string
	file     *os.File
}

func NewPersister(nodeId string) *Persister {
	p := &Persister{
		filename: "raft-data-" + nodeId + ".gob",
	}
	file, err := os.OpenFile(p.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open WAL file: %v", err)
	}
	p.file = file
	return p
}

type WALRecord struct {
	Type     string
	Term     int
	VotedFor string
	Index    int
	Snapshot []byte
	Entries  []LogEntry
}

func (p *Persister) appendRecord(rec WALRecord) {
	if p.file == nil {
		return
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(rec); err != nil {
		log.Printf("WAL encode error: %v", err)
		return
	}
	
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if err := binary.Write(p.file, binary.LittleEndian, uint32(buf.Len())); err != nil {
		log.Printf("WAL write len error: %v", err)
		return
	}
	if _, err := p.file.Write(buf.Bytes()); err != nil {
		log.Printf("WAL write data error: %v", err)
	}
}

func (p *Persister) Sync() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		p.file.Sync()
	}
}

func (p *Persister) SaveMetadata(term int, votedFor string) {
	p.appendRecord(WALRecord{
		Type:     "meta",
		Term:     term,
		VotedFor: votedFor,
	})
	p.Sync() // fsync metadata for Raft safety
}

func (p *Persister) AppendLogs(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	p.appendRecord(WALRecord{
		Type:    "append",
		Entries: entries,
	})
}

func (p *Persister) TruncateLog(index int) {
	p.appendRecord(WALRecord{
		Type:  "truncate",
		Index: index,
	})
}

func (p *Persister) SaveStateAndSnapshot(term int, votedFor string, lastIncludedIndex int, lastIncludedTerm int, snapshot []byte, tailLogs []LogEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmpFile := p.filename + ".tmp"
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Failed to create snapshot tmp file: %v", err)
	}

	writeRec := func(rec WALRecord) {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(rec); err != nil {
			log.Fatalf("Encode error during snapshot: %v", err)
		}
		binary.Write(file, binary.LittleEndian, uint32(buf.Len()))
		file.Write(buf.Bytes())
	}

	writeRec(WALRecord{Type: "meta", Term: term, VotedFor: votedFor})
	if snapshot != nil {
		writeRec(WALRecord{Type: "snapshot", Index: lastIncludedIndex, Term: lastIncludedTerm, Snapshot: snapshot})
	}
	
	if len(tailLogs) > 0 {
		writeRec(WALRecord{Type: "append", Entries: tailLogs})
	}

	file.Sync()
	file.Close()

	if p.file != nil {
		p.file.Close()
	}

	os.Remove(p.filename)
	os.Rename(tmpFile, p.filename)
	
	file, err = os.OpenFile(p.filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to reopen WAL: %v", err)
	}
	p.file = file
}

func (p *Persister) ReadState() (int, string, int, int, []byte, []LogEntry) {
	var term int
	var votedFor string
	var lastIncludedIndex int
	var lastIncludedTerm int
	var snapshot []byte
	var logEntries []LogEntry

	file, err := os.Open(p.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", 0, 0, nil, nil
		}
		log.Printf("Failed to open WAL for reading: %v", err)
		return 0, "", 0, 0, nil, nil
	}
	defer file.Close()

	for {
		var l uint32
		if err := binary.Read(file, binary.LittleEndian, &l); err != nil {
			if err != io.EOF {
				log.Printf("WAL read length error: %v", err)
			}
			break
		}
		
		buf := make([]byte, l)
		if _, err := io.ReadFull(file, buf); err != nil {
			log.Printf("WAL read data error: %v", err)
			break
		}
		
		var rec WALRecord
		dec := gob.NewDecoder(bytes.NewReader(buf))
		if err := dec.Decode(&rec); err != nil {
			log.Printf("WAL decode error: %v", err)
			continue
		}
		
		switch rec.Type {
		case "meta":
			term = rec.Term
			votedFor = rec.VotedFor
		case "append":
			if rec.Entries != nil {
				logEntries = append(logEntries, rec.Entries...)
			}
		case "snapshot":
			lastIncludedIndex = rec.Index
			lastIncludedTerm = rec.Term
			snapshot = rec.Snapshot
			logEntries = nil // reset logs when loading snapshot
		case "truncate":
			if rec.Index >= 0 && rec.Index <= len(logEntries) {
				logEntries = logEntries[:rec.Index]
			}
		}
	}

	return term, votedFor, lastIncludedIndex, lastIncludedTerm, snapshot, logEntries
}

func (p *Persister) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		p.file.Close()
		p.file = nil
	}
}

func (p *Persister) ReadSnapshot() []byte {
	_, _, _, _, snapshot, _ := p.ReadState()
	return snapshot
}
