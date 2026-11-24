package store

import "errors"

var (
	// ErrStaleSequence means a newer request from the same client has already
	// been applied, so this one was not executed.
	ErrStaleSequence = errors.New("store: sequence number is older than the client's latest request")
	// ErrVersionMismatch means a conditional write found a different version.
	ErrVersionMismatch = errors.New("store: version mismatch")
)

// Item is a value and the log index of the write that produced it.
type Item struct {
	Value   string
	Version uint64
}

// Result describes an applied write.
type Result struct {
	Version uint64 // version of the key after the write; 0 if it does not exist
	Existed bool   // whether the key existed before
}

// WriteOptions modify a Set or Delete.
type WriteOptions struct {
	// ClientID and Seq make retries safe: within a session, each Seq takes
	// effect at most once. A client must use increasing numbers and wait for
	// each request to finish before sending the next.
	ClientID string
	Seq      uint64
	// IfVersion, when set, applies the write only if the key is currently at
	// that version (0 means "does not exist").
	IfVersion *uint64
}
