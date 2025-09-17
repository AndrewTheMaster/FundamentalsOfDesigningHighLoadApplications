package engine

import "lsmdb/pkg/types"

// WALEntry is a record stored in the write-ahead log.
type WALEntry struct {
	Sequence types.SequenceNumber
	Key      types.Key
	Value    types.Value
	Tombstone bool
}

// WAL provides durable append and replay.
type WAL interface {
	Append(entry WALEntry, sync bool) error
	// Iterate replays entries starting after the provided sequence.
	Iterate(fromExclusive types.SequenceNumber, fn func(WALEntry) error) error
	Close() error
} 