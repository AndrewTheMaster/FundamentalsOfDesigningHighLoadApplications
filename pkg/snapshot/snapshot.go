package snapshot

import "lsmdb/pkg/types"

// Snapshot provides a consistent view of the database at a given sequence.
type Snapshot interface {
	// Sequence returns the read sequence number.
	Sequence() types.SequenceNumber
	// Close releases the snapshot.
	Close() error
} 