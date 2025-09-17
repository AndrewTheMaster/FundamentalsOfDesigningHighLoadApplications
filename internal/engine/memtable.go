package engine

import "lsmdb/pkg/types"

// MutationType enumerates mutation kinds.
type MutationType int

const (
	MutationTypePut MutationType = iota
	MutationTypeDelete
)

// Mutation represents a single operation applied to a key.
type Mutation struct {
	Type   MutationType
	Key    types.Key
	Value  types.Value
	SeqNum types.SequenceNumber
}

// Memtable is an in-memory, sorted write buffer supporting inserts and lookups.
type Memtable interface {
	Upsert(m Mutation) error
	Get(key types.Key, seq types.SequenceNumber) (types.Value, bool, error)
	ApproximateSize() int64
	Iterator() MemtableIterator
}

// MemtableIterator iterates over memtable entries in key order.
type MemtableIterator interface {
	First()
	Next()
	Valid() bool
	Key() types.Key
	Value() types.Value
	Close() error
} 