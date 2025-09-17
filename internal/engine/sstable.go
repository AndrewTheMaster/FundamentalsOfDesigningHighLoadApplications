package engine

import (
	"io"

	"lsmdb/pkg/iterator"
	"lsmdb/pkg/types"
)

// SSTable represents an immutable, sorted on-disk table.
type SSTable interface {
	ID() uint64
	ApproximateSize() int64
	NewIterator() (iterator.Iterator, error)
	Contains(key types.Key) (bool, error)
	Close() error
}

// TableBuilder creates an SSTable from a sorted stream of key-values.
type TableBuilder interface {
	Add(key types.Key, value types.Value) error
	Finish() (SSTable, error)
	Discard() error
}

// TableReader reads SSTable contents directly, e.g., for compaction.
type TableReader interface {
	Open(path string) (SSTable, error)
	NewBuilder(w io.Writer) (TableBuilder, error)
} 