package iterator

import "lsmdb/pkg/types"

// Iterator iterates over a sorted sequence of key-value pairs.
type Iterator interface {
	// Seek moves the iterator to the first key >= target.
	Seek(target types.Key)
	// First moves to the smallest key.
	First()
	// Last moves to the largest key.
	Last()
	// Next advances to the next key.
	Next()
	// Prev moves to the previous key.
	Prev()
	// Valid reports whether the iterator points to a valid entry.
	Valid() bool
	// Key returns the current key.
	Key() types.Key
	// Value returns the current value.
	Value() types.Value
	// Close releases resources.
	Close() error
} 