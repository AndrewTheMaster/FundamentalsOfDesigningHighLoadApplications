package db

import (
	"context"

	"lsmdb/pkg/batch"
	"lsmdb/pkg/snapshot"
	"lsmdb/pkg/types"
)

// OpenOptions define optional open-time behavior.
type OpenOptions struct {
	ReadOnly bool
}

// ReadOptions define per-read behavior.
type ReadOptions struct {
	Snapshot snapshot.Snapshot
	VerifyChecksums bool
}

// WriteOptions define per-write behavior.
type WriteOptions struct {
	Sync bool
	DisableWAL bool
}

// SearchOptions define search behavior.
type SearchOptions struct {
	ReadOptions
	Limit int // 0 = no limit
	Reverse bool // false = ascending order
}

// SearchResult represents a single key-value pair from search.
type SearchResult struct {
	Key   types.Key
	Value types.Value
}

// SearchCallback is called for each result during search.
type SearchCallback func(SearchResult) error

// DB is the public key-value API.
type DB interface {
	// Basic operations
	Get(ctx context.Context, key types.Key, opts ReadOptions) (types.Value, error)
	Put(ctx context.Context, key types.Key, value types.Value, opts WriteOptions) error
	Delete(ctx context.Context, key types.Key, opts WriteOptions) error
	Write(ctx context.Context, wb batch.WriteBatch, opts WriteOptions) error

	// High-level search operations
	Search(ctx context.Context, start, end types.Key, opts SearchOptions, callback SearchCallback) error
	SearchPrefix(ctx context.Context, prefix types.Key, opts SearchOptions, callback SearchCallback) error
	SearchRange(ctx context.Context, start, end types.Key, opts SearchOptions, callback SearchCallback) error

	// Snapshots
	NewSnapshot(ctx context.Context) (snapshot.Snapshot, error)

	// Maintenance
	CompactRange(ctx context.Context, start, end types.Key) error
	Flush(ctx context.Context) error
	Close() error
}
