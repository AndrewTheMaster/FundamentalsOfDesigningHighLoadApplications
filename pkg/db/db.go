package db

import (
	"context"

	"lsmdb/pkg/batch"
	"lsmdb/pkg/iterator"
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

// DB is the public key-value API.
type DB interface {
	Get(ctx context.Context, key types.Key, opts ReadOptions) (types.Value, error)
	Put(ctx context.Context, key types.Key, value types.Value, opts WriteOptions) error
	Delete(ctx context.Context, key types.Key, opts WriteOptions) error
	Write(ctx context.Context, wb batch.WriteBatch, opts WriteOptions) error

	// Iteration
	NewIterator(ctx context.Context, opts ReadOptions) (iterator.Iterator, error)
	NewSnapshot(ctx context.Context) (snapshot.Snapshot, error)

	// Maintenance
	CompactRange(ctx context.Context, start, end types.Key) error
	Flush(ctx context.Context) error
	Close() error
} 