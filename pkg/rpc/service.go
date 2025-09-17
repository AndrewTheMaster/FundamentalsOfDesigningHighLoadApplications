package rpc

import (
	"context"

	"lsmdb/pkg/db"
	"lsmdb/pkg/types"
)

// KVService defines RPC-facing operations.
type KVService interface {
	Get(ctx context.Context, key types.Key) (types.Value, error)
	Put(ctx context.Context, key types.Key, value types.Value) error
	Delete(ctx context.Context, key types.Key) error
}

// AdminService exposes maintenance endpoints.
type AdminService interface {
	CompactRange(ctx context.Context, start, end types.Key) error
	Flush(ctx context.Context) error
}

// Server ties together services and lifecycle.
type Server interface {
	Start() error
	Stop() error
	RegisterKV(db db.DB)
	RegisterAdmin(db db.DB)
} 