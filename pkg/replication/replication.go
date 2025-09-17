package replication

import (
	"context"

	"lsmdb/pkg/types"
)

// LogEntry is a replicated operation entry.
type LogEntry struct {
	Index types.LogIndex
	Term  types.Term
	Data  []byte
}

// Log abstracts a replicated log storage.
type Log interface {
	Append(ctx context.Context, entries []LogEntry) (types.LogIndex, error)
	LastIndex(ctx context.Context) (types.LogIndex, error)
	Entries(ctx context.Context, from types.LogIndex, max int) ([]LogEntry, error)
}

// Replicator ships log entries to followers.
type Replicator interface {
	Replicate(ctx context.Context, to types.NodeID, entries []LogEntry) error
} 