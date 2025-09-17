package consensus

import (
	"context"

	"lsmdb/pkg/replication"
	"lsmdb/pkg/types"
)

// FSM applies committed log entries to the state machine.
type FSM interface {
	Apply(ctx context.Context, entry replication.LogEntry) error
}

// Consensus exposes a minimal API to coordinate replication and leadership.
type Consensus interface {
	Propose(ctx context.Context, data []byte) (types.LogIndex, error)
	IsLeader() bool
	LeaderID() types.NodeID
	ApplyCh() <-chan replication.LogEntry
	Start() error
	Stop() error
} 