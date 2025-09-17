package engine

import "context"

// CompactionPlanner decides which tables to compact.
type CompactionPlanner interface {
	Next() (CompactionTask, bool)
}

// CompactionTask describes a single compaction unit.
type CompactionTask struct {
	InputTableIDs []uint64
	TargetLevel   int
}

// Compactor performs compactions.
type Compactor interface {
	Run(ctx context.Context, task CompactionTask) error
} 