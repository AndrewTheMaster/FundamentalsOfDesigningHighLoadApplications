package types

// Key is an immutable byte slice type alias used for clarity.
type Key = []byte

// Value is an immutable byte slice type alias used for clarity.
type Value = []byte

// SequenceNumber represents a monotonically increasing sequence used for MVCC and WAL ordering.
type SequenceNumber uint64

// TimestampMs is a millisecond-precision timestamp for time-based policies.
type TimestampMs int64

// ShardID identifies a logical shard.
type ShardID uint32

// NodeID identifies a node in a cluster.
type NodeID string

// Term and Index are used by consensus/replication components.
type Term uint64

type LogIndex uint64 