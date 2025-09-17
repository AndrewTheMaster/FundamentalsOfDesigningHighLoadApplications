package config

// Config holds all configuration for the database node.
type Config struct {
	Node     NodeConfig
	Storage  StorageConfig
	Compaction CompactionConfig
	Sharding ShardingConfig
	Replication ReplicationConfig
	Networking NetworkingConfig
}

// NodeConfig describes identity and metadata of the node.
type NodeConfig struct {
	NodeID   string
	DataCenter string
	Rack     string
}

// StorageConfig covers on-disk layout, WAL, and memtable sizing.
type StorageConfig struct {
	DataDir           string
	WALDir            string
	MaxMemtableBytes  int64
	SSTableTargetSizeBytes int64
	BlockSizeBytes    int32
	BloomBitsPerKey   int32
}

// CompactionConfig controls LSM shape and background work.
type CompactionConfig struct {
	MaxLevels           int
	LevelSizeMultipliers []int
	MaxConcurrentCompactions int
}

// ShardingConfig controls partitioning of keyspace.
type ShardingConfig struct {
	NumShards int
	HashSeed  uint64
}

// ReplicationConfig controls write durability and replication.
type ReplicationConfig struct {
	ReplicationFactor int
	QuorumWrite       bool
	QuorumRead        bool
}

// NetworkingConfig covers RPC exposure.
type NetworkingConfig struct {
	ListenAddress string
	GRPCPort      int
	HTTPPort      int
}

// Default returns a baseline development config.
func Default() Config {
	return Config{
		Node: NodeConfig{NodeID: "node-1"},
		Storage: StorageConfig{
			DataDir: "./data",
			WALDir: "./data/wal",
			MaxMemtableBytes: 64 * 1024 * 1024,
			SSTableTargetSizeBytes: 64 * 1024 * 1024,
			BlockSizeBytes: 4096,
			BloomBitsPerKey: 10,
		},
		Compaction: CompactionConfig{
			MaxLevels: 7,
			LevelSizeMultipliers: []int{1, 10, 10, 10, 10, 10, 10},
			MaxConcurrentCompactions: 2,
		},
		Sharding: ShardingConfig{NumShards: 16, HashSeed: 0xC0FFEE},
		Replication: ReplicationConfig{ReplicationFactor: 1, QuorumWrite: false, QuorumRead: false},
		Networking: NetworkingConfig{ListenAddress: "0.0.0.0", GRPCPort: 54051, HTTPPort: 8080},
	}
} 