package config

import (
	"time"
)

// Config - корневая структура конфигурации приложения
// yaml и validate теги для парсинга и валидации

type Config struct {
	Logger LoggerConfig `yaml:"logger" validate:"required"`
	Server ServerConfig `yaml:"http-server" validate:"required"`
	Raft   RaftConfig   `yaml:"raft" validate:"required"`
	DB     `yaml:"db" validate:"required"`
}

type RaftConfig struct {
	ID                        uint64 `yaml:"id" validate:"required,min=1"`
	ElectionTick              int    `yaml:"election_tick,omitempty"`
	HeartbeatTick             int    `yaml:"heartbeat_tick,omitempty"`
	Applied                   uint64 `yaml:"applied,omitempty"`
	MaxSizePerMsg             uint64 `yaml:"max_size_per_msg,omitempty"`
	MaxCommittedSizePerReady  uint64 `yaml:"max_committed_size_per_ready,omitempty"`
	MaxUncommittedEntriesSize uint64 `yaml:"max_uncommitted_entries_size,omitempty"`
	MaxInflightMsgs           int    `yaml:"max_inflight_msgs,omitempty"`
	CheckQuorum               bool   `yaml:"check_quorum,omitempty"`
	PreVote                   bool   `yaml:"pre_vote,omitempty"`

	Peers []RaftPeerConfig `yaml:"peers" validate:"required,dive"`
}

type RaftPeerConfig struct {
	ID      uint64 `yaml:"id" validate:"required,min=1"`
	Address string `yaml:"address"`
}

type ServerConfig struct {
	Port              int       `yaml:"port" validate:"required,min=1,max=65535"`
	ReadHeaderTimeout time.Time `yaml:"read_header_timeout" validate:"required"`
}

type DB struct {
	Memtable    MemtableConfig    `yaml:"memtable" validate:"required"`
	Persistence PersistenceConfig `yaml:"persistence" validate:"required"`
}

type MemtableConfig struct {
	FlushThresholdBytes int `yaml:"flush_threshold" validate:"required,min=1"`
	FlushChanBuffSize   int `yaml:"flush_chan_buff_size" validate:"required,min=1"`
	MaxImmTables        int `yaml:"max_imm_tables" validate:"min=0"`
}

type PersistenceConfig struct {
	RootPath    string            `yaml:"path" validate:"required,dir"`
	SSTable     SSTableConfig     `yaml:"sstable" validate:"required"`
	Cache       CacheConfig       `yaml:"cache" validate:"required"`
	BloomFilter BloomFilterConfig `yaml:"bloom_filter" validate:"required"`
}

type SSTableConfig struct {
	SizeMultiplier   int `yaml:"size_multiplier" validate:"required,min=1"`
	CompactThreshold int `yaml:"compact_threshold" validate:"required,min=1"`
}

type CacheConfig struct {
	Capacity int `yaml:"capacity" validate:"required,min=1"`
}

type BloomFilterConfig struct {
	FPRate float64 `yaml:"fp_rate" validate:"required,gt=0,lt=1"`
}

type LoggerConfig struct {
	Level string `yaml:"level" validate:"required,oneof=DEBUG INFO WARN ERROR debug info warn error"`
	JSON  bool   `yaml:"json"`
}

// Default returns a baseline development config.
func Default() Config {
	return Config{
		Logger: LoggerConfig{
			Level: "DEBUG",
			JSON:  false,
		},
		Server: ServerConfig{
			Port: 8080,
		},
		DB: DB{
			Memtable: MemtableConfig{
				FlushThresholdBytes: 1024,
				FlushChanBuffSize:   3,
				MaxImmTables:        3,
			},
			Persistence: PersistenceConfig{
				RootPath: "./data",
				SSTable: SSTableConfig{
					SizeMultiplier:   10,
					CompactThreshold: 4,
				},
				Cache: CacheConfig{
					Capacity: 100,
				},
				BloomFilter: BloomFilterConfig{
					FPRate: 0.01,
				},
			},
		},
	}
}
