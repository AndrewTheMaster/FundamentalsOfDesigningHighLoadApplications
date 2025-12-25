package raftadapter

import (
	"lsmdb/pkg/config"

	"go.etcd.io/etcd/raft/v3"
)

func toRaftConfig(cfg *config.RaftConfig) *raft.Config {
	maxInflight := cfg.MaxInflightMsgs
	if maxInflight <= 0 {
		maxInflight = 256
	}

	maxSize := cfg.MaxSizePerMsg
	if maxSize <= 0 {
		maxSize = 1024 * 1024 // 1MB
	}

	return &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		MaxInflightMsgs: maxInflight,
		MaxSizePerMsg:   maxSize,

		CheckQuorum: true,
		PreVote:     true,
	}
}
