package raftadapter

import (
	"lsmdb/pkg/config"

	"go.etcd.io/etcd/raft/v3"
)

func toRaftConfig(c *config.RaftConfig) *raft.Config {
	return &raft.Config{
		ID:                        c.ID,
		ElectionTick:              c.ElectionTick,
		HeartbeatTick:             c.HeartbeatTick,
		MaxSizePerMsg:             c.MaxSizePerMsg,
		MaxCommittedSizePerReady:  c.MaxCommittedSizePerReady,
		MaxUncommittedEntriesSize: c.MaxUncommittedEntriesSize,
		MaxInflightMsgs:           c.MaxInflightMsgs,
		CheckQuorum:               c.CheckQuorum,
		PreVote:                   c.PreVote,
	}
}
