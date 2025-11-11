package cluster

import (
	"os"
	"strings"
)

type NodeID string

type ClusterConfig struct {
	Local NodeID
	Peers []NodeID
}

func FromEnv() ClusterConfig {
	local := NodeID(os.Getenv("LSMDB_NODE_ADDR")) // "node1:8080"
	raw := os.Getenv("LSMDB_PEERS")               // "node1:8080,node2:8080,node3:8080"
	var peers []NodeID
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			peers = append(peers, NodeID(p))
		}
	}
	return ClusterConfig{Local: local, Peers: peers}
}
