package cluster

import "lsmdb/pkg/types"

// NodeInfo holds metadata about a node.
type NodeInfo struct {
	ID     types.NodeID
	Addr   string
	DC     string
	Rack   string
}

// Membership provides a view of cluster nodes.
type Membership interface {
	LocalNode() NodeInfo
	AllNodes() []NodeInfo
	GetNode(id types.NodeID) (NodeInfo, bool)
}

// Placement maps shards to nodes.
type Placement interface {
	Owners(shard types.ShardID) []types.NodeID
	ResponsibleShards(node types.NodeID) []types.ShardID
} 