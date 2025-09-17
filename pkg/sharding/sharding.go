package sharding

import "lsmdb/pkg/types"

// KeyHasher deterministically maps keys to shard IDs.
type KeyHasher interface {
	ShardForKey(key types.Key, totalShards int) types.ShardID
}

// Router decides where requests for a key should go.
type Router interface {
	// Route returns the shard and the preferred owner node IDs in priority order.
	Route(key types.Key) (types.ShardID, []types.NodeID)
} 