package cluster

import "fmt"

func ShardFromRing(r *HashRing, key string) (ShardID, error) {
	nodeName, ok := r.GetNode(key)
	if !ok {
		return 0, fmt.Errorf("ring empty")
	}
	// nodeName = "shard-7"
	var id int
	_, err := fmt.Sscanf(nodeName, "shard-%d", &id)
	if err != nil {
		return 0, fmt.Errorf("parse shard from %q: %w", nodeName, err)
	}
	return ShardID(id), nil
}
