package cluster

type Placement struct {
	Nodes             []string
	ReplicationFactor int
}

func (p *Placement) Owners(shardID ShardID) []string {
	res := make([]string, 0, p.ReplicationFactor)
	if len(p.Nodes) == 0 || p.ReplicationFactor == 0 {
		return res
	}
	start := int(shardID) % len(p.Nodes)
	for i := 0; i < p.ReplicationFactor; i++ {
		idx := (start + i) % len(p.Nodes)
		res = append(res, p.Nodes[idx])
	}
	return res
}
