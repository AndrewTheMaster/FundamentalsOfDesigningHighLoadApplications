package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type Hash = uint32

// реализует consistent hashing с виртуальными нодами.
type HashRing struct {
	replicas int
	nodes    []Hash          // отсортированные хэши виртуальных нод
	nodeMap  map[Hash]string // hash виртуальной ноды -> имя реальной ноды
	mu       sync.RWMutex
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		nodeMap:  make(map[Hash]string),
	}
}

func hashKey(s string) Hash {
	return crc32.ChecksumIEEE([]byte(s))
}

func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		token := fmt.Sprintf("%s#%d", node, i)
		hash := hashKey(token)
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = node
	}
	sort.Slice(h.nodes, func(i, j int) bool { return h.nodes[i] < h.nodes[j] })
}

func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	filtered := h.nodes[:0]
	for _, hash := range h.nodes {
		if h.nodeMap[hash] == node {
			delete(h.nodeMap, hash)
			continue
		}
		filtered = append(filtered, hash)
	}
	h.nodes = filtered
}

// имя ноды по ключу
func (h *HashRing) GetNode(key string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.nodes) == 0 {
		return "", false
	}
	hv := hashKey(key)
	idx := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i] >= hv })
	if idx == len(h.nodes) {
		idx = 0
	}
	return h.nodeMap[h.nodes[idx]], true
}

// возвращает список уникальных имён нод.
func (h *HashRing) ListNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[string]struct{}{}
	var result []string
	for _, name := range h.nodeMap {
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}
