package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// реализует consistent hashing с виртуальными нодами.
type HashRing struct {
	replicas int
	nodes    []int          // отсортированные хэши
	nodeMap  map[int]string // хэш -> имя ноды
	mu       sync.RWMutex
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		nodeMap:  make(map[int]string),
	}
}

func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.replicas; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s#%d", node, i))))
		h.nodes = append(h.nodes, hash)
		h.nodeMap[hash] = node
	}
	sort.Ints(h.nodes)
}

func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	filtered := h.nodes[:0]
	for _, hash := range h.nodes {
		if h.nodeMap[hash] != node {
			filtered = append(filtered, hash)
		} else {
			delete(h.nodeMap, hash)
		}
	}
	h.nodes = filtered
}

// имя ноды по ключу
func (h *HashRing) GetNode(key string) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.nodes) == 0 {
		return ""
	}

	hash := int(crc32.ChecksumIEEE([]byte(key)))
	idx := sort.Search(len(h.nodes), func(i int) bool { return h.nodes[i] >= hash })
	if idx == len(h.nodes) {
		idx = 0
	}
	return h.nodeMap[h.nodes[idx]]
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
