package persistence

import (
	"sync"
	"time"
)

// BlockCacheImpl implements a simple LRU block cache
type BlockCacheImpl struct {
	mu       sync.RWMutex
	capacity int
	items    map[string]*cacheItem
	head     *cacheItem
	tail     *cacheItem
}

type cacheItem struct {
	key      string
	value    []byte
	lastUsed time.Time
	prev     *cacheItem
	next     *cacheItem
}

// NewBlockCache creates a new block cache
func NewBlockCache(capacity int) BlockCache {
	return &BlockCacheImpl{
		capacity: capacity,
		items:    make(map[string]*cacheItem),
	}
}

// Get retrieves a value from the cache
func (bc *BlockCacheImpl) Get(key string) ([]byte, bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	item, found := bc.items[key]
	if !found {
		return nil, false
	}

	// Update last used time and move to head
	item.lastUsed = time.Now()
	bc.moveToHead(item)

	return item.value, true
}

// Set stores a value in the cache
func (bc *BlockCacheImpl) Set(key string, value []byte) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Check if item already exists
	if item, found := bc.items[key]; found {
		item.value = value
		item.lastUsed = time.Now()
		bc.moveToHead(item)
		return
	}

	// Create new item
	item := &cacheItem{
		key:      key,
		value:    value,
		lastUsed: time.Now(),
	}

	// Add to head
	bc.addToHead(item)
	bc.items[key] = item

	// Evict if over capacity
	if len(bc.items) > bc.capacity {
		bc.evictLRU()
	}
}

// moveToHead moves an item to the head of the list
func (bc *BlockCacheImpl) moveToHead(item *cacheItem) {
	if item == bc.head {
		return
	}

	// Remove from current position
	if item.prev != nil {
		item.prev.next = item.next
	}
	if item.next != nil {
		item.next.prev = item.prev
	}
	if item == bc.tail {
		bc.tail = item.prev
	}

	// Add to head
	bc.addToHead(item)
}

// addToHead adds an item to the head of the list
func (bc *BlockCacheImpl) addToHead(item *cacheItem) {
	item.prev = nil
	item.next = bc.head

	if bc.head != nil {
		bc.head.prev = item
	}

	bc.head = item

	if bc.tail == nil {
		bc.tail = item
	}
}

// evictLRU removes the least recently used item
func (bc *BlockCacheImpl) evictLRU() {
	if bc.tail == nil {
		return
	}

	// Remove from map
	delete(bc.items, bc.tail.key)

	// Remove from list
	if bc.tail.prev != nil {
		bc.tail.prev.next = nil
	} else {
		bc.head = nil
	}

	bc.tail = bc.tail.prev
}
