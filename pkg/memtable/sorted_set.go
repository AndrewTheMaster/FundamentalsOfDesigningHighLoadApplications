package memtable

import (
	"bytes"
	"sort"
	"sync"
)

// sortedSetImpl implements iSortedSet using a slice with binary search
type sortedSetImpl struct {
	mu    sync.RWMutex
	items []*Item
}

// NewSortedSet creates a new sorted set implementation
func NewSortedSet() iSortedSet {
	return &sortedSetImpl{
		items: make([]*Item, 0),
	}
}

// Sorted returns all items in sorted order
func (ss *sortedSetImpl) Sorted() []*Item {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	result := make([]*Item, len(ss.items))
	copy(result, ss.items)
	return result
}

// Has checks if an item exists
func (ss *sortedSetImpl) Has(item *Item) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	
	_, found := ss.findItem(item.Key)
	return found
}

// InsertOrReplace inserts or replaces an item
func (ss *sortedSetImpl) InsertOrReplace(item *Item) *Item {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	
	index, found := ss.findItem(item.Key)
	
	if found {
		// Replace existing item
		oldItem := ss.items[index]
		ss.items[index] = item
		return oldItem
	}
	
	// Insert new item
	ss.items = append(ss.items, nil)
	copy(ss.items[index+1:], ss.items[index:])
	ss.items[index] = item
	return nil
}

// Get retrieves an item by key
func (ss *sortedSetImpl) Get(key []byte) *Item {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	
	index, found := ss.findItem(key)
	if found {
		return ss.items[index]
	}
	return nil
}

// Len returns the number of items
func (ss *sortedSetImpl) Len() int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return len(ss.items)
}

// Iterator returns a function that iterates over items
func (ss *sortedSetImpl) Iterator() func(item *Item) bool {
	ss.mu.RLock()
	items := make([]*Item, len(ss.items))
	copy(items, ss.items)
	ss.mu.RUnlock()
	
	index := 0
	return func(item *Item) bool {
		if index >= len(items) {
			return false
		}
		item = items[index]
		index++
		return true
	}
}

// Clear removes all items from the sorted set
func (ss *sortedSetImpl) Clear() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.items = ss.items[:0] // Reset slice but keep capacity
}

// findItem performs binary search to find an item by key
func (ss *sortedSetImpl) findItem(key []byte) (int, bool) {
	index := sort.Search(len(ss.items), func(i int) bool {
		return bytes.Compare(ss.items[i].Key, key) >= 0
	})
	
	if index < len(ss.items) && bytes.Equal(ss.items[index].Key, key) {
		return index, true
	}
	
	return index, false
}
