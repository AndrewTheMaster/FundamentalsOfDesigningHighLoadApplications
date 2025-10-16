package memtable

import (
	"errors"
	"fmt"
	"sync"
)

// Operation types
type operation uint8

const (
	putOp    operation = 1
	deleteOp operation = 2
)

// Value types
type valType uint8

const (
	vTypeString valType = 1
	vTypeInt    valType = 2
	vTypeFloat  valType = 3
)

// encodeMetadata encodes operation and value type into metadata
func encodeMetadata(op operation, valType valType) uint64 {
	return uint64(op) | (uint64(valType) << 8)
}

var (
	ErrMemTableOverload = errors.New("memtable is overloaded")
)

type iSortedSet interface {
	Sorted() []*Item
	Has(item *Item) bool
	InsertOrReplace(item *Item) *Item
	Get(key []byte) *Item
	Len() int
	Iterator() func(item *Item) bool
	Clear()
}

type Memtable struct {
	mu        sync.RWMutex
	threshold int
	size      int
	ss        iSortedSet
	wal       *WAL
	seqNum    uint64
}

func New(threshold int, sortedSet iSortedSet, wal *WAL) *Memtable {
	return &Memtable{
		threshold: threshold,
		ss:        sortedSet,
		size:      0,
		wal:       wal,
		seqNum:    1,
	}
}

func (mt *Memtable) Snapshot() []*Item {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.ss.Sorted()
}

func (mt *Memtable) Upsert(key, value []byte, seqN, meta uint64) (bool, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Write to WAL first
	if mt.wal != nil {
		walEntry := WALEntry{
			SeqNum:    seqN,
			Key:       key,
			Value:     value,
			Tombstone: false,
		}
		if err := mt.wal.Append(walEntry, true); err != nil {
			return false, fmt.Errorf("failed to write to WAL: %w", err)
		}
	}

	// Update memtable
	found := mt.ss.InsertOrReplace(&Item{
		Key:   key,
		Value: value,
		SeqN:  seqN,
		Meta:  meta,
	})

	mt.size += len(key) + len(value) + 8 // 8 = sizeof(seqNumber) + sizeof(meta)
	if mt.size >= mt.threshold {
		return false, ErrMemTableOverload
	}

	return found != nil, nil
}

func (mt *Memtable) Get(key []byte) *Item {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.ss.Get(key)
}

func (mt *Memtable) Delete(key []byte, seqN uint64) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Write delete to WAL
	if mt.wal != nil {
		walEntry := WALEntry{
			SeqNum:    seqN,
			Key:       key,
			Value:     nil,
			Tombstone: true,
		}
		if err := mt.wal.Append(walEntry, true); err != nil {
			return fmt.Errorf("failed to write delete to WAL: %w", err)
		}
	}

	// Insert tombstone
	mt.ss.InsertOrReplace(&Item{
		Key:   key,
		Value: nil,
		SeqN:  seqN,
		Meta:  encodeMetadata(deleteOp, vTypeString),
	})

	mt.size += len(key) + 8
	if mt.size >= mt.threshold {
		return ErrMemTableOverload
	}

	return nil
}

func (mt *Memtable) Flush() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Clear memtable
	mt.ss.Clear()
	mt.size = 0

	// Close and recreate WAL
	if mt.wal != nil {
		if err := mt.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}

	return nil
}

func (mt *Memtable) ReplayFromWAL() error {
	if mt.wal == nil {
		return nil
	}

	return mt.wal.Replay(func(entry WALEntry) error {
		// Update sequence number
		if entry.SeqNum > mt.seqNum {
			mt.seqNum = entry.SeqNum
		}

		if entry.Tombstone {
			// Handle delete - encode delete operation with string type
			mt.ss.InsertOrReplace(&Item{
				Key:   entry.Key,
				Value: nil,
				SeqN:  entry.SeqNum,
				Meta:  encodeMetadata(deleteOp, vTypeString),
			})
			mt.size += len(entry.Key) + 8
		} else {
			// Handle put - encode put operation with string type
			mt.ss.InsertOrReplace(&Item{
				Key:   entry.Key,
				Value: entry.Value,
				SeqN:  entry.SeqNum,
				Meta:  encodeMetadata(putOp, vTypeString),
			})
			mt.size += len(entry.Key) + len(entry.Value) + 8
		}
		return nil
	})
}

func (mt *Memtable) GetNextSeqNum() uint64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.seqNum++
	return mt.seqNum
}

func (mt *Memtable) ApproximateSize() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

func Replace(ss iSortedSet) {
	// TODO: must atomically return snapshot + replace underlying container
	// TODO: by using atomic.Pointer for iSortedSet
}
