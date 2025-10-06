package store

import (
	"fmt"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistance"
	"sync"
	"time"
)

type iTimeProvider interface {
	Now() time.Time
}

type Store struct {
	mu           sync.RWMutex
	mt           *memtable.Memtable
	tp           iTimeProvider
	levelManager *persistance.LevelManager
	manifest     *persistance.Manifest
	seqNum       uint64
	dataDir      string
}

func New(dataDir string, tp iTimeProvider) *Store {
	// Create WAL
	wal, _ := memtable.NewWAL(dataDir)

	// Create sorted set
	sortedSet := memtable.NewSortedSet()

	// Create memtable
	memtable := memtable.New(1000, sortedSet, wal) // 1000 byte threshold

	// Create level manager
	levelManager := persistance.NewLevelManager(dataDir)

	// Create manifest
	manifest := persistance.NewManifest(dataDir)

	store := &Store{
		mt:           memtable,
		tp:           tp,
		levelManager: levelManager,
		manifest:     manifest,
		seqNum:       1,
		dataDir:      dataDir,
	}

	// Load manifest
	if err := manifest.Load(); err != nil {
		// If manifest doesn't exist, that's OK for new database
		fmt.Printf("Warning: Could not load manifest: %v\n", err)
	}

	// Replay WAL to restore memtable state
	if err := memtable.ReplayFromWAL(); err != nil {
		// If WAL doesn't exist, that's OK for new database
		fmt.Printf("Warning: Could not replay WAL: %v\n", err)
	}

	return store
}

func (s *Store) PutString(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Encode operation and value type into metadata
	meta := encodeMetadata(insertOp, vTypeString)

	// Convert to bytes
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	// Try to upsert into memtable
	_, err := s.mt.Upsert(keyBytes, valueBytes, s.seqNum, meta)
	if err != nil {
		if err == memtable.ErrMemTableOverload {
			// Flush memtable to SSTable
			if err := s.flushMemtable(); err != nil {
				return fmt.Errorf("failed to flush memtable: %w", err)
			}

			// Retry the operation
			_, err = s.mt.Upsert(keyBytes, valueBytes, s.seqNum, meta)
			if err != nil {
				return fmt.Errorf("failed to upsert after flush: %w", err)
			}
		} else {
			return fmt.Errorf("failed to upsert: %w", err)
		}
	}

	s.seqNum++
	return nil
}

func (s *Store) GetString(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keyBytes := []byte(key)

	// First check memtable
	item := s.mt.Get(keyBytes)
	if item != nil {
		// Decode metadata
		op, valType, _ := decodeMetadata(item.Meta)
		if op == deleteOp {
			return "", false, nil // Deleted
		}

		if valType == vTypeString {
			// Check if this is a tombstone (empty value)
			if len(item.Value) == 0 {
				return "", false, nil // Deleted
			}
			return string(item.Value), true, nil
		}

		return "", false, fmt.Errorf("value is not a string")
	}

	// Search in LSM-tree levels
	sstableItem, err := s.levelManager.Get(keyBytes)
	if err != nil {
		return "", false, fmt.Errorf("failed to get from LSM-tree: %w", err)
	}

	if sstableItem == nil {
		return "", false, nil
	}

	// Decode metadata from LSM-tree
	op, valType, _ := decodeMetadata(sstableItem.Meta)
	if op == deleteOp {
		return "", false, nil // Deleted
	}

	if valType == vTypeString {
		return string(sstableItem.Value), true, nil
	}

	return "", false, fmt.Errorf("value is not a string")
}

func (s *Store) DeleteString(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyBytes := []byte(key)

	// Use memtable delete method
	err := s.mt.Delete(keyBytes, s.seqNum)
	if err != nil {
		if err == memtable.ErrMemTableOverload {
			// Flush memtable first
			if err := s.flushMemtable(); err != nil {
				return fmt.Errorf("failed to flush memtable: %w", err)
			}

			// Retry delete
			err = s.mt.Delete(keyBytes, s.seqNum)
			if err != nil {
				return fmt.Errorf("failed to delete after flush: %w", err)
			}
		} else {
			return fmt.Errorf("failed to delete: %w", err)
		}
	}

	s.seqNum++
	return nil
}

// flushMemtable saves memtable data to SSTable and clears it
func (s *Store) flushMemtable() error {
	// Get snapshot of memtable
	snapshot := s.mt.Snapshot()

	if len(snapshot) == 0 {
		return nil
	}

	// Create SSTable from memtable data
	tableID := s.manifest.GetNextTableID()
	filePath := fmt.Sprintf("%s/L0_%d.sst", s.dataDir, tableID)

	// Create bloom filter
	bloom := persistance.NewBloomFilter(uint32(len(snapshot)), 0.01)

	// Create cache
	cache := persistance.NewBlockCache(100)

	// Create SSTable
	sstable := persistance.NewSSTable(filePath, bloom, cache)

	// Convert memtable items to SSTable items
	sstableItems := make([]persistance.SSTableItem, len(snapshot))
	for i, item := range snapshot {
		sstableItems[i] = persistance.SSTableItem{
			Key:   item.Key,
			Value: item.Value,
			Meta:  item.Meta,
		}
	}

	// Write data to SSTable
	if err := s.levelManager.WriteSSTableData(sstable, sstableItems); err != nil {
		return fmt.Errorf("failed to write SSTable data: %w", err)
	}

	// Open the table
	if err := sstable.Open(); err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}

	// Add to level manager (L0)
	if err := s.levelManager.AddSSTable(sstable, 0); err != nil {
		return fmt.Errorf("failed to add SSTable to level manager: %w", err)
	}

	// Add to manifest
	if err := s.manifest.AddTable(tableID, filePath, 0, sstable.ApproximateSize()); err != nil {
		return fmt.Errorf("failed to add table to manifest: %w", err)
	}

	// Clear memtable
	if err := s.mt.Flush(); err != nil {
		return fmt.Errorf("failed to flush memtable: %w", err)
	}

	return nil
}

// encodeMetadata encodes operation and value type into metadata
func encodeMetadata(op operation, valType valType) uint64 {
	return uint64(op) | (uint64(valType) << 8)
}

// decodeMetadata decodes metadata into operation, value type, and data
func decodeMetadata(meta uint64) (operation, valType, []byte) {
	op := operation(meta & 0xFF)
	valType := valType((meta >> 8) & 0xFF)
	return op, valType, nil // Data is stored separately in Value field
}
