package persistence

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"lsmdb/pkg/config"
	"math"
	"os"
	"sync"
)

// LevelManager manages the LSM-tree levels
type LevelManager struct {
	mu       sync.RWMutex
	cfg      *config.PersistenceConfig
	levels   []Level
	manifest *Manifest
}

// Level represents a single level in the LSM-tree
type Level struct {
	LevelNum int
	Tables   []*SSTable
	MaxSize  int64
}

// NewLevelManager creates a new level manager
func NewLevelManager(config config.PersistenceConfig) *LevelManager {
	lm := &LevelManager{
		cfg:      &config,
		levels:   make([]Level, 0),
		manifest: NewManifest(config.RootPath),
	}

	// Load existing SSTables from manifest
	lm.loadSSTablesFromManifest()

	return lm
}

// AddSSTable adds an SSTable to the appropriate level
func (lm *LevelManager) AddSSTable(sstable *SSTable, level int) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Ensure we have enough levels
	for len(lm.levels) <= level {
		lm.levels = append(lm.levels, Level{
			LevelNum: len(lm.levels),
			Tables:   []*SSTable{},
			MaxSize:  int64(lm.cfg.SSTable.SizeMultiplier * (1 << (len(lm.levels) * 2))), // 10MB, 40MB, 160MB, etc.
		})
	}

	// Add to level
	lm.levels[level].Tables = append(lm.levels[level].Tables, sstable)

	// TODO compact

	return nil
}

// loadSSTablesFromManifest loads existing SSTables from manifest
func (lm *LevelManager) loadSSTablesFromManifest() {
	// Load manifest
	if err := lm.manifest.Load(); err != nil {
		// If manifest doesn't exist, that's OK for new database
		return
	}

	// Get all tables from manifest
	tablesByLevel := lm.manifest.GetAllTables()

	for level, tables := range tablesByLevel {
		for _, table := range tables {
			// Create SSTable
			bloom := NewBloomFilter(1000, lm.cfg.BloomFilter.FPRate) // TODO replace with correct
			cache := NewBlockCache(lm.cfg.Cache.Capacity)
			sstable := NewSSTable(table.FilePath, bloom, cache)

			// Open the table
			if err := sstable.Open(); err != nil {
				// Skip invalid tables
				continue
			}

			// Add to appropriate level; log error if it fails
			if err := lm.AddSSTable(sstable, level); err != nil {
				slog.Error("failed to add SSTable from manifest", "level", level, "path", table.FilePath, "error", err)
				// Close and remove the table file to avoid leaking resources
				if cerr := sstable.Close(); cerr != nil {
					slog.Warn("failed to close SSTable after AddSSTable error", "error", cerr)
				}
				if rerr := os.Remove(table.FilePath); rerr != nil {
					slog.Warn("failed to remove SSTable file after AddSSTable error", "path", table.FilePath, "error", rerr)
				}
				continue
			}
		}
	}
}

// Get retrieves a value by key from all levels
func (lm *LevelManager) Get(key []byte) (*SSTableItem, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Search from newest to oldest (L0 to Ln)
	for level := 0; level < len(lm.levels); level++ {
		// Search in reverse order (newest first)
		for i := len(lm.levels[level].Tables) - 1; i >= 0; i-- {
			table := lm.levels[level].Tables[i]

			// Check bloom filter first
			if table.bloom != nil && !table.bloom.MayContain(key) {
				continue
			}

			// Try to get from this table
			item, err := table.Get(key)
			if err != nil {
				// If key not found, continue to next table
				if err.Error() == "key not found" {
					continue
				}
				return nil, fmt.Errorf("failed to get from table: %w", err)
			}

			if item != nil {
				return item, nil
			}
		}
	}

	return nil, nil
}

func (lm *LevelManager) WriteSSTableData(sstable *SSTable, items []SSTableItem) error {
	const (
		sizeFieldSize = 4
		seqNumSize    = 8
		metaSize      = 8
	)

	file, err := os.Create(sstable.filePath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			slog.Warn("failed to close sstable file", "error", cerr)
		}
	}()

	// Write data blocks
	blockIndex := make([]IndexEntry, 0)
	blockOffset := int64(0)
	blockNum := 0

	for _, item := range items {
		// Add to bloom filter
		if sstable.bloom != nil {
			sstable.bloom.Add(item.Key)
		}

		// Check sizes before casting
		if len(item.Key) > math.MaxUint32 {
			return fmt.Errorf("key too large: %d", len(item.Key))
		}
		if len(item.Value) > math.MaxUint32 {
			return fmt.Errorf("value too large: %d", len(item.Value))
		}

		// Write key length
		if err := binary.Write(file, binary.LittleEndian, uint32(len(item.Key))); err != nil {
			return err
		}

		// Write key
		if _, err := file.Write(item.Key); err != nil {
			return err
		}

		// Write value length
		if err := binary.Write(file, binary.LittleEndian, uint32(len(item.Value))); err != nil {
			return err
		}

		// Write value
		if _, err := file.Write(item.Value); err != nil {
			return err
		}

		// Write sequence number (placeholder)
		if err := binary.Write(file, binary.LittleEndian, uint64(0)); err != nil {
			return err
		}

		// Write metadata
		if err := binary.Write(file, binary.LittleEndian, item.Meta); err != nil {
			return err
		}

		// Add to block index
		blockSz := sizeFieldSize + len(item.Key) + sizeFieldSize + len(item.Value) + seqNumSize + metaSize
		blockIndex = append(blockIndex, IndexEntry{
			Key:         item.Key,
			BlockOffset: blockOffset,
			BlockSize:   blockSz,
			BlockInd:    blockNum,
		})

		blockOffset += int64(blockSz)
		blockNum++
	}

	// Write block index
	indexData := make([]byte, 0)

	for _, entry := range blockIndex {
		// Write key length
		indexData = append(indexData, make([]byte, 4)...)
		binary.LittleEndian.PutUint32(indexData[len(indexData)-4:], uint32(len(entry.Key)))

		// Write key
		indexData = append(indexData, entry.Key...)

		// Write block offset
		indexData = append(indexData, make([]byte, 8)...)
		if entry.BlockOffset < 0 {
			return fmt.Errorf("negative block offset: %d", entry.BlockOffset)
		}
		binary.LittleEndian.PutUint64(indexData[len(indexData)-8:], uint64(entry.BlockOffset))

		// Write block size
		indexData = append(indexData, make([]byte, 4)...)
		if entry.BlockSize < 0 {
			return fmt.Errorf("negative block size: %d", entry.BlockSize)
		}
		binary.LittleEndian.PutUint32(indexData[len(indexData)-4:], uint32(entry.BlockSize))

		// Write block index
		indexData = append(indexData, make([]byte, 4)...)
		binary.LittleEndian.PutUint32(indexData[len(indexData)-4:], uint32(entry.BlockInd))
	}

	// Write index to file
	if _, err := file.Write(indexData); err != nil {
		return err
	}

	// Write index size
	if len(indexData) > math.MaxUint32 {
		return fmt.Errorf("index too large: %d", len(indexData))
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(len(indexData))); err != nil {
		return err
	}

	return nil
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   []byte
	Value []byte
}
