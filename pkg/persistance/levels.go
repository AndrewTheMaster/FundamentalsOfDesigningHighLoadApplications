package persistance

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// LevelManager manages the LSM-tree levels
type LevelManager struct {
	mu       sync.RWMutex
	dataDir  string
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
func NewLevelManager(dataDir string) *LevelManager {
	lm := &LevelManager{
		dataDir:  dataDir,
		levels:   make([]Level, 0),
		manifest: NewManifest(dataDir),
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
			Tables:   make([]*SSTable, 0),
			MaxSize:  int64(10 * (1 << (len(lm.levels) * 2))), // 10MB, 40MB, 160MB, etc.
		})
	}

	// Add to level
	lm.levels[level].Tables = append(lm.levels[level].Tables, sstable)

	// Check if level needs compaction
	if lm.shouldCompactLevel(level) {
		go lm.compactLevel(level)
	}

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
			bloom := NewBloomFilter(1000, 0.01) // Default values
			cache := NewBlockCache(100)
			sstable := NewSSTable(table.FilePath, bloom, cache)

			// Open the table
			if err := sstable.Open(); err != nil {
				// Skip invalid tables
				continue
			}

			// Add to appropriate level
			lm.AddSSTable(sstable, level)
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

// shouldCompactLevel checks if a level needs compaction
func (lm *LevelManager) shouldCompactLevel(level int) bool {
	if level >= len(lm.levels) {
		return false
	}

	levelData := lm.levels[level]

	// L0: compact if more than 4 tables
	if level == 0 {
		return len(levelData.Tables) > 4
	}

	// Other levels: compact if size exceeds threshold
	totalSize := int64(0)
	for _, table := range levelData.Tables {
		totalSize += table.ApproximateSize()
	}

	return totalSize > levelData.MaxSize
}

// compactLevel compacts a level
func (lm *LevelManager) compactLevel(level int) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if level >= len(lm.levels) {
		return fmt.Errorf("invalid level: %d", level)
	}

	levelData := lm.levels[level]
	if len(levelData.Tables) == 0 {
		return nil
	}

	// For L0, compact all tables
	// For other levels, compact overlapping tables
	tablesToCompact := levelData.Tables
	if level > 0 {
		// Find overlapping tables with next level
		tablesToCompact = lm.findOverlappingTables(level)
	}

	// Create new SSTable from compacted data
	newTable, err := lm.createCompactedTable(tablesToCompact, level+1)
	if err != nil {
		return fmt.Errorf("failed to create compacted table: %w", err)
	}

	// Remove old tables
	lm.removeTables(tablesToCompact)

	// Add new table to next level
	if err := lm.AddSSTable(newTable, level+1); err != nil {
		return fmt.Errorf("failed to add compacted table: %w", err)
	}

	return nil
}

// findOverlappingTables finds tables that overlap with the next level
func (lm *LevelManager) findOverlappingTables(level int) []*SSTable {
	if level+1 >= len(lm.levels) {
		return lm.levels[level].Tables
	}

	// Simple implementation: return all tables in current level
	// In a real implementation, this would find overlapping key ranges
	return lm.levels[level].Tables
}

// createCompactedTable creates a new SSTable from compacted data
func (lm *LevelManager) createCompactedTable(tables []*SSTable, targetLevel int) (*SSTable, error) {
	// Collect all key-value pairs with metadata
	allItems := make([]SSTableItem, 0)

	for _, table := range tables {
		iterator := table.NewIterator()

		for iterator.Valid() {
			allItems = append(allItems, SSTableItem{
				Key:   iterator.Key(),
				Value: iterator.Value(),
				Meta:  iterator.Meta(),
			})
			iterator.Next()
		}
		iterator.Close()
	}

	// Sort by key
	sort.Slice(allItems, func(i, j int) bool {
		return string(allItems[i].Key) < string(allItems[j].Key)
	})

	// Remove duplicates (keep latest)
	deduped := make([]SSTableItem, 0)
	for i, item := range allItems {
		if i == 0 || string(item.Key) != string(allItems[i-1].Key) {
			deduped = append(deduped, item)
		}
	}

	// Create new SSTable
	tableID := time.Now().UnixNano()
	filePath := filepath.Join(lm.dataDir, fmt.Sprintf("L%d_%d.sst", targetLevel, tableID))

	// Create bloom filter
	bloom := NewBloomFilter(uint32(len(deduped)), 0.01)

	// Create cache
	cache := NewBlockCache(100)

	// Create SSTable
	sstable := NewSSTable(filePath, bloom, cache)

	// Write data to file
	if err := lm.writeSSTableData(sstable, deduped); err != nil {
		return nil, fmt.Errorf("failed to write SSTable data: %w", err)
	}

	// Open the table
	if err := sstable.Open(); err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}

	return sstable, nil
}

// WriteSSTableData writes key-value pairs to an SSTable file
func (lm *LevelManager) WriteSSTableData(sstable *SSTable, items []SSTableItem) error {
	return lm.writeSSTableData(sstable, items)
}

func (lm *LevelManager) writeSSTableData(sstable *SSTable, items []SSTableItem) error {
	const (
		sizeFieldSize = 4
		seqNumSize    = 8
		metaSize      = 8
	)

	file, err := os.Create(sstable.filePath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer file.Close()

	// Write data blocks
	blockIndex := make([]IndexEntry, 0)
	blockOffset := int64(0)
	blockNum := 0

	for _, item := range items {
		// Add to bloom filter
		sstable.bloom.Add(item.Key)

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
		binary.LittleEndian.PutUint64(indexData[len(indexData)-8:], uint64(entry.BlockOffset))

		// Write block size
		indexData = append(indexData, make([]byte, 4)...)
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
	if err := binary.Write(file, binary.LittleEndian, uint32(len(indexData))); err != nil {
		return err
	}

	return nil
}

// removeTables removes tables from the level manager
func (lm *LevelManager) removeTables(tables []*SSTable) {
	for _, table := range tables {
		// Close table
		table.Close()

		// Remove from level
		for levelIdx, level := range lm.levels {
			for tableIdx, t := range level.Tables {
				if t == table {
					lm.levels[levelIdx].Tables = append(level.Tables[:tableIdx], level.Tables[tableIdx+1:]...)
					break
				}
			}
		}

		// Delete file
		os.Remove(table.filePath)
	}
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   []byte
	Value []byte
}
