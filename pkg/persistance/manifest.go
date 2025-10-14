package persistance

import (
	"encoding/json"
	"fmt"
	"lsmdb/pkg/types"
	"os"
	"path/filepath"
	"sync"
)

// Manifest manages metadata about SSTables and levels
type Manifest struct {
	mu       sync.RWMutex
	filePath string
	metadata ManifestData
}

// ManifestData represents the manifest data
type ManifestData struct {
	NextTableID  uint64              `json:"next_table_id"`
	Levels       map[int][]TableInfo `json:"levels"`
	Version      int                 `json:"version"`
	PersistentID types.SeqN          `json:"persistent_id"`
}

// TableInfo represents information about an SSTable
type TableInfo struct {
	ID       uint64 `json:"id"`
	FilePath string `json:"file_path"`
	Level    int    `json:"level"`
	Size     int64  `json:"size"`
}

// NewManifest creates a new manifest
func NewManifest(dataDir string) *Manifest {
	return &Manifest{
		filePath: filepath.Join(dataDir, "MANIFEST"),
		metadata: ManifestData{
			NextTableID: 1,
			Levels:      make(map[int][]TableInfo),
			Version:     1,
		},
	}
}

// Load loads the manifest from disk
func (m *Manifest) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if manifest file exists
	if _, err := os.Stat(m.filePath); os.IsNotExist(err) {
		// Create new manifest
		return m.save()
	}

	// Read manifest file
	data, err := os.ReadFile(m.filePath)
	if err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}

	// Parse JSON
	if err := json.Unmarshal(data, &m.metadata); err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}

	return nil
}

// Save saves the manifest to disk
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.save()
}

// save saves the manifest to disk (internal method)
func (m *Manifest) save() error {
	// Ensure directory exists
	dir := filepath.Dir(m.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create manifest directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(m.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Write to file
	if err := os.WriteFile(m.filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	return nil
}

// AddTable adds a table to the manifest
func (m *Manifest) AddTable(tableID uint64, filePath string, level int, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize level if it doesn't exist
	if m.metadata.Levels[level] == nil {
		m.metadata.Levels[level] = make([]TableInfo, 0)
	}

	// Add table info
	tableInfo := TableInfo{
		ID:       tableID,
		FilePath: filePath,
		Level:    level,
		Size:     size,
	}

	m.metadata.Levels[level] = append(m.metadata.Levels[level], tableInfo)

	// Update next table ID
	if tableID >= m.metadata.NextTableID {
		m.metadata.NextTableID = tableID + 1
	}

	return m.save()
}

// RemoveTable removes a table from the manifest
func (m *Manifest) RemoveTable(tableID uint64, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if level >= len(m.metadata.Levels) {
		return fmt.Errorf("invalid level: %d", level)
	}

	// Find and remove table
	tables := m.metadata.Levels[level]
	for i, table := range tables {
		if table.ID == tableID {
			m.metadata.Levels[level] = append(tables[:i], tables[i+1:]...)
			return m.save()
		}
	}

	return fmt.Errorf("table not found: %d", tableID)
}

// GetTables returns all tables for a given level
func (m *Manifest) GetTables(level int) []TableInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if level >= len(m.metadata.Levels) {
		return nil
	}

	return m.metadata.Levels[level]
}

// GetAllTables returns all tables from all levels
func (m *Manifest) GetAllTables() map[int][]TableInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy
	result := make(map[int][]TableInfo)
	for level, tables := range m.metadata.Levels {
		result[level] = make([]TableInfo, len(tables))
		copy(result[level], tables)
	}

	return result
}

// GetNextTableID returns the next available table ID
func (m *Manifest) GetNextTableID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.metadata.NextTableID
	m.metadata.NextTableID++
	return id
}

// GetLevelCount returns the number of levels
func (m *Manifest) GetLevelCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.metadata.Levels)
}

// CompactLevels performs compaction between levels
func (m *Manifest) CompactLevels(fromLevel, toLevel int, tablesToRemove []uint64, newTables []TableInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove old tables
	for _, tableID := range tablesToRemove {
		if err := m.removeTableFromLevel(tableID, fromLevel); err != nil {
			return fmt.Errorf("failed to remove table %d: %w", tableID, err)
		}
	}

	// Add new tables
	for _, table := range newTables {
		if m.metadata.Levels[table.Level] == nil {
			m.metadata.Levels[table.Level] = make([]TableInfo, 0)
		}
		m.metadata.Levels[table.Level] = append(m.metadata.Levels[table.Level], table)
	}

	return m.save()
}

// removeTableFromLevel removes a table from a specific level
func (m *Manifest) removeTableFromLevel(tableID uint64, level int) error {
	if level >= len(m.metadata.Levels) {
		return fmt.Errorf("invalid level: %d", level)
	}

	tables := m.metadata.Levels[level]
	for i, table := range tables {
		if table.ID == tableID {
			m.metadata.Levels[level] = append(tables[:i], tables[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("table not found: %d", tableID)
}

// GetTableInfo returns information about a specific table
func (m *Manifest) GetTableInfo(tableID uint64) (*TableInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, tables := range m.metadata.Levels {
		for _, table := range tables {
			if table.ID == tableID {
				return &table, nil
			}
		}
	}

	return nil, fmt.Errorf("table not found: %d", tableID)
}

// GetTotalSize returns the total size of all tables
func (m *Manifest) GetTotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalSize := int64(0)
	for _, tables := range m.metadata.Levels {
		for _, table := range tables {
			totalSize += table.Size
		}
	}

	return totalSize
}

// GetLevelSize returns the total size of tables in a specific level
func (m *Manifest) GetLevelSize(level int) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if level >= len(m.metadata.Levels) {
		return 0
	}

	totalSize := int64(0)
	for _, table := range m.metadata.Levels[level] {
		totalSize += table.Size
	}

	return totalSize
}

func (m *Manifest) PersistentID() types.SeqN {
	return m.metadata.PersistentID
}
