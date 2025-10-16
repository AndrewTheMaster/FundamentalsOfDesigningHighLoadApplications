package persistance

import (
	"encoding/binary"
	"fmt"
	"lsmdb/pkg/memtable"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	dataDir string
	mu      sync.RWMutex
	// Simple in-memory storage for now
	storage map[string][]byte
}

func NewStore(dataDir string) *Store {
	return &Store{
		dataDir: dataDir,
		storage: make(map[string][]byte),
	}
}

// Put saves a snapshot of memtable items to storage
func (ps *Store) Put(items []*memtable.Item) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	// Simple implementation: store each item
	for _, item := range items {
		key := string(item.Key)
		ps.storage[key] = item.Value
	}
	
	return nil
}

// Get retrieves a value by key
func (ps *Store) Get(key []byte) ([]byte, bool, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	
	value, found := ps.storage[string(key)]
	return value, found, nil
}

// Save saves an SSTable (placeholder)
func (ps *Store) Save(sst *SSTable) error {
	// TODO: Implement actual SSTable saving
	return nil
}

// LoadL0 loads L0 level data
func (ps *Store) LoadL0() (*Iterator, error) {
	// TODO: Implement L0 loading
	return nil, fmt.Errorf("not implemented")
}

// Load loads data from a specific level
func (ps *Store) Load(level int) (*Iterator, error) {
	// TODO: Implement level loading
	return nil, fmt.Errorf("not implemented")
}

// Compact compacts a level
func (ps *Store) Compact(level int) error {
	// TODO: Implement compaction
	return fmt.Errorf("not implemented")
}

// Simple file-based persistence for SSTables
func (ps *Store) saveToFile(items []*memtable.Item, filename string) error {
	filePath := filepath.Join(ps.dataDir, filename)
	
	// Ensure directory exists
	if err := os.MkdirAll(ps.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	
	// Write items to file
	for _, item := range items {
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
		
		// Write sequence number
		if err := binary.Write(file, binary.LittleEndian, item.SeqN); err != nil {
			return err
		}
		
		// Write metadata
		if err := binary.Write(file, binary.LittleEndian, item.Meta); err != nil {
			return err
		}
	}
	
	return nil
}
