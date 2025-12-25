package store

import (
	"fmt"
	"lsmdb/pkg/listener"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistence"
)

type Flusher struct {
	*listener.Listener[memtable.SortedSet]

	lvlManager *persistence.LevelManager
	manifest   *persistence.Manifest
	dataDir    string
}

func NewFlusher(
	in <-chan memtable.SortedSet,
	dataDir string,
	manager *persistence.LevelManager,
	manifest *persistence.Manifest,
) *Flusher {
	flusher := &Flusher{
		lvlManager: manager,
		manifest:   manifest,
		dataDir:    dataDir,
	}
	flusher.Listener = listener.New(in, flusher.flush)
	return flusher
}

func (f *Flusher) flush(ss memtable.SortedSet) error {
	snapshot := ss.Sorted()

	if len(snapshot) == 0 {
		return nil
	}

	// Create SSTable from memtable data
	tableID := f.manifest.GetNextTableID()
	filePath := fmt.Sprintf("%s/L0_%d.sst", f.dataDir, tableID)

	// Create bloom filter
	bloom := persistence.NewBloomFilter(uint32(len(snapshot)), 0.01)

	// Create cache
	cache := persistence.NewBlockCache(100)

	// Create SSTable
	sstable := persistence.NewSSTable(filePath, bloom, cache)

	// Convert memtable items to SSTable items
	sstableItems := make([]persistence.SSTableItem, 0, len(snapshot))
	for _, item := range snapshot {
		sstableItems = append(sstableItems, persistence.SSTableItem{
			Key:   item.Key,
			Value: item.Value,
			Meta:  item.Meta,
			ID:    item.SeqN,
		})
	}

	// Write data to SSTable
	if err := f.lvlManager.WriteSSTableData(sstable, sstableItems); err != nil {
		return fmt.Errorf("failed to write SSTable data: %w", err)
	}

	// Open the table
	if err := sstable.Open(); err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}

	// Add to level manager (L0)
	if err := f.lvlManager.AddSSTable(sstable, 0); err != nil {
		return fmt.Errorf("failed to add SSTable to level manager: %w", err)
	}

	f.manifest.AddTable(tableID, filePath, 0, sstable.ApproximateSize())
	f.manifest.UpdateMeta(sstableItems)
	if err := f.manifest.Save(); err != nil {
		return fmt.Errorf("failed to add table to manifest: %w", err)
	}

	return nil
}
