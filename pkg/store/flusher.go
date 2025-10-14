package store

import (
	"context"
	"errors"
	"fmt"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistance"
)

type Flusher struct {
	lvlManager *persistance.LevelManager
	manifest   *persistance.Manifest
	in         <-chan memtable.SortedSet
	dataDir    string

	cancel func()
}

func NewFlusher(
	in <-chan memtable.SortedSet,
	dataDir string,
	manager *persistance.LevelManager,
	manifest *persistance.Manifest,
) *Flusher {
	return &Flusher{
		lvlManager: manager,
		manifest:   manifest,
		dataDir:    dataDir,
		in:         in,
		cancel:     func() {},
	}
}

func (f *Flusher) Start(ctx context.Context) error {
	ctx, f.cancel = context.WithCancel(ctx)
	for {
		if err := f.run(ctx); err != nil {
			return err
		}
	}
}

func (f *Flusher) run(ctx context.Context) error {
	select {
	case ss := <-f.in:
		err := f.flush(ss)
		if err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	case <-ctx.Done():
		return errors.New("flusher stopped by context")
	}

	return nil
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

	// Add to manifest
	if err := f.manifest.AddTable(tableID, filePath, 0, sstable.ApproximateSize()); err != nil {
		return fmt.Errorf("failed to add table to manifest: %w", err)
	}

	return nil
}

func (f *Flusher) Stop() {
	f.cancel()
}
