package store

import (
	"context"
	"fmt"
	"lsmdb/pkg/clock"
	"lsmdb/pkg/config"
	"lsmdb/pkg/listener"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistence"
	"lsmdb/pkg/types"
	"lsmdb/pkg/wal"
)

type iJournal interface {
	listener.Job

	Append(e wal.Entry)
	Done() <-chan uint64
	Replay(start uint64, callback func(wal.Entry) error) error
}

type iClock interface {
	Val() types.SeqN
	Next() types.SeqN
	Set(t types.SeqN)
}

type Store struct {
	jr   iJournal
	seqN iClock
	cfg  *config.Config

	levelManager *persistence.LevelManager
	mt           *memtable.Memtable

	close func()
}

func New(cfg *config.Config, jr iJournal) (*Store, error) {
	// Create memtable
	mt := memtable.New(cfg.Memtable)

	// Create level manager
	levelManager := persistence.NewLevelManager(cfg.Persistence)

	// Create manifest
	manifest := persistence.NewManifest(cfg.Persistence.RootPath)

	if err := manifest.Load(); err != nil {
		return nil, err
	}

	store := &Store{
		mt:           mt,
		jr:           jr,
		levelManager: levelManager,
		seqN: clock.NewAtomic(
			manifest.PersistentID(),
		),
		cfg: cfg,
	}

	if err := store.restoreFromJournal(); err != nil {
		return nil, err
	}

	// start background goroutine to flush memtable in background
	ctx := context.Background()
	flusher := NewFlusher(mt.FlushChan(), cfg.Persistence.RootPath, levelManager, manifest)
	flusher.Start(ctx)

	// start background goroutine to flush WAL async
	store.jr.Start(ctx)

	store.close = func() {
		flusher.Stop()
		store.jr.Stop()
		store.mt.Close()
	}

	return store, nil
}

func (s *Store) restoreFromJournal() error {
	if s.jr == nil {
		return ErrWALNotInitialized
	}

	// Replay from the last known persistent entry seq number
	return s.jr.Replay(s.seqN.Val()+1, func(entry wal.Entry) error {
		// Actualize seqN if needed
		if entry.SeqNum > s.seqN.Val() {
			s.seqN.Set(entry.SeqNum)
		}

		return s.mt.Upsert(entry.Key, entry.Value, entry.SeqNum, entry.Meta)
	})
}

func (s *Store) Put(key string, value any) error {
	switch typedVal := value.(type) {
	case string:
		return s.PutString(key, typedVal)
	default:
		return ErrValueTypeNotSupported
	}
}

func (s *Store) PutString(key string, value string) error {
	return s.put(key, String(value), InsertOp)
}

func (s *Store) put(key string, val value, op Operation) error {
	entryID := s.seqN.Next()
	md := newMD(op, val.typeOf())

	s.jr.Append(wal.Entry{
		SeqNum: entryID,
		Key:    []byte(key),
		Value:  val.bin(),
		Meta:   uint64(md),
	})
	// wait for the WAL to confirm write
	for id := <-s.jr.Done(); id != entryID; {
		id = <-s.jr.Done()
	}

	return s.mt.Upsert(
		[]byte(key),
		val.bin(),
		entryID,
		uint64(md),
	)
}

func (s *Store) Get(key string) (storable, bool, error) {
	keyBytes := []byte(key)

	// first check memtable
	item, ok := s.mt.Get(keyBytes)
	if ok {
		md := MD(item.Meta)
		if md.operation() == DeleteOp {
			return nil, false, nil // deleted
		}

		storableItem, err := fromMemtableItem(item)
		return storableItem, true, err
	}

	// Search in LSM-tree levels
	sstableItem, err := s.levelManager.Get(keyBytes)
	if err != nil {
		return nil, false, fmt.Errorf("failed to Get from LSM-tree: %w", err)
	}

	if sstableItem == nil {
		return nil, false, nil
	}

	md := MD(sstableItem.Meta)
	if md.operation() == DeleteOp {
		return nil, false, nil // deleted
	}

	storableItem, err := fromSStableItem(*sstableItem)
	return storableItem, true, err
}

func (s *Store) GetString(key string) (string, bool, error) {
	item, has, err := s.Get(key)
	if err != nil || !has {
		return "", has, err
	}

	strItem, ok := item.(String)
	if !ok {
		return "", false, ErrValueTypeMismatch
	}

	return string(strItem), true, nil
}

func (s *Store) Delete(key string) error {
	return s.put(key, tombstone{}, DeleteOp)
}

func (s *Store) Close() {
	s.close()
}
