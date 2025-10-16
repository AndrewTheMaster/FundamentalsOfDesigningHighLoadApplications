package store

import (
	"context"
	"fmt"
	"lsmdb/pkg/clock"
	"lsmdb/pkg/listener"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistance"
	"lsmdb/pkg/types"
	"lsmdb/pkg/wal"
	"time"
)

type iJournal interface {
	listener.Job

	Append(e wal.Entry)
	Done() <-chan uint64
	Replay(start uint64, callback func(wal.Entry) error) error
}

type iTimeProvider interface {
	Now() time.Time
}

type iClock interface {
	Val() types.SeqN
	Next() types.SeqN
	Set(t types.SeqN)
}

type Store struct {
	tp      iTimeProvider
	jr      iJournal
	seqN    iClock
	dataDir string

	levelManager *persistance.LevelManager
	mt           *memtable.Memtable

	close func()
}

func New(dataDir string, tp iTimeProvider) (*Store, error) {
	// Create WAL
	journal, err := wal.New(dataDir)
	if err != nil {
		return nil, err
	}

	// Create memtable
	mt := memtable.New(1024)

	// Create level manager
	levelManager := persistance.NewLevelManager(dataDir)

	// Create manifest
	manifest := persistance.NewManifest(dataDir)

	if err := manifest.Load(); err != nil {
		return nil, err
	}

	store := &Store{
		mt:           mt,
		tp:           tp,
		jr:           journal,
		levelManager: levelManager,
		seqN: clock.NewAtomic(
			manifest.PersistentID(),
		),
		dataDir: dataDir,
	}

	if err := store.restoreFromJournal(); err != nil {
		return nil, err
	}

	// start background goroutine to flush memtable in background
	ctx := context.Background()
	flusher := NewFlusher(mt.FlushChan(), dataDir, levelManager, manifest)
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
	switch value.(type) {
	case string:
		return s.PutString(key, value.(string))
	default:
		return ErrValueTypeNotSupported
	}
}

func (s *Store) PutString(key string, value string) error {
	return s.put(key, String(value), insertOp)
}

func (s *Store) put(key string, val value, op operation) error {
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
		if md.operation() == deleteOp {
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
	if md.operation() == deleteOp {
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
	return s.put(key, tombstone{}, deleteOp)
}

func (s *Store) Close() {
	s.close()
}
