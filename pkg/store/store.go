package store

import (
	"lsmdb/pkg/memtable"
	"time"
)

type iTimeProvider interface {
	Now() time.Time
}

type Store struct {
	mt *memtable.Memtable
	tp iTimeProvider
}

func New(mt *memtable.Memtable, tp iTimeProvider) *Store {
	return &Store{
		mt: mt,
		tp: tp,
	}
}

func (s *Store) PutString(key string, value string) error {
	// TODO
	// encode operation & value type into metadata and save into the memtable
	// if it overloaded - save all the Memtable data into new l0 SSTable and to clean itself
	panic("TODO")
}
