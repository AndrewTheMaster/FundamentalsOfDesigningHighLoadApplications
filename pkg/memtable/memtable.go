package memtable

import "errors"

var (
	ErrMemTableOverload = errors.New("memtable is overloaded")
)

type iSortedSet interface {
	Sorted() []*Item
	Has(item *Item) bool
	InsertOrReplace(item *Item) *Item
	Get(key []byte) *Item
	Len() int
	Iterator() func(item *Item) bool
}

type Memtable struct {
	threshold int
	size      int
	ss        iSortedSet
}

func New(threshold int, sortedSet iSortedSet) *Memtable {
	return &Memtable{
		threshold: threshold,
		ss:        sortedSet,
		size:      0,
	}
}

func (mt *Memtable) Snapshot() []*Item {
	return mt.ss.Sorted()
}

func (mt *Memtable) Upsert(key, value []byte, seqN, meta uint64) (bool, error) {
	found := mt.ss.InsertOrReplace(&Item{
		Key:   key,
		Value: value,
		SeqN:  seqN,
		Meta:  meta,
	})

	mt.size += len(key) + len(value) + 8 // 8 = sizeof(seqNumber) + sizeof(meta)
	if mt.size >= mt.threshold {
		return false, ErrMemTableOverload
	}

	return found != nil, nil
}

func (mt *Memtable) Get(key []byte) *Item {
	return mt.Get(key)
}

func Replace(ss iSortedSet) {
	// TODO: must atomically reuturn snapshop + replace underlying container
	// TODO: by using atomic.Pointer for iSortedSet
}
