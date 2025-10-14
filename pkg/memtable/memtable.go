package memtable

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/zhangyunhao116/skipmap"
)

var (
	ErrMemTableOverload = errors.New("memtable is overloaded")
	ErrTooLargeEntry    = errors.New("entry is too large")
)

type concurrentSet = skipmap.FuncMap[[]byte, Item]

type Memtable struct {
	threshold uint64
	ver       atomic.Uint64
	size      atomic.Uint64

	underlying atomic.Pointer[concurrentSet]
	// old immutable tables
	// the only data origin when rotation is applied
	imm atomic.Pointer[[]*concurrentSet]

	flushChan chan SortedSet
	mu        sync.Mutex
	cond      *sync.Cond
}

func New(threshold uint) *Memtable {
	mt := Memtable{
		threshold: uint64(threshold),
		flushChan: make(chan SortedSet, 2),
	}
	mt.underlying.Store(
		skipmap.NewFunc[[]byte, Item](func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
	)
	mt.cond = sync.NewCond(&mt.mu)
	return &mt
}

func (mt *Memtable) Get(k []byte) (Item, bool) {
	active := mt.underlying.Load()
	it, ok := active.Load(k)
	if ok {
		return it, true
	}

	immutable := mt.imm.Load()
	if immutable == nil {
		return Item{}, false
	}

	for _, ordMap := range *immutable {
		it, ok = ordMap.Load(k)
		if ok {
			return it, true
		}
	}

	return Item{}, false
}

func (mt *Memtable) Upsert(k, value []byte, seqN, meta uint64) error {
	const (
		mdSize   = 8
		seqNSize = 8
	)
	entSize := uint64(len(k)) + uint64(len(value)) + seqNSize + mdSize
	if entSize > mt.threshold {
		return ErrTooLargeEntry
	}

	for {
		currentSize := mt.size.Load()
		newSize := currentSize + entSize

		if newSize < mt.threshold {
			if mt.size.CompareAndSwap(currentSize, newSize) {
				break
			}
			continue
		}

		ver := mt.ver.Load()
		mt.mu.Lock()
		acquired := mt.ver.CompareAndSwap(ver, ver+1)
		if acquired {
			mt.rotate(entSize)
			mt.cond.Broadcast()
			mt.mu.Unlock()
			break
		} else {
			mt.cond.Wait()
			mt.mu.Unlock()
		}

	}

	active := mt.underlying.Load()
	active.Store(k, Item{
		Key:   k,
		Value: value,
		SeqN:  seqN,
		Meta:  meta,
	})

	return nil
}

func (mt *Memtable) rotate(initSize uint64) {
	current := mt.underlying.Load()
	mt.flushChan <- &sortedSet{current}

	oldSlice := mt.imm.Load()
	newSlice := append([]*concurrentSet{}, *oldSlice...)
	newSlice = append(newSlice, current)
	mt.imm.Store(&newSlice)

	mt.underlying.Store(
		skipmap.NewFunc[[]byte, Item](func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}),
	)
	mt.size.Store(initSize)
}

func (mt *Memtable) FlushChan() <-chan SortedSet {
	return mt.flushChan
}
