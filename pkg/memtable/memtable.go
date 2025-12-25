package memtable

import (
	"bytes"
	"errors"
	"lsmdb/pkg/config"
	"sync"
	"sync/atomic"

	"github.com/zhangyunhao116/skipmap"
)

var (
	ErrTooLargeEntry = errors.New("entry is too large")
)

type concurrentSet = skipmap.FuncMap[[]byte, Item]

type Memtable struct {
	cfg  *config.MemtableConfig
	ver  atomic.Uint64
	size atomic.Uint64

	underlying atomic.Pointer[concurrentSet]
	// old immutable tables
	// the only data origin when rotation is applied
	imm atomic.Pointer[[]*concurrentSet]

	flushChan chan SortedSet
	mu        sync.Mutex
	cond      *sync.Cond
}

func New(cfg config.MemtableConfig) *Memtable {
	mt := Memtable{
		cfg:       &cfg,
		flushChan: make(chan SortedSet, cfg.FlushChanBuffSize),
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

	var (
		entSize   = uint64(len(k)) + uint64(len(value)) + seqNSize + mdSize
		threshold = uint64(mt.cfg.FlushThresholdBytes)
	)

	if entSize > threshold {
		return ErrTooLargeEntry
	}

	for {
		currentSize := mt.size.Load()
		newSize := currentSize + entSize

		if newSize < threshold {
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

	oldSlicePtr := mt.imm.Load()
	var newSlice []*concurrentSet
	if oldSlicePtr != nil {
		newSlice = append([]*concurrentSet{}, *oldSlicePtr...)
	}
	newSlice = append(newSlice, current)
	if len(newSlice) > mt.cfg.MaxImmTables {
		// drop the oldest immutable table
		newSlice = newSlice[1:]
	}
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

func (mt *Memtable) Close() {
	close(mt.flushChan)
}
