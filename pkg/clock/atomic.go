package clock

import "sync/atomic"

type AtomicClock struct {
	atomic.Uint64
}

func NewAtomic(init uint64) *AtomicClock {
	var ac AtomicClock
	ac.Set(init)
	return &ac
}

func (ac *AtomicClock) Val() uint64 {
	return ac.Load()
}

func (ac *AtomicClock) Next() uint64 {
	return ac.Add(1)
}

func (ac *AtomicClock) Set(t uint64) {
	ac.Store(t)
}
