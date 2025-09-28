package memtable

import "bytes"

const (
	ZeroMD uint64 = 0
)

type Item struct {
	Key   []byte
	Value []byte
	SeqN  uint64
	Meta  uint64
}

func (it *Item) Less(than *Item) bool {
	return bytes.Compare(it.Key, than.Key) < 0
}
