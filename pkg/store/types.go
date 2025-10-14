package store

import (
	"encoding/binary"
	"errors"
	"lsmdb/pkg/memtable"
	"lsmdb/pkg/persistance"
)

var (
	ErrUnknownValueType = errors.New("unknown value type")

	buildMap = map[valType]func([]byte) storable{
		vTypeBlob:   newStorable(newBlob),
		vTypeString: newStorable(newString),
		vTypeInt32:  newStorable(newInt32),
	}
)

type storable interface {
	typeOf() valType
	bin() []byte
}

func newStorable[T storable](f func([]byte) T) func([]byte) storable {
	return func(b []byte) storable {
		return f(b)
	}
}

func fromMemtableItem(item memtable.Item) (storable, error) {
	md := MD(item.Meta)
	if build, ok := buildMap[md.valType()]; ok {
		return build(item.Value), nil
	}

	return nil, ErrUnknownValueType
}

func fromSStableItem(item persistance.SSTableItem) (storable, error) {
	md := MD(item.Meta)
	if build, ok := buildMap[md.valType()]; ok {
		return build(item.Value), nil
	}

	return nil, ErrUnknownValueType
}

type String string

func (s String) typeOf() valType {
	return vTypeString
}

func (s String) bin() []byte {
	return []byte(s)
}

func newString(b []byte) String {
	return String(b)
}

type Blob []byte

func (b Blob) typeOf() valType {
	return vTypeBlob
}

func (b Blob) bin() []byte {
	return b
}

func newBlob(b []byte) Blob {
	return b
}

type Int32 int32

func (i Int32) typeOf() valType {
	return vTypeInt32
}

func (i Int32) bin() []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(i))
	return bs
}

func newInt32(b []byte) Int32 {
	if len(b) < 4 {
		return 0
	}
	return Int32(int32(binary.LittleEndian.Uint32(b)))
}

// uses for delete op
type tombstone struct{}

func (e tombstone) typeOf() valType {
	return vTypeTombstone
}

func (e tombstone) bin() []byte {
	return nil
}
