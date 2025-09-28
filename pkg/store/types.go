package store

import "encoding/binary"

type String string

func (s String) typeOf() valType {
	return vTypeString
}

func (s String) bin() []byte {
	return []byte(s)
}

type Blob []byte

func (b Blob) typeOf() valType {
	return vTypeBlob
}

func (b Blob) bin() []byte {
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
