package store

const (
	insertOp operation = iota
	deleteOp
)

const (
	vTypeTombstone valType = iota
	vTypeBlob
	vTypeString
	vTypeJson
	vTypeInt32
	vTypeInt64
	vTypeFloat32
	vTypeFloat64
)

type operation uint8

type valType uint8

type value interface {
	bin() []byte
	typeOf() valType
}
