package store

import "time"

const (
	insertOp operation = iota
	deleteOp
)

const (
	vTypeBlob valType = iota
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

type Item struct {
	op        operation
	key       []byte
	val       value
	timestamp time.Time
}
