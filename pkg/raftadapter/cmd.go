package raftadapter

import (
	"lsmdb/pkg/store"

	"github.com/google/uuid"
)

type Cmd struct {
	Op    store.Operation `json:"op"`
	Key   []byte          `json:"key"`
	Value []byte          `json:"value"`
	ID    uuid.UUID       `json:"id"`
}

func NewCmd(op store.Operation, key, value []byte) Cmd {
	return Cmd{
		Op:    op,
		Key:   key,
		Value: value,
		ID:    uuid.New(),
	}
}
