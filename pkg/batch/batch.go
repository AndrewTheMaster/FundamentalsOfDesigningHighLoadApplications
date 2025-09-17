package batch

import "lsmdb/pkg/types"

// WriteBatch groups multiple mutations atomically.
type WriteBatch interface {
	Put(key types.Key, value types.Value)
	Delete(key types.Key)
	Clear()
	Count() int
} 