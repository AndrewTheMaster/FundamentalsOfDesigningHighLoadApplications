package store

import "errors"

var (
	ErrWALNotInitialized     = errors.New("WAL not initialized")
	ErrValueTypeNotSupported = errors.New("value type not supported")
	ErrValueTypeMismatch     = errors.New("value type mismatch")
)
