package dberrors

import "errors"

var (
	ErrNotFound          = errors.New("lsmdb: not found")
	ErrClosed            = errors.New("lsmdb: closed")
	ErrInvalidArgument   = errors.New("lsmdb: invalid argument")
	ErrCompactionRunning = errors.New("lsmdb: compaction running")
) 