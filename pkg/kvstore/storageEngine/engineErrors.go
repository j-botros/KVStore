package storageengine

import (
	"errors"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrBadFile        = errors.New("corrupt or invalid file")
	ErrBadData        = errors.New("corrupt or invalid entry")
	ErrLvlNotFound    = errors.New("level not found")
	ErrEntryNotFound  = errors.New("reader empty; entry not found")
	ErrCompactionDone = errors.New("compaction sources exhausted before newSst is full")
	ErrCompactionFull = errors.New("newSst full before compaction sources are exhausted")
)
