package storageengine

import (
	"errors"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrBadFile       = errors.New("corrupt or invalid file")
	ErrBadData       = errors.New("corrupt or invalid entry")
	ErrLvlNotFound   = errors.New("level not found")
	ErrEntryNotFound = errors.New("reader empty; error not found")
)
