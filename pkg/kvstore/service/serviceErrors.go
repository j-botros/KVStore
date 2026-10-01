package service

import (
	"errors"
)

var (
	ErrNotFound = errors.New("key not found")
	ErrReadOnly = errors.New("this node is a follower and does not accept writes for this shard")
)
