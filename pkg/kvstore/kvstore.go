package kvstore

import (
	"kvstore/pkg/kvstore/service"
)

type KVStore struct {
	data map[string][]byte

	service service.Service
}
