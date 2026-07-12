package kvstore

import (
	"kvstore/pkg/kvstore/controller"
	"kvstore/pkg/kvstore/service"
	"kvstore/pkg/kvstore/storageEngine"
)

type KVStore struct {
	data map[string][]byte

	service service.Service
}

func (c *KVStore) Get(key string) []byte {
	return service.Get(key)
}

func (c *KVStore) Put(key string, value []byte) {
	return service.Put(key, value)
}

func (c *KVStore) Delete(key string) {
	return service.Delete(key)
}
