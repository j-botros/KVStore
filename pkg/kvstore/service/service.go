package service

import (
	storageengine "kvstore/pkg/kvstore/storageEngine"
)

type Service struct {
	engine *storageengine.StorageEngine
}

