package service

import (
	"errors"
	storageengine "kvstore/pkg/kvstore/storageEngine"
)

type Service struct {
	engine *storageengine.StorageEngine
}

func (s *Service) Get(key string) ([]byte, error) {
	value, err := s.engine.Get(key)
	if errors.Is(err, storageengine.ErrKeyNotFound) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Service) Put(key string, value []byte) error {
	err := s.engine.Put(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) Delete(key string) error {
	err := s.engine.Delete(key)
	if err != nil {
		return err
	}
	return nil
}
