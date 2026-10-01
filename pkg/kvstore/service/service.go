package service

import (
	"context"
	"errors"
	"fmt"

	storageengine "kvstore/pkg/kvstore/storageEngine"
)

type Service struct {
	engine     *storageengine.StorageEngine
	nodeClient NodeClient

	minKey   string
	maxKey   string
	isLeader bool
}

func NewService(engine *storageengine.StorageEngine, nodeClient NodeClient, minKey, maxKey string, isLeader bool) *Service {
	return &Service{
		engine:     engine,
		nodeClient: nodeClient,
		minKey:     minKey,
		maxKey:     maxKey,
		isLeader:   isLeader,
	}
}

// ownsKey reports whether this node is responsible for the given key.
// minKey == "" means no lower bound (owns everything up to maxKey).
// maxKey == "" means no upper bound (owns everything from minKey onwards).
// The upper bound is exclusive: a key equal to maxKey belongs to the next shard.
func (s *Service) ownsKey(key string) bool {
	aboveMin := s.minKey == "" || key >= s.minKey
	belowMax := s.maxKey == "" || key < s.maxKey
	return aboveMin && belowMax
}

func (s *Service) Get(key string) ([]byte, error) {
	if !s.ownsKey(key) {
		if s.nodeClient == nil {
			return nil, fmt.Errorf("key %q is out of range and no peer node is configured", key)
		}
		value, found, err := s.nodeClient.ForwardGet(context.Background(), key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrNotFound
		}
		return value, nil
	}

	value, err := s.engine.Get(key)
	if errors.Is(err, storageengine.ErrKeyNotFound) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return value, nil
}

func (s *Service) Put(key string, value []byte) error {
	if !s.ownsKey(key) {
		if s.nodeClient == nil {
			return fmt.Errorf("key %q is out of range and no peer node is configured", key)
		}
		return s.nodeClient.ForwardPut(context.Background(), key, value)
	}

	if !s.isLeader {
		return ErrReadOnly
	}
	return s.engine.Put(key, value)
}

func (s *Service) Delete(key string) error {
	if !s.ownsKey(key) {
		if s.nodeClient == nil {
			return fmt.Errorf("key %q is out of range and no peer node is configured", key)
		}
		return s.nodeClient.ForwardDelete(context.Background(), key)
	}

	if !s.isLeader {
		return ErrReadOnly
	}
	err := s.engine.Delete(key)
	if errors.Is(err, storageengine.ErrKeyNotFound) {
		return ErrNotFound
	}
	return err
}

// NodeClient abstracts communication with a remote KVStore node.
// The concrete implementation lives in interface/grpc to avoid an import cycle.
type NodeClient interface {
	ForwardGet(ctx context.Context, key string) ([]byte, bool, error)
	ForwardPut(ctx context.Context, key string, value []byte) error
	ForwardDelete(ctx context.Context, key string) error
}