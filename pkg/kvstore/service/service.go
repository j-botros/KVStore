package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	storageengine "kvstore/pkg/kvstore/storageEngine"
)

type Service struct {
	engine     *storageengine.StorageEngine
	nodeClient NodeClient   // forwards requests for keys this node doesn't own
	followers  []NodeClient // peers that receive replication entries from this leader

	minKey   string
	maxKey   string
	isLeader bool

	seqNum atomic.Uint64 // monotonically increasing log index for replication
}

func NewService(
	engine *storageengine.StorageEngine,
	nodeClient NodeClient,
	minKey, maxKey string,
	isLeader bool,
	followers []NodeClient,
) *Service {
	return &Service{
		engine:     engine,
		nodeClient: nodeClient,
		followers:  followers,
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

	if err := s.engine.Put(key, value); err != nil {
		return err
	}

	s.replicateAsync(LogEntry{
		SeqNum:    s.seqNum.Add(1),
		Operation: "PUT",
		Key:       key,
		Value:     value,
	})
	return nil
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
	if err != nil {
		return err
	}

	s.replicateAsync(LogEntry{
		SeqNum:    s.seqNum.Add(1),
		Operation: "DELETE",
		Key:       key,
	})
	return nil
}

// ReplicateEntry applies an inbound replication entry from the leader directly
// to the storage engine. It bypasses the isLeader guard — this path is only
// reachable via the gRPC ReplicateEntry RPC, not from client-facing requests.
func (s *Service) ReplicateEntry(entry LogEntry) error {
	switch entry.Operation {
	case "PUT":
		return s.engine.Put(entry.Key, entry.Value)
	case "DELETE":
		err := s.engine.Delete(entry.Key)
		if errors.Is(err, storageengine.ErrKeyNotFound) {
			return nil // idempotent: already absent is fine
		}
		return err
	default:
		return fmt.Errorf("unknown replication operation %q", entry.Operation)
	}
}

// replicateAsync fans out the log entry to all followers in background goroutines.
// Failures are logged but do not block the caller (fire-and-forget).
func (s *Service) replicateAsync(entry LogEntry) {
	for _, follower := range s.followers {
		go func(f NodeClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := f.ReplicateEntry(ctx, entry); err != nil {
				log.Printf("replication to follower failed (seq=%d key=%q): %v",
					entry.SeqNum, entry.Key, err)
			}
		}(follower)
	}
}

// NodeClient abstracts communication with a remote KVStore node.
// The concrete implementation lives in interface/grpc to avoid an import cycle.
type NodeClient interface {
	ForwardGet(ctx context.Context, key string) ([]byte, bool, error)
	ForwardPut(ctx context.Context, key string, value []byte) error
	ForwardDelete(ctx context.Context, key string) error
	ReplicateEntry(ctx context.Context, entry LogEntry) error
}