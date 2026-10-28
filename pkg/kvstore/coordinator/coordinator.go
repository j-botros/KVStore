package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	storageengine "kvstore/pkg/kvstore/storageEngine"
)

type Coordinator struct {
	engine       *storageengine.StorageEngine
	router       *ShardRouter // routes reads/writes to the correct remote shard
	leaderClient NodeClient   // this shard's leader; non-nil only when isLeader == false
	followers    []NodeClient // peers that receive replication entries from this leader

	minKey   string
	maxKey   string
	isLeader atomic.Bool

	seqNum atomic.Uint64 // monotonically increasing log index for replication
}

func NewCoordinator(
	engine *storageengine.StorageEngine,
	router *ShardRouter,
	leaderClient NodeClient,
	minKey, maxKey string,
	isLeader bool,
	followers []NodeClient,
) *Coordinator {
	c := &Coordinator{
		engine:       engine,
		router:       router,
		leaderClient: leaderClient,
		followers:    followers,
		minKey:       minKey,
		maxKey:       maxKey,
		isLeader:     atomic.Bool{},
	}
	c.isLeader.Store(isLeader)
	return c
}

// ownsKey reports whether this node is responsible for the given key.
// minKey == "" means no lower bound (owns everything up to maxKey).
// maxKey == "" means no upper bound (owns everything from minKey onwards).
// The upper bound is exclusive: a key equal to maxKey belongs to the next shard.
func (c *Coordinator) ownsKey(key string) bool {
	aboveMin := c.minKey == "" || key >= c.minKey
	belowMax := c.maxKey == "" || key < c.maxKey
	return aboveMin && belowMax
}

// Get reads a value by key.
//   - If the key belongs to a remote shard, the read is forwarded pseudo-fairly
//     across all nodes (leader + followers) of that shard via the ShardRouter.
//   - If the key belongs to this shard, it is served locally regardless of
//     whether this node is the leader or a follower (both can serve reads).
func (c *Coordinator) Get(ctx context.Context, key string) ([]byte, error) {
	if !c.ownsKey(key) {
		peer, err := c.router.RouteRead(key)
		if err != nil {
			return nil, err
		}
		value, found, err := peer.ForwardGet(ctx, key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrNotFound
		}
		return value, nil
	}

	value, err := c.engine.Get(key)
	if errors.Is(err, storageengine.ErrKeyNotFound) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return value, nil
}

// Put writes a key-value pair.
//   - If the key belongs to a remote shard, the write is forwarded to that
//     shard's leader via the ShardRouter.
//   - If the key belongs to this shard but this node is a follower, the write
//     is proxied to this shard's leader via leaderClient.
//   - If the key belongs to this shard and this node is the leader, the write
//     is applied locally and replicated to followers asynchronously.
func (c *Coordinator) Put(ctx context.Context, key string, value []byte) error {
	if !c.ownsKey(key) {
		leader, err := c.router.RouteWrite(key)
		if err != nil {
			return err
		}
		return leader.ForwardPut(ctx, key, value)
	}

	if !c.isLeader.Load() {
		if c.leaderClient == nil {
			return ErrReadOnly
		}
		return c.leaderClient.ForwardPut(ctx, key, value)
	}

	if err := c.engine.Put(key, value); err != nil {
		return err
	}
	c.replicateAsync(LogEntry{
		SeqNum:    c.seqNum.Add(1),
		Operation: "PUT",
		Key:       key,
		Value:     value,
	})
	return nil
}

// Delete removes a key.
//   - If the key belongs to a remote shard, the delete is forwarded to that
//     shard's leader via the ShardRouter.
//   - If the key belongs to this shard but this node is a follower, the delete
//     is proxied to this shard's leader via leaderClient.
//   - If the key belongs to this shard and this node is the leader, the delete
//     is applied locally and replicated to followers asynchronously.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	if !c.ownsKey(key) {
		leader, err := c.router.RouteWrite(key)
		if err != nil {
			return err
		}
		return leader.ForwardDelete(ctx, key)
	}

	if !c.isLeader.Load() {
		if c.leaderClient == nil {
			return ErrReadOnly
		}
		return c.leaderClient.ForwardDelete(ctx, key)
	}

	err := c.engine.Delete(key)
	if errors.Is(err, storageengine.ErrKeyNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	c.replicateAsync(LogEntry{
		SeqNum:    c.seqNum.Add(1),
		Operation: "DELETE",
		Key:       key,
	})
	return nil
}

// ReplicateEntry applies an inbound replication entry from the leader directly
// to the storage engine. It bypasses the isLeader guard — this path is only
// reachable via the gRPC ReplicateEntry RPC, not from client-facing requests.
func (c *Coordinator) ReplicateEntry(entry LogEntry) error {
	switch entry.Operation {
	case "PUT":
		return c.engine.Put(entry.Key, entry.Value)
	case "DELETE":
		err := c.engine.Delete(entry.Key)
		if errors.Is(err, storageengine.ErrKeyNotFound) {
			return nil // idempotent: already absent is fine
		}
		return err
	default:
		return fmt.Errorf("unknown replication operation %q", entry.Operation)
	}
}

// Return sequence number.
// Used for searching for the latest seqNum in a node.
func (c *Coordinator) GetSeqnum() uint64 {
	return c.seqNum.Load()
}

// Scan iterates every live key-value pair in this node's storage engine.
// The yield callback is called once per entry; returning a non-nil error
// from yield aborts the scan and Scan returns that error.
func (c *Coordinator) Scan(yield func(key string, value []byte) error) error {
	return c.engine.Scan(yield)
}

// Applies a single batch of entries.
func (c *Coordinator) ApplySnapshot(donorSeqNum uint64, entries []LogEntry, isFinal bool) error {
	for _, e := range entries {
		if err := c.engine.Put(e.Key, e.Value); err != nil {
			return err
		}
	}
	if isFinal {
		c.seqNum.Store(donorSeqNum)
	}
	return nil
}

// replicateAsync fans out the log entry to all followers in background goroutines.
// Failures are logged but do not block the caller (fire-and-forget).
func (c *Coordinator) replicateAsync(entry LogEntry) {
	for _, follower := range c.followers {
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
