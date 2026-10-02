package service

import (
	"fmt"
	"sync/atomic"
)

// ShardNodeConfig describes a single remote shard and the NodeClients for its nodes.
// Used to build a ShardRouter in main and passed into NewService.
type ShardNodeConfig struct {
	MinKey    string
	MaxKey    string
	Leader    NodeClient   // always routes writes here
	Followers []NodeClient // included in read round-robin pool alongside leader
}

// shardInfo is the internal runtime state for one remote shard.
type shardInfo struct {
	minKey   string
	maxKey   string
	leader   NodeClient
	allNodes []NodeClient // leader first, then followers
	readIdx  atomic.Uint64
}

func (s *shardInfo) ownsKey(key string) bool {
	aboveMin := s.minKey == "" || key >= s.minKey
	belowMax := s.maxKey == "" || key < s.maxKey
	return aboveMin && belowMax
}

// ShardRouter routes requests to the correct remote shard.
//
//   - Reads  (RouteRead)  round-robin across all nodes of the owning shard
//     for pseudo-fair load distribution.
//   - Writes (RouteWrite) always return the shard's leader.
type ShardRouter struct {
	shards []*shardInfo
}

// NewShardRouter builds a ShardRouter from the given per-shard configurations.
// Only include shards that this node does NOT own — the service handles its own
// key range locally and will never call the router for those keys.
func NewShardRouter(configs []ShardNodeConfig) *ShardRouter {
	shards := make([]*shardInfo, 0, len(configs))
	for _, c := range configs {
		allNodes := make([]NodeClient, 0, 1+len(c.Followers))
		if c.Leader != nil {
			allNodes = append(allNodes, c.Leader)
		}
		allNodes = append(allNodes, c.Followers...)
		shards = append(shards, &shardInfo{
			minKey:   c.MinKey,
			maxKey:   c.MaxKey,
			leader:   c.Leader,
			allNodes: allNodes,
		})
	}
	return &ShardRouter{shards: shards}
}

// RouteRead returns a node from the owning shard using atomic round-robin,
// distributing reads pseudo-fairly across the leader and all followers.
func (r *ShardRouter) RouteRead(key string) (NodeClient, error) {
	s := r.findShard(key)
	if s == nil {
		return nil, fmt.Errorf("no remote shard found for key %q", key)
	}
	if len(s.allNodes) == 0 {
		return nil, fmt.Errorf("shard for key %q has no available nodes", key)
	}
	idx := s.readIdx.Add(1) - 1
	return s.allNodes[idx%uint64(len(s.allNodes))], nil
}

// RouteWrite returns the leader of the owning shard.
// All writes must go to the leader to preserve consistency.
func (r *ShardRouter) RouteWrite(key string) (NodeClient, error) {
	s := r.findShard(key)
	if s == nil {
		return nil, fmt.Errorf("no remote shard found for key %q", key)
	}
	if s.leader == nil {
		return nil, fmt.Errorf("shard for key %q has no leader configured", key)
	}
	return s.leader, nil
}

func (r *ShardRouter) findShard(key string) *shardInfo {
	for _, s := range r.shards {
		if s.ownsKey(key) {
			return s
		}
	}
	return nil
}
