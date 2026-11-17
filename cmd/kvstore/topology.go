package main

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// loadConfig parses config.yaml contents and validates the storage settings.
func loadConfig(data []byte) (config, error) {
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return config{}, fmt.Errorf("Failed to parse config.yaml: %w", err)
	}

	if cfg.Storage.L0SizeMb == 0 {
		return config{}, fmt.Errorf("Invalid configuration: level0_max_size_mb must be greater than 0")
	}
	if cfg.Storage.LvlGrowthFactor <= 1 {
		return config{}, fmt.Errorf("Invalid configuration: level_size_multiplier must be greater than 1")
	}
	return cfg, nil
}

// topology is this node's view of the cluster, resolved from the shard config.
// It holds node IDs only; main.go resolves them to gRPC addresses and dials them.
type topology struct {
	minKey, maxKey string
	isLeader       bool
	leaderID       string        // this shard's leader; set only when !isLeader
	followerIDs    []string      // this shard's followers; set only when isLeader
	remoteShards   []shardConfig // shards handed to the ShardRouter
}

// resolveTopology determines this node's key range, leadership role, and the
// remote shards it must route to.
func resolveTopology(cfg config, nodeID string) topology {
	var t topology

	// Determine this node's key range and leadership role from the shard config.
	for _, shard := range cfg.Cluster.Shards {
		if shard.Leader == nodeID {
			t.minKey = shard.StartKey
			t.maxKey = shard.EndKey
			t.isLeader = true
			t.followerIDs = shard.Followers
			break
		}
		for _, follower := range shard.Followers {
			if follower == nodeID {
				t.minKey = shard.StartKey
				t.maxKey = shard.EndKey
				// isLeader stays false
				break
			}
		}
		if t.minKey != "" {
			break
		}
	}

	// Collect remote shards for the ShardRouter.
	for _, shard := range cfg.Cluster.Shards {
		// Skip this node's own shard — handled locally by Coordinator.
		isThisShard := shard.Leader == nodeID
		if !isThisShard {
			for _, f := range shard.Followers {
				if f == nodeID {
					isThisShard = true
					break
				}
			}
		}
		if isThisShard {
			continue
		}
		t.remoteShards = append(t.remoteShards, shard)
	}

	// Find this shard's leader so followers can proxy writes to it.
	if !t.isLeader {
		for _, shard := range cfg.Cluster.Shards {
			isMyFollowerShard := false
			for _, f := range shard.Followers {
				if f == nodeID {
					isMyFollowerShard = true
					break
				}
			}
			if !isMyFollowerShard {
				continue
			}
			t.leaderID = shard.Leader
			break
		}
	}

	return t
}
