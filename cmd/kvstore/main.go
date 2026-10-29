package main

import (
	"context"
	"kvstore/pkg/kvstore"
	coordinator "kvstore/pkg/kvstore/coordinator"
	grpcInterface "kvstore/pkg/kvstore/interface/grpc"
	ctrl "kvstore/pkg/kvstore/interface/http"
	storageengine "kvstore/pkg/kvstore/storageEngine"
	"log"
	"os"
	"time"
)

func main() {
	// Configure logging
	log.SetOutput(os.Stdout)

	// Read the config file
	configFile, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config.yaml: %v", err)
	}

	config, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("%v", err)
	}

	engine, err := storageengine.OpenStorageEngine(
		config.storage.memtableSizeMb*1024*1024,
		config.storage.sstSizeMb*1024*1024,
		config.storage.l0SizeMb*1024*1024,
		config.storage.lvlGrowthFactor,
	)
	if err != nil {
		log.Fatalf("Failed to open storage engine: %v", err)
	}

	// Determine this node's key range, leadership role, and remote shards.
	topo := resolveTopology(config, config.server.nodeId)
	minKey, maxKey, isLeader := topo.minKey, topo.maxKey, topo.isLeader

	// Build a lookup of node ID --> gRPC address.
	nodeAddrs := make(map[string]string, len(config.cluster.nodes))
	for _, node := range config.cluster.nodes {
		nodeAddrs[node.id] = node.grpcAddr
	}

	// Build a NodeClientAdapter for each follower of this shard (leader only).
	var followers []coordinator.NodeClient
	if isLeader {
		for _, followerId := range topo.followerIDs {
			addr, ok := nodeAddrs[followerId]
			if !ok {
				log.Fatalf("No gRPC address found for follower node %q", followerId)
			}
			adapter, adapterErr := grpcInterface.NewNodeClientAdapter(addr)
			if adapterErr != nil {
				log.Fatalf("Failed to connect to follower %s at %s: %v", followerId, addr, adapterErr)
			}
			followers = append(followers, adapter)
		}
	}

	// Build a ShardRouter for forwarding reads and writes to remote shards.
	// Reads round-robin across all nodes; writes always target the shard leader.
	var routerConfigs []coordinator.ShardNodeConfig
	for _, shard := range topo.remoteShards {
		leaderAddr, ok := nodeAddrs[shard.leader]
		if !ok {
			log.Fatalf("No gRPC address found for shard leader %q", shard.leader)
		}
		leaderAdapter, adapterErr := grpcInterface.NewNodeClientAdapter(leaderAddr)
		if adapterErr != nil {
			log.Fatalf("Failed to connect to shard leader %s at %s: %v", shard.leader, leaderAddr, adapterErr)
		}

		shardConfig := coordinator.ShardNodeConfig{
			MinKey: shard.startKey,
			MaxKey: shard.endKey,
			Leader: leaderAdapter,
		}
		for _, followerId := range shard.followers {
			addr, ok := nodeAddrs[followerId]
			if !ok {
				log.Printf("Warning: no gRPC address for follower %q, skipping", followerId)
				continue
			}
			followerAdapter, adapterErr := grpcInterface.NewNodeClientAdapter(addr)
			if adapterErr != nil {
				log.Printf("Warning: failed to connect to follower %s at %s: %v", followerId, addr, adapterErr)
				continue
			}
			shardConfig.Followers = append(shardConfig.Followers, followerAdapter)
		}
		routerConfigs = append(routerConfigs, shardConfig)
	}
	router := coordinator.NewShardRouter(routerConfigs)

	// Build a leaderClient so followers of this shard can proxy writes to the leader.
	var leaderClient coordinator.NodeClient
	if !isLeader && topo.leaderID != "" {
		addr, ok := nodeAddrs[topo.leaderID]
		if !ok {
			log.Fatalf("No gRPC address for this shard's leader %q", topo.leaderID)
		}
		adapter, adapterErr := grpcInterface.NewNodeClientAdapter(addr)
		if adapterErr != nil {
			log.Fatalf("Failed to connect to shard leader %s at %s: %v", topo.leaderID, addr, adapterErr)
		}
		leaderClient = adapter
	}

	coord := coordinator.NewCoordinator(engine, router, leaderClient, minKey, maxKey, isLeader, followers)

	// Check if this node needs to recover state from peers before serving traffic:
	// - Follower recovery: recovers from the leader if empty or behind.
	// - Peer-assisted leader recovery: recovers from the most up-to-date follower if empty.
	if (!isLeader && leaderClient != nil) || (isLeader && len(followers) > 0) {
		recoveryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := coord.RecoverIfNeeded(recoveryCtx); err != nil {
			log.Printf("Warning: startup recovery failed: %v", err)
		}
		cancel()
	}

	// Build HTTP handler timeouts from config, falling back to safe defaults if
	// the fields were omitted from config.yaml (zero value means not set).
	const defaultGetTimeout = 5 * time.Second
	const defaultPutTimeout = 10 * time.Second
	const defaultDeleteTimeout = 10 * time.Second

	getTimeout := time.Duration(config.server.timeouts.getTimeoutS) * time.Second
	if getTimeout <= 0 {
		getTimeout = defaultGetTimeout
	}
	putTimeout := time.Duration(config.server.timeouts.putTimeoutS) * time.Second
	if putTimeout <= 0 {
		putTimeout = defaultPutTimeout
	}
	deleteTimeout := time.Duration(config.server.timeouts.deleteTimeoutS) * time.Second
	if deleteTimeout <= 0 {
		deleteTimeout = defaultDeleteTimeout
	}

	storeCtrl := ctrl.NewStoreController(coord, ctrl.StoreTimeouts{
		Get:    getTimeout,
		Put:    putTimeout,
		Delete: deleteTimeout,
	})
	monCtrl := ctrl.NewMonitoringController(engine)
	grpcSrv := grpcInterface.NewGRPCServer(config.server.nodeId, coord)

	kvstore := kvstore.NewKVStore(storeCtrl, monCtrl, grpcSrv, config.server.nodeId)

	err = kvstore.Start(config.server.httpPort, config.server.grpcPort)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

/* ====================================================================================
	CONFIG STRUCTS
==================================================================================== */

type config struct {
	server  serverConfig  `yaml:"server"`
	storage storageConfig `yaml:"storage"`
	cluster clusterConfig `yaml:"cluster"`
}

type serverConfig struct {
	nodeId   string         `yaml:"node_id"`
	httpPort int            `yaml:"http_port"`
	grpcPort int            `yaml:"grpc_port"`
	dataDir  string         `yaml:"data_dir"`
	timeouts timeoutsConfig `yaml:"timeouts"`
}

type timeoutsConfig struct {
	getTimeoutS    int `yaml:"get_timeout_s"`
	putTimeoutS    int `yaml:"put_timeout_s"`
	deleteTimeoutS int `yaml:"delete_timeout_s"`
}

type storageConfig struct {
	memtableSizeMb  uint64 `yaml:"memtable_size_mb"`
	sstSizeMb       uint64 `yaml:"sst_size_mb"`
	l0SizeMb        uint64 `yaml:"level0_max_size_mb"`
	lvlGrowthFactor int    `yaml:"level_size_multiplier"`
}

type clusterConfig struct {
	nodes  []nodeConfig  `yaml:"nodes"`
	shards []shardConfig `yaml:"shards"`
}

type nodeConfig struct {
	id       string `yaml:"id"`
	grpcAddr string `yaml:"grpc_addr"`
}

type shardConfig struct {
	id        string   `yaml:"id"`
	startKey  string   `yaml:"start_key"`
	endKey    string   `yaml:"end_key"`
	leader    string   `yaml:"leader"`
	followers []string `yaml:"followers"`
}
