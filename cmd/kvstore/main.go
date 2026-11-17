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
		config.Storage.MemtableSizeMb*1024*1024,
		config.Storage.SstSizeMb*1024*1024,
		config.Storage.L0SizeMb*1024*1024,
		config.Storage.LvlGrowthFactor,
	)
	if err != nil {
		log.Fatalf("Failed to open storage engine: %v", err)
	}

	// Determine this node's key range, leadership role, and remote shards.
	topo := resolveTopology(config, config.Server.NodeId)
	minKey, maxKey, isLeader := topo.minKey, topo.maxKey, topo.isLeader

	// Build a lookup of node ID --> gRPC address.
	nodeAddrs := make(map[string]string, len(config.Cluster.Nodes))
	for _, node := range config.Cluster.Nodes {
		nodeAddrs[node.Id] = node.GrpcAddr
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
		leaderAddr, ok := nodeAddrs[shard.Leader]
		if !ok {
			log.Fatalf("No gRPC address found for shard leader %q", shard.Leader)
		}
		leaderAdapter, adapterErr := grpcInterface.NewNodeClientAdapter(leaderAddr)
		if adapterErr != nil {
			log.Fatalf("Failed to connect to shard leader %s at %s: %v", shard.Leader, leaderAddr, adapterErr)
		}

		shardConfig := coordinator.ShardNodeConfig{
			MinKey: shard.StartKey,
			MaxKey: shard.EndKey,
			Leader: leaderAdapter,
		}
		for _, followerId := range shard.Followers {
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

	getTimeout := time.Duration(config.Server.Timeouts.GetTimeoutS) * time.Second
	if getTimeout <= 0 {
		getTimeout = defaultGetTimeout
	}
	putTimeout := time.Duration(config.Server.Timeouts.PutTimeoutS) * time.Second
	if putTimeout <= 0 {
		putTimeout = defaultPutTimeout
	}
	deleteTimeout := time.Duration(config.Server.Timeouts.DeleteTimeoutS) * time.Second
	if deleteTimeout <= 0 {
		deleteTimeout = defaultDeleteTimeout
	}

	storeCtrl := ctrl.NewStoreController(coord, ctrl.StoreTimeouts{
		Get:    getTimeout,
		Put:    putTimeout,
		Delete: deleteTimeout,
	})
	monCtrl := ctrl.NewMonitoringController(engine)
	grpcSrv := grpcInterface.NewGRPCServer(config.Server.NodeId, coord)

	kvstore := kvstore.NewKVStore(storeCtrl, monCtrl, grpcSrv, config.Server.NodeId)

	err = kvstore.Start(config.Server.HttpPort, config.Server.GrpcPort)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

/* ====================================================================================
	CONFIG STRUCTS
==================================================================================== */

// Fields are exported so yaml.v3 can populate them; the decoder skips
// unexported fields, which would leave the whole config at its zero value.

type config struct {
	Server  serverConfig  `yaml:"server"`
	Storage storageConfig `yaml:"storage"`
	Cluster clusterConfig `yaml:"cluster"`
}

type serverConfig struct {
	NodeId   string         `yaml:"node_id"`
	HttpPort int            `yaml:"http_port"`
	GrpcPort int            `yaml:"grpc_port"`
	DataDir  string         `yaml:"data_dir"`
	Timeouts timeoutsConfig `yaml:"timeouts"`
}

type timeoutsConfig struct {
	GetTimeoutS    int `yaml:"get_timeout_s"`
	PutTimeoutS    int `yaml:"put_timeout_s"`
	DeleteTimeoutS int `yaml:"delete_timeout_s"`
}

type storageConfig struct {
	MemtableSizeMb  uint64 `yaml:"memtable_size_mb"`
	SstSizeMb       uint64 `yaml:"sst_size_mb"`
	L0SizeMb        uint64 `yaml:"level0_max_size_mb"`
	LvlGrowthFactor int    `yaml:"level_size_multiplier"`
}

type clusterConfig struct {
	Nodes  []nodeConfig  `yaml:"nodes"`
	Shards []shardConfig `yaml:"shards"`
}

type nodeConfig struct {
	Id       string `yaml:"id"`
	GrpcAddr string `yaml:"grpc_addr"`
}

type shardConfig struct {
	Id        string   `yaml:"id"`
	StartKey  string   `yaml:"start_key"`
	EndKey    string   `yaml:"end_key"`
	Leader    string   `yaml:"leader"`
	Followers []string `yaml:"followers"`
}
