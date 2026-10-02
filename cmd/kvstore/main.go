package main

import (
	"kvstore/pkg/kvstore"
	grpcInterface "kvstore/pkg/kvstore/interface/grpc"
	ctrl "kvstore/pkg/kvstore/interface/http"
	service "kvstore/pkg/kvstore/service"
	storageengine "kvstore/pkg/kvstore/storageEngine"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	// Configure logging
	log.SetOutput(os.Stdout)

	// Read the config file
	configFile, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config.yaml: %v", err)
	}

	var config config
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("Failed to parse config.yaml: %v", err)
	}

	if config.storage.l0SizeMb == 0 {
		log.Fatalf("Invalid configuration: level0_max_size_mb must be greater than 0")
	}
	if config.storage.lvlGrowthFactor <= 1 {
		log.Fatalf("Invalid configuration: level_size_multiplier must be greater than 1")
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

	// Determine this node's key range and leadership role from the shard config.
	var minKey, maxKey string
	isLeader := false
	var followerIds []string
	for _, shard := range config.cluster.shards {
		if shard.leader == config.server.nodeId {
			minKey = shard.startKey
			maxKey = shard.endKey
			isLeader = true
			followerIds = shard.followers
			break
		}
		for _, follower := range shard.followers {
			if follower == config.server.nodeId {
				minKey = shard.startKey
				maxKey = shard.endKey
				// isLeader stays false
				break
			}
		}
		if minKey != "" {
			break
		}
	}

	// Build a lookup of node ID --> gRPC address.
	nodeAddrs := make(map[string]string, len(config.cluster.nodes))
	for _, node := range config.cluster.nodes {
		nodeAddrs[node.id] = node.grpcAddr
	}

	// Build a NodeClientAdapter for each follower of this shard (leader only).
	var followers []service.NodeClient
	if isLeader {
		for _, followerId := range followerIds {
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

	// Build a NodeClientAdapter for request forwarding (first peer node that is
	// a leader of a different shard). Used to forward out-of-range keys.
	var nodeClient service.NodeClient
	for _, shard := range config.cluster.shards {
		if shard.leader != config.server.nodeId {
			addr, ok := nodeAddrs[shard.leader]
			if !ok {
				continue
			}
			adapter, adapterErr := grpcInterface.NewNodeClientAdapter(addr)
			if adapterErr != nil {
				log.Fatalf("Failed to connect to peer node %s at %s: %v", shard.leader, addr, adapterErr)
			}
			nodeClient = adapter
			break // single-peer for now; extend to a full router for multi-shard
		}
	}

	svc := service.NewService(engine, nodeClient, minKey, maxKey, isLeader, followers)


	storeCtrl := ctrl.NewStoreController(svc)
	monCtrl := ctrl.NewMonitoringController(engine)
	grpcSrv := grpcInterface.NewGRPCServer(svc)

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
	nodeId   string `yaml:"node_id"`
	httpPort int    `yaml:"http_port"`
	grpcPort int    `yaml:"grpc_port"`
	dataDir  string `yaml:"data_dir"`
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
