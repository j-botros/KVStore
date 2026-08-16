package main

import (
	"kvstore/pkg/kvstore"
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

	engine := storageengine.NewStorageEngine(
		config.storage.memtableSizeMb*1024*1024,
		config.storage.sstSizeMb*1024*1024,
		config.storage.l0SizeMb*1024*1024,
		config.storage.lvlGrowthFactor,
	)

	service := service.NewService(engine)

	ctrl := ctrl.NewStoreController(service)

	kvstore := kvstore.NewKVStore(ctrl)

	err = kvstore.Start(config.server.httpPort, config.server.nodeId)
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
	walSizeMb       uint64 `yaml:"wal_segment_size_mb"`
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
