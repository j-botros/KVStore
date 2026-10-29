package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

/* ====================================================================================
	loadConfig (B-1)
==================================================================================== */

// repoConfig reads the config.yaml at the repository root.
func repoConfig(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "config.yaml"))
	if err != nil {
		t.Fatalf("read repo config.yaml: %v", err)
	}
	return data
}

const validStorageYAML = `
storage:
  memtable_size_mb: 64
  sst_size_mb: 64
  level0_max_size_mb: 256
  level_size_multiplier: 10
`

func TestLoadConfig_RepoConfig(t *testing.T) {
	cfg, err := loadConfig(repoConfig(t))
	if err != nil {
		t.Fatalf("BUG B-1 (config struct fields are unexported, so yaml.v3 populates nothing): "+
			"got error %q, want repo config.yaml to load", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"server.node_id", cfg.server.nodeId, "kvstore-0"},
		{"server.http_port", cfg.server.httpPort, 8080},
		{"server.grpc_port", cfg.server.grpcPort, 9090},
		{"server.timeouts.put_timeout_s", cfg.server.timeouts.putTimeoutS, 10},
		{"storage.level0_max_size_mb", cfg.storage.l0SizeMb, uint64(256)},
		{"storage.level_size_multiplier", cfg.storage.lvlGrowthFactor, 10},
		{"len(cluster.nodes)", len(cfg.cluster.nodes), 3},
		{"len(cluster.shards)", len(cfg.cluster.shards), 3},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("BUG B-1 (config struct fields are unexported, so yaml.v3 populates nothing): "+
				"%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	_, err := loadConfig([]byte("server: [unterminated"))
	if err == nil {
		t.Fatal("got nil error for malformed YAML, want parse error")
	}
	if !strings.Contains(err.Error(), "parse") {
		t.Errorf("got error %q, want it to mention parsing", err)
	}
}

func TestLoadConfig_RejectsZeroLevel0Size(t *testing.T) {
	yaml := `
storage:
  memtable_size_mb: 64
  sst_size_mb: 64
  level0_max_size_mb: 0
  level_size_multiplier: 10
`
	_, err := loadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "level0_max_size_mb") {
		t.Fatalf("got error %v, want level0_max_size_mb validation error", err)
	}
}

func TestLoadConfig_RejectsGrowthFactorOne(t *testing.T) {
	yaml := `
storage:
  memtable_size_mb: 64
  sst_size_mb: 64
  level0_max_size_mb: 256
  level_size_multiplier: 1
`
	_, err := loadConfig([]byte(yaml))
	if err == nil {
		t.Fatal("got nil error, want level_size_multiplier validation error")
	}
	if !strings.Contains(err.Error(), "level_size_multiplier") {
		t.Fatalf("BUG B-1 (config struct fields are unexported, so yaml.v3 populates nothing): "+
			"got error %q, want the level_size_multiplier validation error (level0_max_size_mb=256 was set)", err)
	}
}

func TestLoadConfig_ValidStorageOnly(t *testing.T) {
	_, err := loadConfig([]byte(validStorageYAML))
	if err != nil {
		t.Fatalf("BUG B-1 (config struct fields are unexported, so yaml.v3 populates nothing): "+
			"got error %q, want nil for valid storage settings", err)
	}
}

/* ====================================================================================
	resolveTopology (B-6)

	Configs are built directly as Go structs (not YAML) so these tests are
	independent of B-1.
==================================================================================== */

// repoClusterConfig mirrors the repo config.yaml: every node is in every shard.
func repoClusterConfig() config {
	return config{
		cluster: clusterConfig{
			nodes: []nodeConfig{
				{id: "kvstore-0", grpcAddr: "kvstore-0:9090"},
				{id: "kvstore-1", grpcAddr: "kvstore-1:9090"},
				{id: "kvstore-2", grpcAddr: "kvstore-2:9090"},
			},
			shards: []shardConfig{
				{id: "shard-1", startKey: "", endKey: "I", leader: "kvstore-0", followers: []string{"kvstore-1", "kvstore-2"}},
				{id: "shard-2", startKey: "I", endKey: "Q", leader: "kvstore-1", followers: []string{"kvstore-0", "kvstore-2"}},
				{id: "shard-3", startKey: "Q", endKey: "", leader: "kvstore-2", followers: []string{"kvstore-0", "kvstore-1"}},
			},
		},
	}
}

// disjointClusterConfig has each node in exactly one shard.
func disjointClusterConfig() config {
	return config{
		cluster: clusterConfig{
			shards: []shardConfig{
				{id: "s1", startKey: "", endKey: "M", leader: "a", followers: []string{"b"}},
				{id: "s2", startKey: "M", endKey: "", leader: "c", followers: []string{"d"}},
			},
		},
	}
}

func inRange(key, min, max string) bool {
	return (min == "" || key >= min) && (max == "" || key < max)
}

// shardFor returns the shard in cfg that owns key.
func shardFor(cfg config, key string) shardConfig {
	for _, s := range cfg.cluster.shards {
		if inRange(key, s.startKey, s.endKey) {
			return s
		}
	}
	return shardConfig{}
}

// routable reports whether a node with topology topo can serve key, either
// locally or through one of its remote shards.
func routable(topo topology, key string) bool {
	if inRange(key, topo.minKey, topo.maxKey) {
		return true
	}
	for _, s := range topo.remoteShards {
		if inRange(key, s.startKey, s.endKey) {
			return true
		}
	}
	return false
}

// boundaryKeys are keys at, just below, and just above each shard boundary.
var boundaryKeys = []string{"", "A", "H", "Hz", "Hzzz", "I", "I0", "Ia", "P", "Pzzz", "Q", "Q0", "Qa", "z", "\xff"}

func TestResolveTopology_DisjointShards(t *testing.T) {
	cfg := disjointClusterConfig()
	tests := []struct {
		node           string
		minKey, maxKey string
		isLeader       bool
		leaderID       string
		followerIDs    []string
		remoteShardIDs []string
	}{
		{"a", "", "M", true, "", []string{"b"}, []string{"s2"}},
		{"b", "", "M", false, "a", nil, []string{"s2"}},
		{"c", "M", "", true, "", []string{"d"}, []string{"s1"}},
		{"d", "M", "", false, "c", nil, []string{"s1"}},
	}
	for _, tc := range tests {
		t.Run(tc.node, func(t *testing.T) {
			topo := resolveTopology(cfg, tc.node)
			if topo.minKey != tc.minKey || topo.maxKey != tc.maxKey {
				t.Errorf("range = [%q,%q), want [%q,%q)", topo.minKey, topo.maxKey, tc.minKey, tc.maxKey)
			}
			if topo.isLeader != tc.isLeader {
				t.Errorf("isLeader = %v, want %v", topo.isLeader, tc.isLeader)
			}
			if topo.leaderID != tc.leaderID {
				t.Errorf("leaderID = %q, want %q", topo.leaderID, tc.leaderID)
			}
			if !slices.Equal(topo.followerIDs, tc.followerIDs) {
				t.Errorf("followerIDs = %v, want %v", topo.followerIDs, tc.followerIDs)
			}
			var gotRemote []string
			for _, s := range topo.remoteShards {
				gotRemote = append(gotRemote, s.id)
			}
			if !slices.Equal(gotRemote, tc.remoteShardIDs) {
				t.Errorf("remoteShards = %v, want %v", gotRemote, tc.remoteShardIDs)
			}
			for _, k := range boundaryKeys {
				if !routable(topo, k) {
					t.Errorf("key %q is neither local nor routed", k)
				}
			}
		})
	}
}

// Each node must be able to serve every key of the repo layout: either it is a
// member of the owning shard (local) or it routes to that shard.
func TestResolveTopology_RepoLayout_EveryKeyRoutable(t *testing.T) {
	cfg := repoClusterConfig()
	for _, node := range []string{"kvstore-0", "kvstore-1", "kvstore-2"} {
		t.Run(node, func(t *testing.T) {
			topo := resolveTopology(cfg, node)
			var unroutable []string
			for _, k := range boundaryKeys {
				if !routable(topo, k) {
					unroutable = append(unroutable, k+"("+shardFor(cfg, k).id+")")
				}
			}
			if len(unroutable) > 0 {
				t.Errorf("BUG B-6 (node resolves to one key range although it belongs to every shard; router is empty): "+
					"got local range [%q,%q) and %d remote shards, leaving keys %q unservable; want every key servable",
					topo.minKey, topo.maxKey, len(topo.remoteShards), unroutable)
			}
		})
	}
}

// A node that leads a shard must resolve as that shard's leader, owning its range.
func TestResolveTopology_RepoLayout_LeaderRole(t *testing.T) {
	cfg := repoClusterConfig()
	for _, s := range cfg.cluster.shards {
		t.Run(s.leader, func(t *testing.T) {
			topo := resolveTopology(cfg, s.leader)
			if !topo.isLeader || topo.minKey != s.startKey || topo.maxKey != s.endKey {
				t.Errorf("BUG B-6 (node resolves to the wrong shard/role when it is in several shards): "+
					"got isLeader=%v range [%q,%q), want leader of %s range [%q,%q)",
					topo.isLeader, topo.minKey, topo.maxKey, s.id, s.startKey, s.endKey)
			}
		})
	}
}

// When a node resolves as a follower, its leaderID must be the leader of the
// shard whose range it resolved to (otherwise writes are proxied to a node
// that does not own the key).
func TestResolveTopology_RepoLayout_LeaderIDMatchesRange(t *testing.T) {
	cfg := repoClusterConfig()
	for _, node := range []string{"kvstore-0", "kvstore-1", "kvstore-2"} {
		t.Run(node, func(t *testing.T) {
			topo := resolveTopology(cfg, node)
			if topo.isLeader {
				return
			}
			var want string
			for _, s := range cfg.cluster.shards {
				if s.startKey == topo.minKey && s.endKey == topo.maxKey {
					want = s.leader
				}
			}
			if topo.leaderID != want {
				t.Errorf("BUG B-6 (node resolves to the wrong shard/role when it is in several shards): "+
					"follower range [%q,%q): got leaderID %q, want %q",
					topo.minKey, topo.maxKey, topo.leaderID, want)
			}
		})
	}
}
