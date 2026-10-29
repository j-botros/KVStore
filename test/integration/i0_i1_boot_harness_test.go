//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// I0: the binary must boot with a valid config. Until B-1 is fixed this fails
// with the node log, and every other scenario fails fast via requireBoot.
func TestI0_BootWithValidConfig(t *testing.T) {
	if msg := bootCheck(); msg != "" {
		t.Fatal(msg)
	}
}

// I1: the generated config.yaml must contain every field the binary reads, so a
// boot failure cannot be blamed on the harness. Parsed with exported mirror
// structs (the same yaml.v3 the binary uses). Does not start the binary.
func TestI1_GeneratedConfigIsComplete(t *testing.T) {
	type mirror struct {
		Server struct {
			NodeID   string `yaml:"node_id"`
			HTTPPort int    `yaml:"http_port"`
			GRPCPort int    `yaml:"grpc_port"`
			Timeouts struct {
				Get    int `yaml:"get_timeout_s"`
				Put    int `yaml:"put_timeout_s"`
				Delete int `yaml:"delete_timeout_s"`
			} `yaml:"timeouts"`
		} `yaml:"server"`
		Storage struct {
			Memtable   uint64 `yaml:"memtable_size_mb"`
			SST        uint64 `yaml:"sst_size_mb"`
			L0         uint64 `yaml:"level0_max_size_mb"`
			Multiplier int    `yaml:"level_size_multiplier"`
		} `yaml:"storage"`
		Cluster struct {
			Nodes []struct {
				ID       string `yaml:"id"`
				GRPCAddr string `yaml:"grpc_addr"`
			} `yaml:"nodes"`
			Shards []struct {
				ID        string   `yaml:"id"`
				StartKey  string   `yaml:"start_key"`
				EndKey    string   `yaml:"end_key"`
				Leader    string   `yaml:"leader"`
				Followers []string `yaml:"followers"`
			} `yaml:"shards"`
		} `yaml:"cluster"`
	}

	for _, tp := range []topoSpec{topoOneNode(), topoSingle(), topoMulti()} {
		t.Run(tp.name, func(t *testing.T) {
			ps, err := freePorts(len(tp.nodeIDs))
			if err != nil {
				t.Fatal(err)
			}
			o := defaultOpts()
			for i, id := range tp.nodeIDs {
				var m mirror
				if err := yaml.Unmarshal([]byte(renderConfig(tp, i, ps, o)), &m); err != nil {
					t.Fatalf("%s: generated config does not parse: %v", id, err)
				}
				if m.Server.NodeID != id || m.Server.HTTPPort != ps[i].http || m.Server.GRPCPort != ps[i].grpc {
					t.Errorf("%s: server section = %+v", id, m.Server)
				}
				if m.Server.Timeouts.Get != o.getS || m.Server.Timeouts.Put != o.putS || m.Server.Timeouts.Delete != o.deleteS {
					t.Errorf("%s: timeouts = %+v", id, m.Server.Timeouts)
				}
				if m.Storage.Memtable != o.memtableMB || m.Storage.SST != o.sstMB || m.Storage.L0 != o.l0MB || m.Storage.Multiplier != o.multiplier {
					t.Errorf("%s: storage = %+v", id, m.Storage)
				}
				if len(m.Cluster.Nodes) != len(tp.nodeIDs) || len(m.Cluster.Shards) != len(tp.shards) {
					t.Fatalf("%s: cluster = %+v", id, m.Cluster)
				}
				for j, s := range tp.shards {
					got := m.Cluster.Shards[j]
					if got.StartKey != s.start || got.EndKey != s.end || got.Leader != tp.nodeIDs[s.leader] || len(got.Followers) != len(s.followers) {
						t.Errorf("%s: shard %d = %+v, want %+v", id, j, got, s)
					}
				}
				if !strings.HasPrefix(m.Cluster.Nodes[i].GRPCAddr, "127.0.0.1:") {
					t.Errorf("%s: grpc_addr %q", id, m.Cluster.Nodes[i].GRPCAddr)
				}
			}
		})
	}
}

// I1: lifecycle primitives work against the real binary: start, readiness,
// SIGTERM/SIGKILL stop, restart in the same dir, wipe.
func TestI1_NodeLifecycle(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	n := c.nodes[0]
	c.startAll()

	n.mustPut(t, "", "life", "1")
	if _, err := os.Stat(filepath.Join(n.dir, "data", "wal")); err != nil {
		t.Fatalf("expected data/wal in the node's own working dir: %v", err)
	}

	n.term()
	if n.running() {
		t.Fatal("node still running after term()")
	}
	c.startNode(n)
	// "life" is this engine's very first write (seq 0), so it also exercises B-I18.
	if err := n.hasValue("life", "1"); err != nil {
		t.Fatalf("%s: after restart: %v", bugBI18, err)
	}

	n.kill()
	if n.running() {
		t.Fatal("node still running after kill()")
	}
	c.wipe(n)
	if _, err := os.Stat(filepath.Join(n.dir, "data")); !os.IsNotExist(err) {
		t.Fatalf("data dir still present after wipe: %v", err)
	}
	c.startNode(n)
	if err := n.isAbsent("life"); err != nil {
		t.Fatalf("after wipe+restart: %v", err)
	}
}
