//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"syscall"
	"testing"
)

// I2: single node, no peers.

func TestI2_CRUD(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	c.startAll()
	n := c.nodes[0]

	large := strings.Repeat("L", 512*1024)
	cases := []struct{ key, value string }{
		{"alpha", "1"},
		{"empty-value", ""},
		{"large-value", large},
		{"with space", "s p a c e"},
		{"ünïcødé-キー", "値"},
		{"json-ish", `{"nested": "quotes \" and \\ backslash"}`},
	}
	for _, tc := range cases {
		n.mustPut(t, "", tc.key, tc.value)
	}
	for _, tc := range cases {
		if err := n.hasValue(tc.key, tc.value); err != nil {
			t.Error(err)
		}
	}

	// Overwrite.
	n.mustPut(t, "", "alpha", "2")
	if err := n.hasValue("alpha", "2"); err != nil {
		t.Errorf("overwrite: %v", err)
	}

	// Missing key.
	if err := n.isAbsent("never-written"); err != nil {
		t.Errorf("missing key: %v", err)
	}

	// Delete, then read and re-create.
	n.mustDel(t, "", "alpha")
	if err := n.isAbsent("alpha"); err != nil {
		t.Errorf("after delete: %v", err)
	}
	n.mustPut(t, "", "alpha", "3")
	if err := n.hasValue("alpha", "3"); err != nil {
		t.Errorf("re-create after delete: %v", err)
	}

	// Bad requests.
	for _, bad := range []struct{ name, body string }{
		{"malformed JSON", `{"key": "x", "value": `},
		{"missing key", `{"value": "v"}`},
		{"empty key", `{"key": "", "value": "v"}`},
	} {
		r, err := n.do(http.MethodPost, "/kv", []byte(bad.body))
		if err != nil || r.status != http.StatusBadRequest {
			t.Errorf("POST /kv %s: got %v err=%v, want 400", bad.name, r, err)
		}
	}
}

// B-7: deleting a key that never existed must be 404 and must not drive the key
// count negative.
func TestI2_DeleteMissingKey(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	c.startAll()
	n := c.nodes[0]

	r, err := n.del("ghost")
	if err != nil {
		t.Fatal(err)
	}
	if r.status != http.StatusNotFound {
		t.Errorf("%s: DELETE of never-written key: got %v, want 404", bugB7, r)
	}
	keys, err := n.metric("kv_keys_total")
	if err != nil {
		t.Fatal(err)
	}
	if keys != 0 {
		t.Errorf("%s: kv_keys_total after deleting a missing key on an empty node: got %v, want 0", bugB7, keys)
	}
}

// B-I13: GET / returns service info that names this node.
func TestI2_ServiceInfo(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	c.startAll()
	n := c.nodes[0]

	r, err := n.do(http.MethodGet, "/", nil)
	if err != nil || r.status != http.StatusOK {
		t.Fatalf("GET /: got %v err=%v, want 200", r, err)
	}
	var info struct {
		Service string `json:"service"`
		Status  string `json:"status"`
		NodeID  string `json:"node_id"`
	}
	if err := json.Unmarshal([]byte(r.body), &info); err != nil {
		t.Fatalf("GET /: body is not JSON: %v\n%s", err, r.body)
	}
	if info.Service != "kvstore" || info.Status != "running" {
		t.Errorf("GET /: service=%q status=%q, want kvstore/running", info.Service, info.Status)
	}
	if info.NodeID != n.id {
		t.Errorf("%s: GET / node_id: got %q, want %q", bugBI13, info.NodeID, n.id)
	}
}

// Data survives a restart after SIGTERM and after SIGKILL (WAL replay), including
// overwrites and deletes.
func TestI2_PersistenceAcrossRestart(t *testing.T) {
	requireBoot(t)
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(sig.String(), func(t *testing.T) {
			c := newCluster(t, topoOneNode(), defaultOpts())
			c.startAll()
			n := c.nodes[0]

			want := map[string]string{}
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("p%03d", i)
				n.mustPut(t, "", k, "v1-"+k)
				want[k] = "v1-" + k
			}
			for i := 0; i < 200; i += 3 {
				k := fmt.Sprintf("p%03d", i)
				n.mustPut(t, "", k, "v2-"+k)
				want[k] = "v2-" + k
			}
			for i := 1; i < 200; i += 7 {
				k := fmt.Sprintf("p%03d", i)
				n.mustDel(t, "", k)
				delete(want, k)
			}

			if sig == syscall.SIGKILL {
				n.kill()
			} else {
				n.term()
			}
			c.startNode(n)
			checkModel(t, "", n, 200, "p%03d", want)
		})
	}
}

// B-I18: the very first write to a fresh node must survive a restart, whether
// the node is stopped with SIGTERM or SIGKILL. The WAL write is fsynced; the
// loss happens on replay (seq 0 is treated as already persisted in an SST).
func TestI2_FirstWriteSurvivesRestart(t *testing.T) {
	requireBoot(t)
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(sig.String(), func(t *testing.T) {
			c := newCluster(t, topoOneNode(), defaultOpts())
			c.startAll()
			n := c.nodes[0]
			n.mustPut(t, "", "first", "v0")
			n.mustPut(t, "", "second", "v1")
			if sig == syscall.SIGKILL {
				n.kill()
			} else {
				n.term()
			}
			c.startNode(n)
			if err := n.hasValue("second", "v1"); err != nil {
				t.Errorf("second write after restart: %v", err)
			}
			if err := n.hasValue("first", "v0"); err != nil {
				t.Errorf("%s: first write after restart: %v", bugBI18, err)
			}
		})
	}
}

// B-2: enough data to force memtable flushes and compaction, across restarts.
// The newest value of every key must win after each restart.
func TestI2_FlushCompactionAcrossRestart(t *testing.T) {
	requireBoot(t)
	o := defaultOpts()
	o.memtableMB, o.sstMB, o.l0MB, o.multiplier = 1, 1, 1, 2
	c := newCluster(t, topoOneNode(), o)
	c.startAll()
	n := c.nodes[0]

	const keys = 48
	val := func(gen, i int) string {
		return fmt.Sprintf("gen%d-%03d-", gen, i) + strings.Repeat("x", 64*1024)
	}
	want := map[string]string{}
	writeGen := func(gen int) {
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("c%03d", i)
			n.mustPut(t, "", k, val(gen, i))
			want[k] = val(gen, i)
		}
	}
	waitCounter := func(name string, min float64) {
		t.Helper()
		err := eventually(t, replTimeout, func() error {
			v, err := n.metric(name)
			if err != nil {
				return err
			}
			if v < min {
				return fmt.Errorf("%s = %v, want >= %v", name, v, min)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("harness: workload did not trigger background work: %v", err)
		}
	}

	// Generation 1: ~3 MB with a 1 MB memtable -> several flushes and compaction.
	writeGen(1)
	waitCounter("kv_flush_runs_total", 2)
	checkModel(t, "", n, keys, "c%03d", want)

	// Restart, then overwrite every key so that newer SSTs share keys with the
	// SSTs recovered from disk.
	c.restart(n)
	checkModel(t, bugB2, n, keys, "c%03d", want)
	writeGen(2)
	waitCounter("kv_flush_runs_total", 2)
	waitCounter("kv_compaction_runs_total", 1)
	checkModel(t, bugB2, n, keys, "c%03d", want)

	// Once more, so recovered L0/L1 files from two generations coexist.
	c.restart(n)
	checkModel(t, bugB2, n, keys, "c%03d", want)
	writeGen(3)
	waitCounter("kv_flush_runs_total", 2)
	waitCounter("kv_compaction_runs_total", 1)
	checkModel(t, bugB2, n, keys, "c%03d", want)
	c.restart(n)
	checkModel(t, bugB2, n, keys, "c%03d", want)
}

// checkModel asserts that keys fmt(0..count-1) on n match want (absent keys must
// be 404). It reports at most 5 mismatches.
func checkModel(t *testing.T, prefix string, n *node, count int, format string, want map[string]string) {
	t.Helper()
	var bad []string
	for i := 0; i < count; i++ {
		k := fmt.Sprintf(format, i)
		var err error
		if v, ok := want[k]; ok {
			err = n.hasValue(k, v)
		} else {
			err = n.isAbsent(k)
		}
		if err != nil {
			bad = append(bad, err.Error())
		}
	}
	if nBad := len(bad); nBad > 0 {
		if nBad > 5 {
			bad = append(bad[:5], fmt.Sprintf("... and %d more", nBad-5))
		}
		t.Errorf("%s%d/%d keys wrong on %s (got vs want):\n  %s", tag(prefix), nBad, count, n.id, strings.Join(bad, "\n  "))
	}
}
