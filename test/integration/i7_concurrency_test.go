//go:build integration

package integration

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// I7: concurrency, observability, and an opt-in soak.

// Many clients against every node of T-single, with distinct keys per client and
// a few hot keys shared by all. Afterwards every node must agree with the leader.
func TestI7_ConcurrentClients(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader := c.node("n0")

	const clients, opsPerClient, hotKeys = 8, 60, 5
	written := make([]map[string]string, clients) // per-client exact model of its own keys
	hotValues := map[string]bool{}
	var hotMu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan string, clients)
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci)))
			model := map[string]string{}
			for op := 0; op < opsPerClient; op++ {
				via := c.nodes[rng.Intn(len(c.nodes))]
				var key, val string
				if op%4 == 3 {
					key = fmt.Sprintf("hot%d", rng.Intn(hotKeys))
					val = fmt.Sprintf("c%d-op%d", ci, op)
					hotMu.Lock()
					hotValues[val] = true
					hotMu.Unlock()
				} else {
					key = fmt.Sprintf("cc%d-%02d", ci, rng.Intn(15))
					val = fmt.Sprintf("op%d", op)
				}
				if !strings.HasPrefix(key, "hot") && rng.Intn(5) == 0 {
					r, err := via.del(key)
					// 404 is the correct answer for a key this client never wrote.
					if err != nil || (r.status != http.StatusOK && r.status != http.StatusNotFound) {
						errs <- fmt.Sprintf("DELETE %q via %s: got %v err=%v", key, via.id, r, err)
						return
					}
					delete(model, key)
					continue
				}
				r, err := via.put(key, val)
				if err != nil || r.status != http.StatusOK {
					errs <- fmt.Sprintf("PUT %q via %s: got %v err=%v, want 200", key, via.id, r, err)
					return
				}
				if !strings.HasPrefix(key, "hot") {
					model[key] = val
				}
			}
			written[ci] = model
		}(ci)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	// Distinct keys: exact per-client model, on every node.
	for ci, model := range written {
		format := fmt.Sprintf("cc%d-%%02d", ci)
		checkModel(t, "", leader, 15, format, model)
		for _, f := range []*node{c.node("n1"), c.node("n2")} {
			awaitModel(t, bugB3, f, 15, format, model)
		}
	}

	// Hot keys: the leader holds one of the written values, and followers converge to it.
	for h := 0; h < hotKeys; h++ {
		key := fmt.Sprintf("hot%d", h)
		st, lv, err := leader.getValue(key)
		if err != nil {
			t.Fatal(err)
		}
		if st == http.StatusNotFound {
			continue // never picked by any client
		}
		if st != http.StatusOK || !hotValues[lv] {
			t.Errorf("leader %q: status %d value %q, want one of the written values", key, st, lv)
			continue
		}
		for _, f := range []*node{c.node("n1"), c.node("n2")} {
			if err := eventually(t, replTimeout, func() error { return f.hasValue(key, lv) }); err != nil {
				t.Errorf("%s: hot key on follower vs leader: %v", bugB3, err)
			}
		}
	}
}

// /metrics responds and its request counters and key gauge reflect activity.
func TestI7_MetricsReflectActivity(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	c.startAll()
	n := c.nodes[0]

	names := []string{
		"kv_get_requests_total", "kv_put_requests_total", "kv_delete_requests_total",
		"kv_keys_total", "kv_wal_size_bytes", "kv_sstable_disk_usage_bytes",
		"kv_memtable_size_bytes", "kv_flush_runs_total", "kv_compaction_runs_total",
	}
	before := map[string]float64{}
	for _, name := range names {
		v, err := n.metric(name)
		if err != nil {
			t.Fatal(err)
		}
		before[name] = v
	}

	for i := 0; i < 5; i++ {
		n.mustPut(t, "", fmt.Sprintf("m%d", i), "v")
	}
	for i := 0; i < 3; i++ {
		if err := n.hasValue(fmt.Sprintf("m%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	n.mustDel(t, "", "m3")
	n.mustDel(t, "", "m4")

	deltas := map[string]float64{"kv_put_requests_total": 5, "kv_get_requests_total": 3, "kv_delete_requests_total": 2}
	for name, d := range deltas {
		v, err := n.metric(name)
		if err != nil {
			t.Fatal(err)
		}
		if v-before[name] != d {
			t.Errorf("%s: went from %v to %v, want +%v", name, before[name], v, d)
		}
	}
	keys, err := n.metric("kv_keys_total")
	if err != nil {
		t.Fatal(err)
	}
	if keys != 3 {
		t.Errorf("kv_keys_total after 5 distinct puts and 2 deletes: got %v, want 3", keys)
	}
	wal, err := n.metric("kv_wal_size_bytes")
	if err != nil {
		t.Fatal(err)
	}
	if wal <= 0 {
		t.Errorf("kv_wal_size_bytes after writes: got %v, want > 0", wal)
	}
}

// TestSoak is opt-in (go test ... -run Soak). Several clients write and delete
// their own keys through every node of T-single while followers are repeatedly
// killed and restarted. At the end the leader must match the client model
// exactly, and so must every follower after a final restart (recovery).
// KVSTORE_SOAK_DURATION (e.g. "10m") sets the length; default 2m.
func TestSoak(t *testing.T) {
	if f := flag.Lookup("test.run"); f == nil || !strings.Contains(f.Value.String(), "Soak") {
		t.Skip("opt-in soak; run with -run Soak")
	}
	requireBoot(t)
	dur := 2 * time.Minute
	if s := os.Getenv("KVSTORE_SOAK_DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatalf("KVSTORE_SOAK_DURATION: %v", err)
		}
		dur = d
	}
	o := defaultOpts()
	o.memtableMB, o.sstMB, o.l0MB, o.multiplier = 1, 1, 2, 2 // keep flush/compaction busy
	c := newCluster(t, topoSingle(), o)
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")

	const clients, keysPerClient, soakOpsPerRound = 6, 200, 300
	stop := make(chan struct{})
	models := make([]map[string]string, clients)
	var wg sync.WaitGroup
	errs := make(chan string, clients)
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + ci)))
			model := map[string]string{}
			defer func() { models[ci] = model }()
			payload := strings.Repeat("p", 2048)
			for op := 0; ; op++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("soak%d-%03d", ci, rng.Intn(keysPerClient))
				// Writes go to the leader: followers come and go, and a proxied
				// write that fails while its node restarts has an unknown outcome.
				if rng.Intn(6) == 0 {
					r, err := leader.del(key)
					if err != nil || (r.status != http.StatusOK && r.status != http.StatusNotFound) {
						errs <- fmt.Sprintf("DELETE %q via leader: got %v err=%v", key, r, err)
						return
					}
					delete(model, key)
					continue
				}
				val := fmt.Sprintf("op%d-%s", op, payload)
				r, err := leader.put(key, val)
				if err != nil || r.status != http.StatusOK {
					errs <- fmt.Sprintf("PUT %q via leader: got %v err=%v, want 200", key, r, err)
					return
				}
				model[key] = val
				// Follower reads must never error (stale values are allowed mid-run).
				f := c.nodes[1+rng.Intn(2)]
				if f.running() {
					if gr, err := f.get(key); err == nil && gr.status != http.StatusOK && gr.status != http.StatusNotFound {
						errs <- fmt.Sprintf("GET %q on %s: got %v", key, f.id, gr)
						return
					}
				}
			}
		}(ci)
	}

	// Chaos: alternately kill and restart a follower until the deadline.
	deadline := time.Now().Add(dur)
	for i := 0; time.Now().Before(deadline); i++ {
		f := []*node{n1, n2}[i%2]
		f.kill()
		c.startNode(f)
		// Let the follower serve under load before the next round: wait until the
		// leader has acknowledged soakOpsPerRound more writes (progress-based, not
		// a sleep), or the soak deadline passes. The follower must stay up.
		base, err := leader.metric("kv_put_requests_total")
		if err != nil {
			t.Errorf("leader /metrics during soak: %v", err)
			break // not Fatal: close(stop) below must still run
		}
		if err := eventually(t, dur, func() error {
			if !f.running() {
				return nil // reported below
			}
			if time.Now().After(deadline) {
				return nil
			}
			v, err := leader.metric("kv_put_requests_total")
			if err != nil {
				return err
			}
			if v-base < soakOpsPerRound {
				return fmt.Errorf("leader acknowledged %v writes since %s restarted, want %d", v-base, f.id, soakOpsPerRound)
			}
			return nil
		}); err != nil {
			t.Errorf("soak stalled: %v", err)
		}
		if !f.running() {
			t.Errorf("follower %s exited under soak load: %v", f.id, f.exitErr())
			break
		}
		if _, err := f.metric("kv_put_requests_total"); err != nil {
			t.Errorf("follower %s stopped serving during soak: %v", f.id, err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}

	for ci, model := range models {
		format := fmt.Sprintf("soak%d-%%03d", ci)
		checkModel(t, "", leader, keysPerClient, format, model)
	}
	// Followers missed writes while down (replication is fire-and-forget); a
	// final restart must bring each one to the leader's exact state.
	for _, f := range []*node{n1, n2} {
		c.restart(f)
		for ci, model := range models {
			format := fmt.Sprintf("soak%d-%%03d", ci)
			checkModel(t, bugB5, f, keysPerClient, format, model)
		}
	}
}

// B-I15 / B-I16: kv_keys_total is the number of live keys: overwrites do not
// change it, and it survives a restart.
func TestI7_KeysTotalGauge(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoOneNode(), defaultOpts())
	c.startAll()
	n := c.nodes[0]

	keysTotal := func() float64 {
		t.Helper()
		v, err := n.metric("kv_keys_total")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, k := range []string{"g1", "g2", "g3"} {
		n.mustPut(t, "", k, "v1")
	}
	if got := keysTotal(); got != 3 {
		t.Fatalf("kv_keys_total after 3 distinct puts: got %v, want 3", got)
	}
	n.mustPut(t, "", "g1", "v2")
	n.mustPut(t, "", "g1", "v3")
	if got := keysTotal(); got != 3 {
		t.Errorf("%s: kv_keys_total after overwriting g1 twice: got %v, want 3", bugBI15, got)
	}

	c.restart(n)
	if err := n.hasValue("g1", "v3"); err != nil {
		t.Fatalf("after restart: %v", err)
	}
	if got := keysTotal(); got != 3 {
		t.Errorf("%s: kv_keys_total after restart with 3 live keys on disk: got %v, want 3", bugBI16, got)
	}
}
