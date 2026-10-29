//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// I6: failure modes.

// slack covers process scheduling and gRPC overhead on top of a configured timeout.
const slack = 2 * time.Second

// expectFailFast checks that a request against an unavailable peer fails (not
// 2xx), within the configured timeout plus slack (no hang), with the status code
// a client can act on (B-8: currently 500 for everything).
func expectFailFast(t *testing.T, what string, r result, err error, timeout time.Duration, wantStatus int) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: client error %v (took %v), want an HTTP error response within %v", what, err, r.dur, timeout+slack)
		return
	}
	if r.status >= 200 && r.status < 300 {
		t.Errorf("%s: got %v, want an error status", what, r)
		return
	}
	if r.dur > timeout+slack {
		t.Errorf("%s: took %v, want <= configured timeout %v + %v slack", what, r.dur, timeout, slack)
	}
	if r.status != wantStatus {
		t.Errorf("%s: %s: got %v, want %d %s", bugB8, what, r, wantStatus, http.StatusText(wantStatus))
	}
}

// T-multi: with shard-2's leader (kvstore-1) down, writes to shard-2 fail fast
// through every other node, reads of shard-2 data are still served by its
// followers, and the other shards keep working.
func TestI6_ShardLeaderDown(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoMulti(), defaultOpts())
	c.startAll()
	c.settle(bugB6)
	k0, k1, k2 := c.node("kvstore-0"), c.node("kvstore-1"), c.node("kvstore-2")

	k1.mustPut(t, bugB6, "K-before", "kept")
	for _, n := range []*node{k0, k2} {
		if err := eventually(t, replTimeout, func() error { return n.hasValue("K-before", "kept") }); err != nil {
			t.Fatalf("%s: shard-2 write never reached %s: %v", bugB6, n.id, err)
		}
	}

	k1.kill()
	putTimeout := time.Duration(c.opts.putS) * time.Second
	delTimeout := time.Duration(c.opts.deleteS) * time.Second
	for _, n := range []*node{k0, k2} {
		r, err := n.put("K-during", "x")
		expectFailFast(t, fmt.Sprintf("PUT shard-2 key via %s with leader kvstore-1 down", n.id), r, err, putTimeout, http.StatusServiceUnavailable)
		r, err = n.del("K-before")
		expectFailFast(t, fmt.Sprintf("DELETE shard-2 key via %s with leader kvstore-1 down", n.id), r, err, delTimeout, http.StatusServiceUnavailable)
		if err := n.hasValue("K-before", "kept"); err != nil {
			t.Errorf("read of shard-2 data on follower %s with its leader down: %v", n.id, err)
		}
	}

	// Shards 1 and 3 are unaffected (their leaders are up; replication to the
	// downed kvstore-1 fails silently).
	for _, key := range []string{"A-during", "R-during"} {
		w := c.leaderOf(key)
		other := k0
		if w == k0 {
			other = k2
		}
		other.mustPut(t, bugB6, key, "ok")
		if err := w.hasValue(key, "ok"); err != nil {
			t.Errorf("%s: %v", bugB6, err)
		}
	}
}

// T-single: with the leader down, followers keep serving reads, and proxied writes
// fail fast; after the leader returns, proxied writes work again.
func TestI6_LeaderDown(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")

	want := map[string]string{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("ld%02d", i)
		leader.mustPut(t, "", k, "v-"+k)
		want[k] = "v-" + k
	}
	awaitModel(t, "", n1, 10, "ld%02d", want)
	awaitModel(t, "", n2, 10, "ld%02d", want)

	leader.kill()
	putTimeout := time.Duration(c.opts.putS) * time.Second
	delTimeout := time.Duration(c.opts.deleteS) * time.Second
	r, err := n1.put("ld-during", "x")
	expectFailFast(t, "PUT via follower n1 with leader down", r, err, putTimeout, http.StatusServiceUnavailable)
	r, err = n2.del("ld00")
	expectFailFast(t, "DELETE via follower n2 with leader down", r, err, delTimeout, http.StatusServiceUnavailable)
	checkModel(t, "", n1, 10, "ld%02d", want)
	checkModel(t, "", n2, 10, "ld%02d", want)

	c.startNode(leader)
	c.settle("")
	n1.mustPut(t, "", "ld-after", "back")
	if err := leader.hasValue("ld-after", "back"); err != nil {
		t.Errorf("after leader restart: %v", err)
	}
}

// A leader that accepts connections but never answers (SIGSTOP) must not hang
// proxied requests: they end at the configured put/delete timeout with 504.
func TestI6_TimeoutsRespected(t *testing.T) {
	requireBoot(t)
	o := defaultOpts()
	o.putS, o.deleteS = 2, 2
	c := newCluster(t, topoSingle(), o)
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")
	leader.mustPut(t, "", "to-key", "v")
	if err := eventually(t, replTimeout, func() error { return n2.hasValue("to-key", "v") }); err != nil {
		t.Fatal(err)
	}

	leader.pause()
	t.Cleanup(leader.resume)
	r, err := n1.put("to-key", "w")
	expectFailFast(t, "PUT via follower n1 with leader frozen", r, err, 2*time.Second, http.StatusGatewayTimeout)
	r, err = n2.del("to-key")
	expectFailFast(t, "DELETE via follower n2 with leader frozen", r, err, 2*time.Second, http.StatusGatewayTimeout)
	// Reads are local on followers and must not be affected.
	if err := n2.hasValue("to-key", "v"); err != nil {
		t.Errorf("follower read while leader frozen: %v", err)
	}

	leader.resume()
	if err := eventually(t, settleTimeout, func() error {
		r, err := n1.put("to-after", "ok")
		if err != nil || r.status != http.StatusOK {
			return fmt.Errorf("PUT via n1 after leader resumed: %v err=%v", r, err)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// Invalid configs make the binary exit non-zero with a message naming the problem.
//
// Deliberately not gated on requireBoot: this tests startup *failure*, so it must
// run even while B-1 keeps valid configs from booting. Under B-1 every config
// loads as all-zero, so the level0_max_size_mb check fires first for every case:
// "level0_max_size_mb=0" passes vacuously, and the multiplier cases fail tagged
// B-1 because the validation they target is never reached.
func TestI6_BadConfigExitsNonZero(t *testing.T) {
	const l0Msg = "level0_max_size_mb must be greater than 0"
	cases := []struct {
		name    string
		mutate  func(t *testing.T, cfg string) string // nil = remove config.yaml
		wantLog string
	}{
		{"level0_max_size_mb=0", func(t *testing.T, s string) string {
			return replaceLine(t, s, "level0_max_size_mb:", "  level0_max_size_mb: 0")
		}, l0Msg},
		{"level_size_multiplier=1", func(t *testing.T, s string) string {
			return replaceLine(t, s, "level_size_multiplier:", "  level_size_multiplier: 1")
		}, "level_size_multiplier must be greater than 1"},
		{"level_size_multiplier=0", func(t *testing.T, s string) string {
			return replaceLine(t, s, "level_size_multiplier:", "  level_size_multiplier: 0")
		}, "level_size_multiplier must be greater than 1"},
		{"malformed yaml", func(t *testing.T, s string) string { return s + "\n  : : [unbalanced\n" }, "Failed to parse config.yaml"},
		{"missing config.yaml", nil, "Failed to read config.yaml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, topoOneNode(), defaultOpts())
			n := c.nodes[0]
			if tc.mutate == nil {
				if err := os.Remove(n.configPath()); err != nil {
					t.Fatal(err)
				}
			} else {
				b, err := os.ReadFile(n.configPath())
				if err != nil {
					t.Fatal(err)
				}
				if err := n.writeConfig(tc.mutate(t, string(b))); err != nil {
					t.Fatal(err)
				}
			}
			if err := n.start(); err != nil {
				t.Fatal(err)
			}
			if !n.waitExit(readyTimeout) {
				t.Fatalf("binary with %s is still running after %v, want it to exit non-zero", tc.name, readyTimeout)
			}
			if err := n.exitErr(); err == nil || strings.Contains(err.Error(), "status 0") {
				t.Errorf("binary with %s: exit = %v, want non-zero", tc.name, err)
			}
			log := n.logs()
			if !strings.Contains(log, tc.wantLog) {
				prefix := ""
				if tc.wantLog != l0Msg && strings.Contains(log, l0Msg) {
					prefix = tag(bugB1) // the config never loaded, so the earlier l0 check fired instead
				}
				t.Errorf("%sbinary with %s: log does not mention %q:\n%s", prefix, tc.name, tc.wantLog, log)
			}
		})
	}
}

func replaceLine(t *testing.T, s, prefix, repl string) string {
	t.Helper()
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), prefix) {
			lines[i] = repl
			return strings.Join(lines, "\n")
		}
	}
	t.Fatalf("harness: no line %q in config", prefix)
	return ""
}
