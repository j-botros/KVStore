//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// I4: replication on T-single (one shard, leader n0, followers n1 and n2).

func TestI4_LeaderWritesPropagate(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader := c.node("n0")

	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("r%02d", i)
		leader.mustPut(t, "", k, "v1-"+k)
		want[k] = "v1-" + k
	}
	for i := 0; i < 50; i += 5 {
		k := fmt.Sprintf("r%02d", i)
		leader.mustPut(t, "", k, "v2-"+k)
		want[k] = "v2-" + k
	}
	for i := 1; i < 50; i += 5 {
		k := fmt.Sprintf("r%02d", i)
		leader.mustDel(t, "", k)
		delete(want, k)
	}
	checkModel(t, "", leader, 50, "r%02d", want)
	for _, f := range []*node{c.node("n1"), c.node("n2")} {
		awaitModel(t, "", f, 50, "r%02d", want)
	}
}

// Writes sent to a follower are proxied to the leader: the leader has them as
// soon as the follower acknowledges, and the other follower eventually does.
func TestI4_FollowerProxiesWrites(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")

	n1.mustPut(t, "", "proxied", "from-n1")
	if err := leader.hasValue("proxied", "from-n1"); err != nil {
		t.Errorf("PUT via follower n1 acknowledged but leader lacks it: %v", err)
	}
	if err := eventually(t, replTimeout, func() error { return n2.hasValue("proxied", "from-n1") }); err != nil {
		t.Errorf("PUT via n1 never reached follower n2: %v", err)
	}
	if err := eventually(t, replTimeout, func() error { return n1.hasValue("proxied", "from-n1") }); err != nil {
		t.Errorf("PUT via n1 never replicated back to n1: %v", err)
	}

	n2.mustDel(t, "", "proxied")
	if err := leader.isAbsent("proxied"); err != nil {
		t.Errorf("DELETE via follower n2 acknowledged but leader still has it: %v", err)
	}
	for _, f := range []*node{n1, n2} {
		if err := eventually(t, replTimeout, func() error { return f.isAbsent("proxied") }); err != nil {
			t.Errorf("DELETE via n2 never reached %s: %v", f.id, err)
		}
	}
}

// With one follower down, leader writes still succeed promptly and reach the other
// follower; the downed follower catches up when it restarts.
func TestI4_FollowerDown(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")

	n2.kill()
	want := map[string]string{}
	for i := 0; i < 30; i++ {
		k := fmt.Sprintf("fd%02d", i)
		r, err := leader.put(k, "v-"+k)
		if err != nil || r.status != http.StatusOK {
			t.Fatalf("PUT %q via leader with follower n2 down: got %v err=%v, want 200", k, r, err)
		}
		if r.dur > time.Duration(c.opts.putS)*time.Second {
			t.Errorf("PUT %q via leader with follower n2 down took %v, want < put timeout %ds (replication must not block)", k, r.dur, c.opts.putS)
		}
		want[k] = "v-" + k
	}
	// Writes through the surviving follower still work too.
	n1.mustPut(t, "", "fd-via-n1", "ok")
	want["fd-via-n1"] = "ok"

	for k, v := range want {
		if err := eventually(t, replTimeout, func() error { return n1.hasValue(k, v) }); err != nil {
			t.Errorf("follower n1 while n2 is down: %v", err)
		}
	}

	// n2 restarts and recovers from the leader before it serves.
	c.startNode(n2)
	for k, v := range want {
		if err := n2.hasValue(k, v); err != nil {
			t.Errorf("follower n2 right after restart (recovery runs before serving): %v", err)
		}
	}
	// And it receives new replication once the leader reconnects.
	c.settle("")
}

// B-3: rapid overwrites of the same keys must leave every follower with the last
// value the leader acknowledged.
func TestI4_RapidOverwrites(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader := c.node("n0")

	const keys, rounds, writers = 20, 40, 4
	// Each writer owns keys i%writers==w and overwrites them in order, so the last
	// acknowledged value per key is known exactly.
	final := make([]string, keys)
	var wg sync.WaitGroup
	errs := make(chan string, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				for i := w; i < keys; i += writers {
					k := fmt.Sprintf("ow%02d", i)
					v := fmt.Sprintf("round-%03d", r)
					res, err := leader.put(k, v)
					if err != nil || res.status != http.StatusOK {
						errs <- fmt.Sprintf("PUT %q=%q via leader: got %v err=%v, want 200", k, v, res, err)
						return
					}
					final[i] = v
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	want := map[string]string{}
	for i, v := range final {
		want[fmt.Sprintf("ow%02d", i)] = v
	}
	checkModel(t, "", leader, keys, "ow%02d", want)
	for _, f := range []*node{c.node("n1"), c.node("n2")} {
		awaitModel(t, bugB3, f, keys, "ow%02d", want)
	}
}

// awaitModel polls until n matches want for keys fmt(0..count-1), then reports
// the remaining mismatches (at most 5) if it never converges.
func awaitModel(t *testing.T, prefix string, n *node, count int, format string, want map[string]string) {
	t.Helper()
	var bad []string
	err := eventually(t, replTimeout, func() error {
		bad = bad[:0]
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
		if len(bad) > 0 {
			return fmt.Errorf("%d keys differ", len(bad))
		}
		return nil
	})
	if err != nil {
		nBad := len(bad)
		if nBad > 5 {
			bad = append(bad[:5], fmt.Sprintf("... and %d more", nBad-5))
		}
		t.Errorf("%s%d/%d keys on %s never converged within %v (got vs want):\n  %s",
			tag(prefix), nBad, count, n.id, replTimeout, strings.Join(bad, "\n  "))
	}
}

// B-7 / B-I14: deleting a key that was never written must be 404 whether the
// DELETE hits the leader directly or is proxied from a follower over gRPC.
// GRPCServer.ForwardDelete turns ErrNotFound into Success:true, so the proxied
// path stays 200 even once the local path (B-7) is fixed.
func TestI4_DeleteMissingKeyViaFollower(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n1 := c.node("n0"), c.node("n1")

	direct, err := leader.del("ghost-direct")
	if err != nil {
		t.Fatal(err)
	}
	if direct.status != http.StatusNotFound {
		t.Errorf("%s: DELETE of never-written key on leader: got %v, want 404", bugB7, direct)
	}
	proxied, err := n1.del("ghost-proxied")
	if err != nil {
		t.Fatal(err)
	}
	if proxied.status != http.StatusNotFound {
		note := ""
		if direct.status != http.StatusNotFound {
			note = " (B-7 also makes the leader's own answer 200; B-I14 remains once B-7 is fixed)"
		}
		t.Errorf("%s: DELETE of never-written key via follower n1: got %v, want 404%s", bugBI14, proxied, note)
	}
}
