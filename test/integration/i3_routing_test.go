//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"
)

// I3: routing on T-multi (the repo's 3-shard layout, every node in every shard).

// Keys on and around every shard boundary ("I" and "Q", end exclusive).
var routingKeys = []string{"0", "A", "H", "Hello", "Hz", "I", "I0", "Java", "P", "Python", "Pzzz", "Q", "Q0", "Zebra", "a", "zzz"}

// Every key, written through every node, is readable (eventually) through every
// node, and deletes through any node remove it everywhere.
func TestI3_EveryKeyThroughEveryNode(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoMulti(), defaultOpts())
	c.startAll()
	c.settle(bugB6)

	for _, w := range c.nodes {
		for _, base := range routingKeys {
			key := base + "-via-" + w.id // stays in base's shard
			val := fmt.Sprintf("%s written via %s", key, w.id)
			r, err := w.put(key, val)
			if err != nil || r.status != http.StatusOK {
				t.Errorf("%s: PUT %q (shard %s, leader %s) via %s: got %v err=%v, want 200",
					bugB6, key, c.topo.shardFor(key).id, c.leaderOf(key).id, w.id, r, err)
				continue
			}
			for _, rd := range c.nodes {
				if err := eventually(t, replTimeout, func() error { return rd.hasValue(key, val) }); err != nil {
					t.Errorf("%s: key %q (shard %s) written via %s, read via %s: %v",
						bugB6, key, c.topo.shardFor(key).id, w.id, rd.id, err)
				}
			}
		}
	}
	if t.Failed() {
		return
	}

	// Delete each key through a node other than the writer; it must vanish everywhere.
	for wi, w := range c.nodes {
		d := c.nodes[(wi+1)%len(c.nodes)]
		for _, base := range routingKeys {
			key := base + "-via-" + w.id
			r, err := d.del(key)
			if err != nil || r.status != http.StatusOK {
				t.Errorf("%s: DELETE %q via %s: got %v err=%v, want 200", bugB6, key, d.id, r, err)
				continue
			}
			for _, rd := range c.nodes {
				if err := eventually(t, replTimeout, func() error { return rd.isAbsent(key) }); err != nil {
					t.Errorf("%s: key %q deleted via %s, read via %s: %v", bugB6, key, d.id, rd.id, err)
				}
			}
		}
	}
}

// A write acknowledged by any node must already be applied on the owning shard's
// configured leader (writes go to the leader synchronously), so a read on that
// leader immediately afterwards sees it. This pins each shard to its configured
// leader, including kvstore-2 for shard-3.
func TestI3_WritesLandOnConfiguredLeader(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoMulti(), defaultOpts())
	c.startAll()
	c.settle(bugB6)

	for _, w := range c.nodes {
		for _, base := range routingKeys {
			key := base + "-leader-via-" + w.id
			val := "v-" + key
			leader := c.leaderOf(key)
			r, err := w.put(key, val)
			if err != nil || r.status != http.StatusOK {
				t.Errorf("%s: PUT %q via %s: got %v err=%v, want 200", bugB6, key, w.id, r, err)
				continue
			}
			if err := leader.hasValue(key, val); err != nil {
				t.Errorf("%s: PUT %q via %s acknowledged, but configured leader %s of %s does not have it yet: %v",
					bugB6, key, w.id, leader.id, c.topo.shardFor(key).id, err)
			}
		}
	}
}
