//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// I5: startup recovery on T-single. RecoverIfNeeded runs before a node serves
// HTTP, so once a restarted node is ready its recovered state is asserted
// directly, without polling.

// writeAndReplicate writes count keys fmt(i) -> "v-"+key via the leader and
// waits until both followers have them.
func writeAndReplicate(t *testing.T, c *cluster, format string, count int, want map[string]string) {
	t.Helper()
	leader := c.node("n0")
	for i := 0; i < count; i++ {
		k := fmt.Sprintf(format, i)
		leader.mustPut(t, "", k, "v-"+k)
		want[k] = "v-" + k
	}
	for _, f := range []*node{c.node("n1"), c.node("n2")} {
		awaitModel(t, "", f, count, format, want)
	}
	if t.Failed() {
		t.FailNow()
	}
}

// A follower whose disk was wiped recovers the full data set from the leader.
// 1201 keys make Replay send three chunks (500 + 500 + 201).
func TestI5_WipedFollowerRecovers(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	n1 := c.node("n1")

	want := map[string]string{}
	writeAndReplicate(t, c, "wf%04d", 1201, want)

	n1.kill()
	c.wipe(n1)
	c.startNode(n1)
	checkModel(t, "", n1, 1201, "wf%04d", want)
}

// B-5: a follower that was down while keys were deleted, overwritten and added
// must match the leader after it restarts: catch up on new and changed keys and
// drop the deleted ones (tombstones survive recovery).
func TestI5_BehindFollowerAcrossDeletes(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n2 := c.node("n0"), c.node("n2")

	want := map[string]string{}
	writeAndReplicate(t, c, "bf%02d", 20, want)

	n2.kill()
	for i := 0; i < 10; i++ { // delete bf00..bf09
		k := fmt.Sprintf("bf%02d", i)
		leader.mustDel(t, "", k)
		delete(want, k)
	}
	for i := 10; i < 15; i++ { // overwrite bf10..bf14
		k := fmt.Sprintf("bf%02d", i)
		leader.mustPut(t, "", k, "new-"+k)
		want[k] = "new-" + k
	}
	for i := 20; i < 25; i++ { // add bf20..bf24
		k := fmt.Sprintf("bf%02d", i)
		leader.mustPut(t, "", k, "v-"+k)
		want[k] = "v-" + k
	}

	c.startNode(n2)
	// Live keys: catch-up via Replay.
	var live, deleted []int
	for i := 10; i < 25; i++ {
		live = append(live, i)
	}
	for i := 0; i < 10; i++ {
		deleted = append(deleted, i)
	}
	for _, i := range live {
		k := fmt.Sprintf("bf%02d", i)
		if err := n2.hasValue(k, want[k]); err != nil {
			t.Errorf("follower n2 after restart did not catch up: %v", err)
		}
	}
	// Deleted keys: must be gone.
	for _, i := range deleted {
		k := fmt.Sprintf("bf%02d", i)
		if err := n2.isAbsent(k); err != nil {
			t.Errorf("%s: key deleted on the leader while n2 was down: %v", bugB5, err)
		}
	}
}

// B-4: followers that applied replicated writes must report the leader's seq num.
func TestI5_FollowerSeqNumAdvances(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader := c.node("n0")

	want := map[string]string{}
	writeAndReplicate(t, c, "sq%02d", 10, want)

	leaderSeq, err := leader.seqNum()
	if err != nil {
		t.Fatal(err)
	}
	if leaderSeq < 10 {
		t.Fatalf("leader seq num after >=10 writes: got %d, want >= 10", leaderSeq)
	}
	for _, f := range []*node{c.node("n1"), c.node("n2")} {
		if err := eventually(t, replTimeout, func() error {
			s, err := f.seqNum()
			if err != nil {
				return err
			}
			if s != leaderSeq {
				return fmt.Errorf("%s seq num: got %d, want leader's %d", f.id, s, leaderSeq)
			}
			return nil
		}); err != nil {
			t.Errorf("%s: %v", bugB4, err)
		}
	}

	// And the seq num must survive a restart (leader keeps its data on disk).
	c.restart(leader)
	s, err := leader.seqNum()
	if err != nil {
		t.Fatal(err)
	}
	if s < leaderSeq {
		t.Errorf("%s: leader seq num after restart with its data intact: got %d, want >= %d", bugB4, s, leaderSeq)
	}
}

// B-4: a leader whose disk was wiped recovers everything from a follower.
func TestI5_WipedLeaderRecovers(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader := c.node("n0")

	want := map[string]string{}
	writeAndReplicate(t, c, "wl%03d", 100, want)

	leader.kill()
	c.wipe(leader)
	c.startNode(leader)
	checkModel(t, bugB4, leader, 100, "wl%03d", want)
}

// B-4: a wiped leader must pick the most up-to-date follower. n2 misses a batch
// of writes; the leader must recover from n1 and have both batches.
func TestI5_WipedLeaderPicksMostUpToDateFollower(t *testing.T) {
	requireBoot(t)
	c := newCluster(t, topoSingle(), defaultOpts())
	c.startAll()
	c.settle("")
	leader, n1, n2 := c.node("n0"), c.node("n1"), c.node("n2")

	want := map[string]string{}
	writeAndReplicate(t, c, "ua%03d", 50, want) // batch A: everyone

	n2.kill()
	for i := 0; i < 50; i++ { // batch B: only n1 gets it
		k := fmt.Sprintf("ub%03d", i)
		leader.mustPut(t, "", k, "v-"+k)
		want[k] = "v-" + k
	}
	wantB := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("ub%03d", i)
		wantB[k] = want[k]
	}
	awaitModel(t, "", n1, 50, "ub%03d", wantB)

	leader.kill()
	c.wipe(leader)
	// n2 comes back while the leader is down, so it cannot catch up and stays behind.
	c.startNode(n2)
	c.startNode(leader)

	wantA := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("ua%03d", i)
		wantA[k] = want[k]
	}
	checkModel(t, bugB4, leader, 50, "ua%03d", wantA)
	checkModel(t, bugB4, leader, 50, "ub%03d", wantB)
}
