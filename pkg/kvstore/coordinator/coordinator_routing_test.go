package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storageengine "kvstore/pkg/kvstore/storageEngine"
)

/* ====================================================================================
	HELPERS
==================================================================================== */

// chdirTemp switches the process cwd to a fresh temp dir (the engine uses
// relative paths) and restores it on cleanup. Not safe with t.Parallel().
func chdirTemp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(orig) })
	return dir
}

// openEngineHere opens a storage engine in the current working directory.
func openEngineHere(t *testing.T) *storageengine.StorageEngine {
	t.Helper()
	engine, err := storageengine.OpenStorageEngine(64*1024*1024, 64*1024*1024, 4096, 10)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	return engine
}

type coordOpts struct {
	minKey, maxKey string
	isLeader       bool
	leaderClient   NodeClient
	followers      []NodeClient
	remote         []ShardNodeConfig
}

// newTestCoord builds a coordinator over a fresh engine in a fresh temp dir.
func newTestCoord(t *testing.T, o coordOpts) (*Coordinator, *storageengine.StorageEngine) {
	t.Helper()
	chdirTemp(t)
	engine := openEngineHere(t)
	return NewCoordinator(engine, NewShardRouter(o.remote), o.leaderClient, o.minKey, o.maxKey, o.isLeader, o.followers), engine
}

// recordingClient is a mockNodeClient that records which methods were called.
type recordingClient struct {
	mockNodeClient
	name  string
	mu    sync.Mutex
	calls []string
}

func (r *recordingClient) record(s string) {
	r.mu.Lock()
	r.calls = append(r.calls, s)
	r.mu.Unlock()
}

func (r *recordingClient) Calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recordingClient) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	r.record("get:" + key)
	return r.mockNodeClient.ForwardGet(ctx, key)
}

func (r *recordingClient) ForwardPut(ctx context.Context, key string, value []byte) error {
	r.record("put:" + key + "=" + string(value))
	return r.mockNodeClient.ForwardPut(ctx, key, value)
}

func (r *recordingClient) ForwardDelete(ctx context.Context, key string) error {
	r.record("delete:" + key)
	return r.mockNodeClient.ForwardDelete(ctx, key)
}

// coordClient is an in-process NodeClient backed by a real *Coordinator.
// It mirrors what interface/grpc does over the wire, including Replay chunking.
type coordClient struct{ c *Coordinator }

func (cc coordClient) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := cc.c.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}
func (cc coordClient) ForwardPut(ctx context.Context, key string, value []byte) error {
	return cc.c.Put(ctx, key, value)
}
func (cc coordClient) ForwardDelete(ctx context.Context, key string) error {
	return cc.c.Delete(ctx, key)
}
func (cc coordClient) ReplicateEntry(ctx context.Context, e LogEntry) error {
	return cc.c.ReplicateEntry(e)
}
func (cc coordClient) GetSeqNum(ctx context.Context) (uint64, error) { return cc.c.GetSeqNum(), nil }
func (cc coordClient) ReplaySnapshot(ctx context.Context, _ uint64, onChunk func(uint64, []LogEntry, bool) error) error {
	var batch []LogEntry
	err := cc.c.Scan(func(k string, v []byte) error {
		batch = append(batch, LogEntry{Key: k, Value: slices.Clone(v)})
		if len(batch) >= 500 {
			if err := onChunk(0, batch, false); err != nil {
				return err
			}
			batch = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	return onChunk(cc.c.GetSeqNum(), batch, true)
}

func mustGet(t *testing.T, c *Coordinator, key string) string {
	t.Helper()
	v, err := c.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return string(v)
}

/* ====================================================================================
	U1: ownsKey bounds
==================================================================================== */

func TestOwnsKey_Bounds(t *testing.T) {
	tests := []struct {
		min, max string
		key      string
		want     bool
	}{
		// ["", "I")
		{"", "I", "", true},
		{"", "I", "A", true},
		{"", "I", "H", true},
		{"", "I", "Hzzz", true},
		{"", "I", "I", false},
		{"", "I", "I0", false},
		// ["I", "Q")
		{"I", "Q", "Hzzz", false},
		{"I", "Q", "I", true},
		{"I", "Q", "I0", true},
		{"I", "Q", "Pzzz", true},
		{"I", "Q", "Q", false},
		{"I", "Q", "Q0", false},
		// ["Q", "")
		{"Q", "", "Pzzz", false},
		{"Q", "", "Q", true},
		{"Q", "", "z", true},
		{"Q", "", "\xff\xff", true},
		// ["", "") owns everything
		{"", "", "", true},
		{"", "", "anything", true},
	}
	for _, tc := range tests {
		c := &Coordinator{minKey: tc.min, maxKey: tc.max}
		if got := c.ownsKey(tc.key); got != tc.want {
			t.Errorf("[%q,%q).ownsKey(%q) = %v, want %v", tc.min, tc.max, tc.key, got, tc.want)
		}
	}
}

/* ====================================================================================
	U1: local paths
==================================================================================== */

func TestCoordinator_Local_PutGetDelete(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true})
	ctx := context.Background()

	if _, err := c.Get(ctx, "A"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
	if err := c.Put(ctx, "A", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGet(t, c, "A"); got != "1" {
		t.Fatalf("Get = %q, want 1", got)
	}
	if err := c.Delete(ctx, "A"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(ctx, "A"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(after delete) err = %v, want ErrNotFound", err)
	}
}

func TestCoordinator_Follower_ServesLocalReads(t *testing.T) {
	leader := &recordingClient{}
	c, engine := newTestCoord(t, coordOpts{minKey: "I", maxKey: "Q", isLeader: false, leaderClient: leader})
	if err := engine.Put("K", []byte("local")); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, c, "K"); got != "local" {
		t.Fatalf("Get = %q, want local", got)
	}
	if calls := leader.Calls(); len(calls) != 0 {
		t.Fatalf("follower read contacted leader: %v", calls)
	}
}

func TestCoordinator_Follower_ProxiesWritesToLeader(t *testing.T) {
	leader := &recordingClient{}
	c, engine := newTestCoord(t, coordOpts{minKey: "I", maxKey: "Q", isLeader: false, leaderClient: leader})
	ctx := context.Background()

	if err := c.Put(ctx, "K", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(ctx, "K"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	want := []string{"put:K=v", "delete:K"}
	if got := leader.Calls(); !slices.Equal(got, want) {
		t.Fatalf("leader calls = %v, want %v", got, want)
	}
	if _, err := engine.Get("K"); !errors.Is(err, storageengine.ErrKeyNotFound) {
		t.Fatalf("follower wrote locally: engine.Get err = %v, want ErrKeyNotFound", err)
	}
}

func TestCoordinator_Follower_ProxyErrorPropagates(t *testing.T) {
	sentinel := errors.New("leader down")
	leader := &mockNodeClient{
		forwardPutFunc:    func(context.Context, string, []byte) error { return sentinel },
		forwardDeleteFunc: func(context.Context, string) error { return sentinel },
	}
	c, _ := newTestCoord(t, coordOpts{minKey: "I", maxKey: "Q", isLeader: false, leaderClient: leader})
	if err := c.Put(context.Background(), "K", []byte("v")); !errors.Is(err, sentinel) {
		t.Errorf("Put err = %v, want %v", err, sentinel)
	}
	if err := c.Delete(context.Background(), "K"); !errors.Is(err, sentinel) {
		t.Errorf("Delete err = %v, want %v", err, sentinel)
	}
}

func TestCoordinator_Follower_NoLeader_ReadOnly(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{minKey: "I", maxKey: "Q", isLeader: false})
	if err := c.Put(context.Background(), "K", []byte("v")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Put err = %v, want ErrReadOnly", err)
	}
	if err := c.Delete(context.Background(), "K"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Delete err = %v, want ErrReadOnly", err)
	}
}

func TestCoordinator_Leader_DeleteMissingKey_NotFound(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{isLeader: true})
	err := c.Delete(context.Background(), "never-written")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("BUG B-7 (deleting a missing key succeeds): got err %v, want ErrNotFound", err)
	}
}

/* ====================================================================================
	U1: remote paths (ShardRouter)
==================================================================================== */

func TestCoordinator_Remote_Get(t *testing.T) {
	sentinel := errors.New("remote failure")
	tests := []struct {
		name    string
		fn      func(context.Context, string) ([]byte, bool, error)
		want    string
		wantErr error
	}{
		{"found", func(context.Context, string) ([]byte, bool, error) { return []byte("rv"), true, nil }, "rv", nil},
		{"not found", func(context.Context, string) ([]byte, bool, error) { return nil, false, nil }, "", ErrNotFound},
		{"error", func(context.Context, string) ([]byte, bool, error) { return nil, false, sentinel }, "", sentinel},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			remote := &recordingClient{mockNodeClient: mockNodeClient{forwardGetFunc: tc.fn}}
			c, _ := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true,
				remote: []ShardNodeConfig{{MinKey: "I", MaxKey: "", Leader: remote}}})
			v, err := c.Get(context.Background(), "K")
			if !errors.Is(err, tc.wantErr) || string(v) != tc.want {
				t.Fatalf("Get = (%q, %v), want (%q, %v)", v, err, tc.want, tc.wantErr)
			}
			if got := remote.Calls(); !slices.Equal(got, []string{"get:K"}) {
				t.Fatalf("remote calls = %v, want [get:K]", got)
			}
		})
	}
}

func TestCoordinator_Remote_WritesGoToShardLeader(t *testing.T) {
	leader := &recordingClient{}
	follower := &recordingClient{}
	c, engine := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true,
		remote: []ShardNodeConfig{{MinKey: "I", MaxKey: "", Leader: leader, Followers: []NodeClient{follower}}}})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := c.Put(ctx, "K", []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := c.Delete(ctx, "K"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	want := []string{"put:K=v", "put:K=v", "put:K=v", "delete:K"}
	if got := leader.Calls(); !slices.Equal(got, want) {
		t.Errorf("leader calls = %v, want %v", got, want)
	}
	if got := follower.Calls(); len(got) != 0 {
		t.Errorf("remote follower received writes: %v", got)
	}
	if _, err := engine.Get("K"); !errors.Is(err, storageengine.ErrKeyNotFound) {
		t.Errorf("remote key written locally: err = %v", err)
	}
	if c.GetSeqNum() != 0 {
		t.Errorf("forwarded writes advanced local seq to %d, want 0", c.GetSeqNum())
	}
}

func TestCoordinator_Remote_ErrorsPropagate(t *testing.T) {
	sentinel := errors.New("remote failure")
	leader := &mockNodeClient{
		forwardPutFunc:    func(context.Context, string, []byte) error { return sentinel },
		forwardDeleteFunc: func(context.Context, string) error { return sentinel },
	}
	c, _ := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true,
		remote: []ShardNodeConfig{{MinKey: "I", MaxKey: "", Leader: leader}}})
	if err := c.Put(context.Background(), "K", nil); !errors.Is(err, sentinel) {
		t.Errorf("Put err = %v, want %v", err, sentinel)
	}
	if err := c.Delete(context.Background(), "K"); !errors.Is(err, sentinel) {
		t.Errorf("Delete err = %v, want %v", err, sentinel)
	}
}

func TestCoordinator_Remote_NoShardForKey(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true}) // empty router
	ctx := context.Background()
	if _, err := c.Get(ctx, "K"); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("Get err = %v, want a routing error (not ErrNotFound)", err)
	}
	if err := c.Put(ctx, "K", nil); err == nil {
		t.Errorf("Put err = nil, want routing error")
	}
	if err := c.Delete(ctx, "K"); err == nil {
		t.Errorf("Delete err = nil, want routing error")
	}
}

func TestCoordinator_Remote_ContextPassedThrough(t *testing.T) {
	remote := &mockNodeClient{forwardGetFunc: func(ctx context.Context, _ string) ([]byte, bool, error) {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}}
	c, _ := newTestCoord(t, coordOpts{minKey: "", maxKey: "I", isLeader: true,
		remote: []ShardNodeConfig{{MinKey: "I", MaxKey: "", Leader: remote}}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, "K"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get err = %v, want context.DeadlineExceeded", err)
	}
}

/* ====================================================================================
	U1: replication fan-out
==================================================================================== */

// chanFollower forwards every replicated entry to a channel.
func chanFollower(ch chan<- LogEntry, err error) *mockNodeClient {
	return &mockNodeClient{replicateEntryFunc: func(_ context.Context, e LogEntry) error {
		ch <- e
		return err
	}}
}

func recvEntry(t *testing.T, ch <-chan LogEntry) LogEntry {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for replicated entry")
		return LogEntry{}
	}
}

func TestCoordinator_Leader_ReplicatesToAllFollowers(t *testing.T) {
	ch1, ch2 := make(chan LogEntry, 4), make(chan LogEntry, 4)
	c, engine := newTestCoord(t, coordOpts{isLeader: true,
		followers: []NodeClient{chanFollower(ch1, nil), chanFollower(ch2, nil)}})
	ctx := context.Background()

	if err := c.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i, ch := range []chan LogEntry{ch1, ch2} {
		e := recvEntry(t, ch)
		want := LogEntry{SeqNum: 1, Operation: "PUT", Key: "k", Value: []byte("v")}
		if e.SeqNum != want.SeqNum || e.Operation != want.Operation || e.Key != want.Key || string(e.Value) != "v" {
			t.Errorf("follower %d got %+v, want %+v", i, e, want)
		}
	}

	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	for i, ch := range []chan LogEntry{ch1, ch2} {
		e := recvEntry(t, ch)
		if e.SeqNum != 2 || e.Operation != "DELETE" || e.Key != "k" {
			t.Errorf("follower %d got %+v, want {SeqNum:2 Operation:DELETE Key:k}", i, e)
		}
	}
	if _, err := engine.Get("k"); !errors.Is(err, storageengine.ErrKeyNotFound) {
		t.Errorf("leader engine still has k: %v", err)
	}
	if c.GetSeqNum() != 2 {
		t.Errorf("leader seq = %d, want 2", c.GetSeqNum())
	}
}

func TestCoordinator_Leader_FollowerFailureDoesNotFailWrite(t *testing.T) {
	ch := make(chan LogEntry, 1)
	c, _ := newTestCoord(t, coordOpts{isLeader: true,
		followers: []NodeClient{chanFollower(ch, errors.New("follower down"))}})
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put err = %v, want nil (replication is fire-and-forget)", err)
	}
	recvEntry(t, ch)
	if got := mustGet(t, c, "k"); got != "v" {
		t.Fatalf("Get = %q, want v", got)
	}
}

// Deterministic B-3 reproduction: the follower holds seq 1 until seq 2 shows up
// (up to 1s). A replicator that preserves per-follower order never sends seq 2
// while seq 1 is in flight, so this only fails when entries race.
func TestReplication_SameKeyOverwrites_AppliedInOrder(t *testing.T) {
	var mu sync.Mutex
	var order []uint64
	var final string
	second := make(chan struct{})
	done := make(chan struct{}, 2)

	apply := func(e LogEntry) {
		mu.Lock()
		order = append(order, e.SeqNum)
		final = string(e.Value)
		mu.Unlock()
	}
	follower := &mockNodeClient{replicateEntryFunc: func(_ context.Context, e LogEntry) error {
		defer func() { done <- struct{}{} }()
		if e.SeqNum == 1 {
			select {
			case <-second:
			case <-time.After(time.Second):
			}
			apply(e)
			return nil
		}
		apply(e)
		close(second)
		return nil
	}}

	c, _ := newTestCoord(t, coordOpts{isLeader: true, followers: []NodeClient{follower}})
	ctx := context.Background()
	if err := c.Put(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for replication")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if final != "v2" {
		t.Fatalf("BUG B-3 (async replication can apply two writes to one key out of order): "+
			"follower applied seq order %v, final value %q; want order [1 2], final value \"v2\"", order, final)
	}
}

// Stress B-3 reproduction with no artificial delay: several clients overwrite one
// key concurrently on the leader, which replicates to a real follower
// coordinator. Once replication drains, the replicas must converge.
func TestReplication_ConcurrentOverwrites_ReplicasConverge_Stress(t *testing.T) {
	const writers, perWriter = 8, 25
	const n = writers * perWriter
	// Leader and follower share one temp dir; see TestLeaderRecovery_FromRealFollower.
	chdirTemp(t)
	follower := NewCoordinator(openEngineHere(t), NewShardRouter(nil), nil, "", "", false, nil)

	var applied atomic.Int64
	allDone := make(chan struct{})
	tap := &mockNodeClient{replicateEntryFunc: func(_ context.Context, e LogEntry) error {
		err := follower.ReplicateEntry(e)
		if applied.Add(1) == n {
			close(allDone)
		}
		return err
	}}
	leader := NewCoordinator(openEngineHere(t), NewShardRouter(nil), nil, "", "", true, []NodeClient{tap})

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := leader.Put(context.Background(), "k", []byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case <-allDone:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out: %d/%d entries replicated", applied.Load(), n)
	}

	want := mustGet(t, leader, "k")
	got := mustGet(t, follower, "k")
	if got != want {
		t.Fatalf("BUG B-3 (async replication can apply two writes to one key out of order): "+
			"after %d concurrent overwrites, follower value %q, want leader value %q", n, got, want)
	}
}

/* ====================================================================================
	U2: ReplicateEntry, seq nums
==================================================================================== */

func TestReplicateEntry_Operations(t *testing.T) {
	// A follower with no leader client: client writes are read-only, but
	// replication must bypass that guard.
	c, engine := newTestCoord(t, coordOpts{isLeader: false})

	if err := c.ReplicateEntry(LogEntry{SeqNum: 1, Operation: "PUT", Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if v, err := engine.Get("k"); err != nil || string(v) != "v" {
		t.Fatalf("after PUT: Get = (%q, %v), want v", v, err)
	}
	if err := c.ReplicateEntry(LogEntry{SeqNum: 2, Operation: "DELETE", Key: "k"}); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if _, err := engine.Get("k"); !errors.Is(err, storageengine.ErrKeyNotFound) {
		t.Fatalf("after DELETE: err = %v, want ErrKeyNotFound", err)
	}
	if err := c.ReplicateEntry(LogEntry{SeqNum: 3, Operation: "DELETE", Key: "absent"}); err != nil {
		t.Fatalf("DELETE of absent key: err = %v, want nil (idempotent)", err)
	}
	if err := c.ReplicateEntry(LogEntry{SeqNum: 4, Operation: "BOGUS", Key: "k"}); err == nil {
		t.Fatal("unknown operation: err = nil, want error")
	}
}

func TestSeqNum_LeaderIncrementsPerLocalWrite(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{isLeader: true})
	ctx := context.Background()
	for i, k := range []string{"a", "b", "a"} {
		if err := c.Put(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
		if got := c.GetSeqNum(); got != uint64(i+1) {
			t.Fatalf("after write %d: seq = %d, want %d", i+1, got, i+1)
		}
	}
	if err := c.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if got := c.GetSeqNum(); got != 4 {
		t.Fatalf("after delete: seq = %d, want 4", got)
	}
}

func TestSeqNum_FollowerAdvancesOnReplicate(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{isLeader: false})
	for seq := uint64(1); seq <= 5; seq++ {
		if err := c.ReplicateEntry(LogEntry{SeqNum: seq, Operation: "PUT", Key: "k", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.GetSeqNum(); got != 5 {
		t.Fatalf("BUG B-4 (follower seq num never advances on ReplicateEntry): got seq %d, want 5", got)
	}
}

func TestSeqNum_PersistsAcrossRestart(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{isLeader: true})
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		if err := c.Put(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Restart: reopen the engine from the same directory.
	engine2 := openEngineHere(t)
	c2 := NewCoordinator(engine2, NewShardRouter(nil), nil, "", "", true, nil)
	if got := mustGet(t, c2, "b"); got != "v" {
		t.Fatalf("data not recovered: Get(b) = %q", got)
	}
	if got := c2.GetSeqNum(); got != 3 {
		t.Fatalf("BUG B-4 (seq num is not persisted across restart): got seq %d after restart, want 3", got)
	}
}

/* ====================================================================================
	U2: ApplySnapshot
==================================================================================== */

func TestApplySnapshot_MultiChunk(t *testing.T) {
	c, engine := newTestCoord(t, coordOpts{isLeader: false})

	if err := c.ApplySnapshot(0, []LogEntry{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}}, false); err != nil {
		t.Fatal(err)
	}
	if got := c.GetSeqNum(); got != 0 {
		t.Fatalf("seq after non-final chunk = %d, want 0", got)
	}
	if err := c.ApplySnapshot(77, []LogEntry{{Key: "c", Value: []byte("3")}}, true); err != nil {
		t.Fatal(err)
	}
	if got := c.GetSeqNum(); got != 77 {
		t.Fatalf("seq after final chunk = %d, want 77", got)
	}
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if v, err := engine.Get(k); err != nil || string(v) != want {
			t.Errorf("Get(%s) = (%q, %v), want %q", k, v, err, want)
		}
	}
}

func TestApplySnapshot_EmptyFinalChunkSetsSeq(t *testing.T) {
	c, _ := newTestCoord(t, coordOpts{isLeader: false})
	if err := c.ApplySnapshot(9, nil, true); err != nil {
		t.Fatal(err)
	}
	if got := c.GetSeqNum(); got != 9 {
		t.Fatalf("seq = %d, want 9", got)
	}
}

/* ====================================================================================
	U2: recovery
==================================================================================== */

func TestRecoverIfNeeded_FollowerDonorErrorMidStream(t *testing.T) {
	sentinel := errors.New("stream broke")
	leader := &mockNodeClient{
		getSeqNumFunc: func(context.Context) (uint64, error) { return 10, nil },
		replaySnapshotFunc: func(_ context.Context, _ uint64, onChunk func(uint64, []LogEntry, bool) error) error {
			if err := onChunk(0, []LogEntry{{Key: "a", Value: []byte("1")}}, false); err != nil {
				return err
			}
			return sentinel
		},
	}
	c, _ := newTestCoord(t, coordOpts{isLeader: false, leaderClient: leader})
	if err := c.RecoverIfNeeded(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if got := c.GetSeqNum(); got != 0 {
		t.Fatalf("seq after incomplete snapshot = %d, want 0", got)
	}
}

func TestRecoverIfNeeded_FollowerBehind_DropsKeysDeletedOnDonor(t *testing.T) {
	// Leader state at seq 5: x was deleted, y was updated.
	leader := &mockNodeClient{
		getSeqNumFunc: func(context.Context) (uint64, error) { return 5, nil },
		replaySnapshotFunc: func(_ context.Context, _ uint64, onChunk func(uint64, []LogEntry, bool) error) error {
			return onChunk(5, []LogEntry{{Key: "y", Value: []byte("new")}}, true)
		},
	}
	c, engine := newTestCoord(t, coordOpts{isLeader: false, leaderClient: leader})
	// Follower's stale local state from before it fell behind.
	if err := engine.Put("x", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put("y", []byte("old")); err != nil {
		t.Fatal(err)
	}

	if err := c.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("RecoverIfNeeded: %v", err)
	}
	if got := mustGet(t, c, "y"); got != "new" {
		t.Fatalf("Get(y) = %q, want new", got)
	}
	if v, err := c.Get(context.Background(), "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("BUG B-5 (recovery does not delete keys the donor has deleted): "+
			"got Get(x) = (%q, %v) after recovery, want ErrNotFound", v, err)
	}
}

// Two real coordinators: a leader replicates to a follower; the leader's disk is
// wiped; the restarted (empty) leader must recover from that follower.
func TestLeaderRecovery_FromRealFollower(t *testing.T) {
	// Leader L1 and follower F share one temp dir. Both engines append to
	// data/wal/0.log there, but neither is ever reopened from disk, so the
	// shared file is harmless; all reads come from the memtables.
	chdirTemp(t)
	fEngine := openEngineHere(t)
	follower := NewCoordinator(fEngine, NewShardRouter(nil), nil, "", "", false, nil)

	var replicated atomic.Int64
	allReplicated := make(chan struct{})
	keys := []string{"a", "b", "c"}
	tap := &mockNodeClient{replicateEntryFunc: func(ctx context.Context, e LogEntry) error {
		err := follower.ReplicateEntry(e)
		if replicated.Add(1) == int64(len(keys)) {
			close(allReplicated)
		}
		return err
	}}
	l1 := NewCoordinator(openEngineHere(t), NewShardRouter(nil), nil, "", "", true, []NodeClient{tap})
	for _, k := range keys {
		if err := l1.Put(context.Background(), k, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-allReplicated:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for replication")
	}

	// Leader disk wiped: restart in a fresh, empty directory.
	chdirTemp(t)
	l2 := NewCoordinator(openEngineHere(t), NewShardRouter(nil), nil, "", "", true, []NodeClient{coordClient{follower}})
	if err := l2.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("RecoverIfNeeded: %v", err)
	}
	for _, k := range keys {
		v, err := l2.Get(context.Background(), k)
		if err != nil || string(v) != "v-"+k {
			t.Fatalf("BUG B-4 (follower seq num stays 0, so an empty leader never picks it as donor): "+
				"follower seq = %d; recovered leader Get(%q) = (%q, %v), want %q",
				follower.GetSeqNum(), k, v, err, "v-"+k)
		}
	}
	if got, want := l2.GetSeqNum(), uint64(len(keys)); got != want {
		t.Errorf("recovered leader seq = %d, want %d", got, want)
	}
}

// When the engine fails while applying a chunk, ApplySnapshot must stop and
// return the error, and a failing final chunk must not adopt the donor seq.
// The engine is made to fail by removing its WAL directory (every Put opens
// data/wal/<n>.log with O_CREATE, which fails once the parent is gone).
func TestApplySnapshot_EngineErrorStopsAndReturns(t *testing.T) {
	c, engine := newTestCoord(t, coordOpts{isLeader: false})

	if err := c.ApplySnapshot(0, []LogEntry{{Key: "a", Value: []byte("1")}}, false); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := os.RemoveAll("data/wal"); err != nil {
		t.Fatal(err)
	}

	err := c.ApplySnapshot(42, []LogEntry{{Key: "b", Value: []byte("2")}, {Key: "c", Value: []byte("3")}}, true)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ApplySnapshot with failing engine: err = %v, want an fs.ErrNotExist error from the WAL", err)
	}
	if got := c.GetSeqNum(); got != 0 {
		t.Fatalf("seq after failed final chunk = %d, want 0 (snapshot incomplete)", got)
	}
	for _, k := range []string{"b", "c"} {
		if v, err := engine.Get(k); !errors.Is(err, storageengine.ErrKeyNotFound) {
			t.Errorf("Get(%s) after failed chunk = (%q, %v), want ErrKeyNotFound", k, v, err)
		}
	}
	if v, err := engine.Get("a"); err != nil || string(v) != "1" {
		t.Errorf("Get(a) from the earlier successful chunk = (%q, %v), want 1", v, err)
	}
}
