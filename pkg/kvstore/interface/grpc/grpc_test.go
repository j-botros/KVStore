package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	coordinator "kvstore/pkg/kvstore/coordinator"
	storageengine "kvstore/pkg/kvstore/storageEngine"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

/* ====================================================================================
	HELPERS
==================================================================================== */

func chdirTemp(t *testing.T) {
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
}

// openEngine opens an engine in the current directory with a memtable large
// enough that nothing is flushed during a test.
func openEngine(t *testing.T) *storageengine.StorageEngine {
	t.Helper()
	e, err := storageengine.OpenStorageEngine(64<<20, 64<<20, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// fakeNode is a coordinator.NodeClient with function-field overrides.
type fakeNode struct {
	get    func(ctx context.Context, key string) ([]byte, bool, error)
	put    func(ctx context.Context, key string, value []byte) error
	delete func(ctx context.Context, key string) error
}

func (f *fakeNode) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	if f.get != nil {
		return f.get(ctx, key)
	}
	return nil, false, nil
}
func (f *fakeNode) ForwardPut(ctx context.Context, key string, value []byte) error {
	if f.put != nil {
		return f.put(ctx, key, value)
	}
	return nil
}
func (f *fakeNode) ForwardDelete(ctx context.Context, key string) error {
	if f.delete != nil {
		return f.delete(ctx, key)
	}
	return nil
}
func (f *fakeNode) ReplicateEntry(context.Context, coordinator.LogEntry) error { return nil }
func (f *fakeNode) GetSeqNum(context.Context) (uint64, error)                  { return 0, nil }
func (f *fakeNode) ReplaySnapshot(context.Context, uint64, func(uint64, []coordinator.LogEntry, bool) error) error {
	return nil
}

// serve starts a GRPCServer for coord over bufconn and returns an adapter
// and the raw generated client connected to it.
func serve(t *testing.T, nodeID string, coord *coordinator.Coordinator) (*NodeClientAdapter, KVStoreClient) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := ggrpc.NewServer()
	RegisterKVStoreServer(s, NewGRPCServer(nodeID, coord))
	go s.Serve(lis)
	t.Cleanup(s.Stop)

	conn, err := ggrpc.NewClient("passthrough:///bufnet",
		ggrpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := NewKVStoreClient(conn)
	return &NodeClientAdapter{client: client}, client
}

type nodeOpts struct {
	minKey, maxKey string
	isLeader       bool
	leaderClient   coordinator.NodeClient
	remote         []coordinator.ShardNodeConfig
}

// newNode builds a coordinator over a fresh engine in a fresh temp dir and
// serves it over bufconn.
func newNode(t *testing.T, o nodeOpts) (*NodeClientAdapter, KVStoreClient, *coordinator.Coordinator, *storageengine.StorageEngine) {
	t.Helper()
	chdirTemp(t)
	engine := openEngine(t)
	coord := coordinator.NewCoordinator(engine, coordinator.NewShardRouter(o.remote), o.leaderClient, o.minKey, o.maxKey, o.isLeader, nil)
	a, c := serve(t, "node-under-test", coord)
	return a, c, coord, engine
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

/* ====================================================================================
	Forwarding RPCs
==================================================================================== */

func TestGRPC_ForwardPutGetDelete(t *testing.T) {
	a, _, _, engine := newNode(t, nodeOpts{isLeader: true})
	ctx := ctxT(t)

	if err := a.ForwardPut(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("ForwardPut: %v", err)
	}
	if v, err := engine.Get("k"); err != nil || string(v) != "v" {
		t.Fatalf("server engine Get(k) = (%q, %v), want v", v, err)
	}
	v, found, err := a.ForwardGet(ctx, "k")
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("ForwardGet(k) = (%q, %v, %v), want (v, true, nil)", v, found, err)
	}
	if err := a.ForwardDelete(ctx, "k"); err != nil {
		t.Fatalf("ForwardDelete: %v", err)
	}
	v, found, err = a.ForwardGet(ctx, "k")
	if err != nil || found {
		t.Fatalf("ForwardGet after delete = (%q, %v, %v), want (nil, false, nil)", v, found, err)
	}
}

func TestGRPC_ForwardPut_EmptyAndBinaryValues(t *testing.T) {
	a, _, _, _ := newNode(t, nodeOpts{isLeader: true})
	ctx := ctxT(t)
	for name, val := range map[string][]byte{
		"empty":  {},
		"binary": {0, 1, 0xff, 0xfe, 0},
		"large":  make([]byte, 256*1024),
	} {
		t.Run(name, func(t *testing.T) {
			if err := a.ForwardPut(ctx, name, val); err != nil {
				t.Fatal(err)
			}
			got, found, err := a.ForwardGet(ctx, name)
			if err != nil || !found || string(got) != string(val) {
				t.Fatalf("ForwardGet = (len %d, %v, %v), want len %d found", len(got), found, err, len(val))
			}
		})
	}
}

func TestGRPC_ForwardGet_ServerErrorPropagates(t *testing.T) {
	boom := errors.New("boom")
	remote := &fakeNode{get: func(context.Context, string) ([]byte, bool, error) { return nil, false, boom }}
	a, _, _, _ := newNode(t, nodeOpts{isLeader: true, minKey: "", maxKey: "m",
		remote: []coordinator.ShardNodeConfig{{MinKey: "m", MaxKey: "", Leader: remote}}})
	_, found, err := a.ForwardGet(ctxT(t), "z")
	if err == nil || found {
		t.Fatalf("ForwardGet with failing server = (found %v, err %v), want an error", found, err)
	}
}

// A follower proxies a forwarded write to its own leader; the deadline set by
// the original caller must reach that leader.
func TestGRPC_DeadlinePropagatesToServer(t *testing.T) {
	seen := make(chan error, 1)
	leader := &fakeNode{put: func(ctx context.Context, _ string, _ []byte) error {
		select {
		case <-ctx.Done():
			seen <- ctx.Err()
			return ctx.Err()
		case <-time.After(5 * time.Second):
			seen <- errors.New("server context never ended")
			return nil
		}
	}}
	a, _, _, _ := newNode(t, nodeOpts{isLeader: false, leaderClient: leader})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := a.ForwardPut(ctx, "k", []byte("v"))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("client err = %v (code %v), want DeadlineExceeded", err, status.Code(err))
	}
	if serr := <-seen; !errors.Is(serr, context.DeadlineExceeded) && !errors.Is(serr, context.Canceled) {
		t.Fatalf("server-side ctx ended with %v, want deadline/cancel", serr)
	}
}

func TestGRPC_ForwardPut_ReadOnlyIsNotInternal(t *testing.T) {
	_, raw, _, _ := newNode(t, nodeOpts{isLeader: false}) // follower, no leader client
	_, err := raw.ForwardPut(ctxT(t), &ForwardPutRequest{Key: "k", Value: []byte("v")})
	if err == nil {
		t.Fatal("ForwardPut on a read-only follower succeeded, want an error")
	}
	if c := status.Code(err); c != codes.FailedPrecondition && c != codes.PermissionDenied {
		t.Fatalf("BUG B-8 (gRPC server wraps every error as Internal): ForwardPut on read-only follower "+
			"got code %v, want FailedPrecondition or PermissionDenied", c)
	}
}

// The adapter maps codes.NotFound back to coordinator.ErrNotFound, so the
// server must send NotFound when the coordinator reports a missing key.
// Isolated from B-7 by serving a follower whose leader returns ErrNotFound.
func TestGRPC_ForwardDelete_NotFoundReachesCaller(t *testing.T) {
	leader := &fakeNode{delete: func(context.Context, string) error { return coordinator.ErrNotFound }}
	a, _, _, _ := newNode(t, nodeOpts{isLeader: false, leaderClient: leader})
	err := a.ForwardDelete(ctxT(t), "missing")
	if !errors.Is(err, coordinator.ErrNotFound) {
		t.Fatalf("BUG B-13 (gRPC ForwardDelete turns ErrNotFound into success, so proxied deletes of "+
			"missing keys report success): got err %v, want coordinator.ErrNotFound", err)
	}
}

func TestGRPC_Heartbeat_Unimplemented(t *testing.T) {
	_, raw, _, _ := newNode(t, nodeOpts{isLeader: true})
	_, err := raw.Heartbeat(ctxT(t), &HeartbeatRequest{NodeId: "x"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Heartbeat err = %v, want Unimplemented", err)
	}
}

/* ====================================================================================
	ReplicateEntry / GetSeqNum
==================================================================================== */

func TestGRPC_ReplicateEntry(t *testing.T) {
	a, raw, _, engine := newNode(t, nodeOpts{isLeader: false})
	ctx := ctxT(t)

	if err := a.ReplicateEntry(ctx, coordinator.LogEntry{SeqNum: 1, Operation: "PUT", Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("replicate PUT: %v", err)
	}
	if v, err := engine.Get("k"); err != nil || string(v) != "v" {
		t.Fatalf("after replicated PUT Get(k) = (%q, %v)", v, err)
	}
	if err := a.ReplicateEntry(ctx, coordinator.LogEntry{SeqNum: 2, Operation: "DELETE", Key: "k"}); err != nil {
		t.Fatalf("replicate DELETE: %v", err)
	}
	if _, err := engine.Get("k"); !errors.Is(err, storageengine.ErrKeyNotFound) {
		t.Fatalf("after replicated DELETE Get(k) err = %v, want ErrKeyNotFound", err)
	}
	if err := a.ReplicateEntry(ctx, coordinator.LogEntry{SeqNum: 3, Operation: "DELETE", Key: "never"}); err != nil {
		t.Fatalf("replicated DELETE of absent key must be idempotent, got %v", err)
	}
	if err := a.ReplicateEntry(ctx, coordinator.LogEntry{SeqNum: 4, Operation: "BOGUS", Key: "k"}); err == nil {
		t.Fatal("replicate unknown op: want error, got nil")
	}

	resp, err := raw.ReplicateEntry(ctx, &ReplicateEntryRequest{Entry: nil})
	if err != nil {
		t.Fatalf("nil entry: transport err %v", err)
	}
	if resp.Success || resp.ErrorDetail == "" {
		t.Fatalf("nil entry: resp = %+v, want Success=false with detail", resp)
	}
}

func TestGRPC_GetSeqNum(t *testing.T) {
	a, raw, coord, _ := newNode(t, nodeOpts{isLeader: true})
	ctx := ctxT(t)
	for i := 0; i < 3; i++ {
		if err := coord.Put(ctx, fmt.Sprint(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	seq, err := a.GetSeqNum(ctx)
	if err != nil || seq != 3 {
		t.Fatalf("GetSeqNum = (%d, %v), want 3", seq, err)
	}
	resp, err := raw.GetSeqNum(ctx, &GetSeqNumRequest{})
	if err != nil || resp.NodeId != "node-under-test" {
		t.Fatalf("raw GetSeqNum = (%+v, %v), want NodeId node-under-test", resp, err)
	}
}

/* ====================================================================================
	Replay
==================================================================================== */

func seed(t *testing.T, coord *coordinator.Coordinator, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := coord.Put(context.Background(), fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGRPC_Replay_Chunking(t *testing.T) {
	cases := []struct {
		n      int
		chunks []int
	}{
		{0, []int{0}},
		{500, []int{500, 0}},
		{1201, []int{500, 500, 201}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			_, raw, coord, _ := newNode(t, nodeOpts{isLeader: true})
			seed(t, coord, tc.n)

			stream, err := raw.Replay(ctxT(t), &ReplayRequest{})
			if err != nil {
				t.Fatal(err)
			}
			var sizes []int
			next := 0
			for {
				chunk, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Recv: %v", err)
				}
				sizes = append(sizes, len(chunk.Entries))
				last := len(sizes) == len(tc.chunks)
				if chunk.IsLast != last {
					t.Errorf("chunk %d IsLast = %v, want %v", len(sizes)-1, chunk.IsLast, last)
				}
				if chunk.IsLast && chunk.SenderSeqNum != uint64(tc.n) {
					t.Errorf("final chunk SenderSeqNum = %d, want %d", chunk.SenderSeqNum, tc.n)
				}
				for _, e := range chunk.Entries {
					wantK, wantV := fmt.Sprintf("key-%05d", next), fmt.Sprintf("val-%d", next)
					if e.Key != wantK || string(e.Value) != wantV {
						t.Fatalf("entry %d = (%q, %q), want (%q, %q)", next, e.Key, e.Value, wantK, wantV)
					}
					next++
				}
			}
			if fmt.Sprint(sizes) != fmt.Sprint(tc.chunks) {
				t.Fatalf("chunk sizes = %v, want %v", sizes, tc.chunks)
			}
			if next != tc.n {
				t.Fatalf("received %d entries, want %d", next, tc.n)
			}
		})
	}
}

func TestGRPC_ReplaySnapshot_Adapter(t *testing.T) {
	a, _, coord, _ := newNode(t, nodeOpts{isLeader: true})
	seed(t, coord, 1201)

	t.Run("delivers all chunks", func(t *testing.T) {
		var sizes []int
		var lastFlags []bool
		var finalSeq uint64
		got := 0
		err := a.ReplaySnapshot(ctxT(t), 0, func(seq uint64, entries []coordinator.LogEntry, isLast bool) error {
			sizes = append(sizes, len(entries))
			lastFlags = append(lastFlags, isLast)
			if isLast {
				finalSeq = seq
			}
			got += len(entries)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(sizes) != "[500 500 201]" || fmt.Sprint(lastFlags) != "[false false true]" {
			t.Fatalf("chunks = %v last = %v, want [500 500 201] [false false true]", sizes, lastFlags)
		}
		if got != 1201 || finalSeq != 1201 {
			t.Fatalf("got %d entries, final seq %d; want 1201, 1201", got, finalSeq)
		}
	})

	t.Run("callback error stops", func(t *testing.T) {
		stop := errors.New("stop")
		calls := 0
		err := a.ReplaySnapshot(ctxT(t), 0, func(uint64, []coordinator.LogEntry, bool) error {
			calls++
			return stop
		})
		if !errors.Is(err, stop) || calls != 1 {
			t.Fatalf("ReplaySnapshot = %v after %d callbacks, want %v after 1", err, calls, stop)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		// Already-cancelled context: deterministic, unlike cancelling mid-stream
		// (chunks already buffered client-side may still be delivered).
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := a.ReplaySnapshot(ctx, 0, func(uint64, []coordinator.LogEntry, bool) error {
			calls++
			return nil
		})
		if status.Code(err) != codes.Canceled || calls != 0 {
			t.Fatalf("ReplaySnapshot with cancelled ctx = %v after %d callbacks, want codes.Canceled after 0", err, calls)
		}
	})
}

// End-to-end: an empty follower recovers 600 keys (two chunks) from a real leader over gRPC.
func TestGRPC_FollowerRecoversFromLeaderOverGRPC(t *testing.T) {
	// Both engines live in one temp dir and share data/wal/0.log. Neither is
	// reopened from disk and nothing is flushed, so all reads come from the
	// memtables and the shared file is harmless.
	leaderAdapter, _, leader, _ := newNode(t, nodeOpts{isLeader: true})
	seed(t, leader, 600)
	if err := leader.Delete(context.Background(), "key-00007"); err != nil {
		t.Fatal(err)
	}

	follower := coordinator.NewCoordinator(openEngine(t), coordinator.NewShardRouter(nil), leaderAdapter, "", "", false, nil)
	// The shared WAL makes the follower's disk look non-empty; that is fine
	// because it is behind (seq 0 < 601) and recovers anyway.
	if err := follower.RecoverIfNeeded(ctxT(t)); err != nil {
		t.Fatalf("RecoverIfNeeded: %v", err)
	}
	if got := follower.GetSeqNum(); got != 601 {
		t.Fatalf("follower seq after recovery = %d, want 601", got)
	}
	for _, i := range []int{0, 499, 500, 599} {
		k := fmt.Sprintf("key-%05d", i)
		v, err := follower.Get(context.Background(), k)
		if err != nil || string(v) != fmt.Sprintf("val-%d", i) {
			t.Fatalf("follower Get(%s) = (%q, %v)", k, v, err)
		}
	}
	if _, err := follower.Get(context.Background(), "key-00007"); !errors.Is(err, coordinator.ErrNotFound) {
		t.Fatalf("follower Get(deleted key) err = %v, want ErrNotFound", err)
	}
}
