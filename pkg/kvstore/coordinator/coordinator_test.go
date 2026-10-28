package coordinator

import (
	"context"
	"errors"
	"os"
	"testing"

	storageengine "kvstore/pkg/kvstore/storageEngine"
)

// mockNodeClient is an in-memory implementation of NodeClient for testing.
type mockNodeClient struct {
	getSeqNumFunc            func(ctx context.Context) (uint64, error)
	replaySnapshotFunc       func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error
	forwardGetFunc           func(ctx context.Context, key string) ([]byte, bool, error)
	forwardPutFunc           func(ctx context.Context, key string, value []byte) error
	forwardDeleteFunc        func(ctx context.Context, key string) error
	replicateEntryFunc       func(ctx context.Context, entry LogEntry) error
}

func (m *mockNodeClient) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	if m.forwardGetFunc != nil {
		return m.forwardGetFunc(ctx, key)
	}
	return nil, false, nil
}

func (m *mockNodeClient) ForwardPut(ctx context.Context, key string, value []byte) error {
	if m.forwardPutFunc != nil {
		return m.forwardPutFunc(ctx, key, value)
	}
	return nil
}

func (m *mockNodeClient) ForwardDelete(ctx context.Context, key string) error {
	if m.forwardDeleteFunc != nil {
		return m.forwardDeleteFunc(ctx, key)
	}
	return nil
}

func (m *mockNodeClient) ReplicateEntry(ctx context.Context, entry LogEntry) error {
	if m.replicateEntryFunc != nil {
		return m.replicateEntryFunc(ctx, entry)
	}
	return nil
}

func (m *mockNodeClient) GetSeqNum(ctx context.Context) (uint64, error) {
	if m.getSeqNumFunc != nil {
		return m.getSeqNumFunc(ctx)
	}
	return 0, nil
}

func (m *mockNodeClient) ReplaySnapshot(
	ctx context.Context,
	recoveringSeqNum uint64,
	onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error,
) error {
	if m.replaySnapshotFunc != nil {
		return m.replaySnapshotFunc(ctx, recoveringSeqNum, onChunk)
	}
	return nil
}

func setupTestCoordinator(t *testing.T, isLeader bool, leaderClient NodeClient) (*Coordinator, *storageengine.StorageEngine) {
	return setupTestCoordinatorWithFollowers(t, isLeader, leaderClient, nil)
}

func setupTestCoordinatorWithFollowers(t *testing.T, isLeader bool, leaderClient NodeClient, followers []NodeClient) (*Coordinator, *storageengine.StorageEngine) {
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

	engine, err := storageengine.OpenStorageEngine(64*1024*1024, 64*1024*1024, 4096, 10)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}

	router := NewShardRouter(nil)
	coord := NewCoordinator(engine, router, leaderClient, "", "", isLeader, followers)
	return coord, engine
}

func TestRecoverIfNeeded_LeaderSkips(t *testing.T) {
	called := false
	mockLeader := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			called = true
			return 100, nil
		},
	}

	coord, _ := setupTestCoordinator(t, true, mockLeader) // isLeader = true

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Errorf("expected leader to skip recovery, but GetSeqNum was called")
	}
}

func TestRecoverIfNeeded_LeaderHasNoData(t *testing.T) {
	replayCalled := false
	mockLeader := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil // leader seq is 0
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			replayCalled = true
			return nil
		},
	}

	coord, _ := setupTestCoordinator(t, false, mockLeader)

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replayCalled {
		t.Errorf("ReplaySnapshot should not be called when leader seq is 0")
	}
}

func TestRecoverIfNeeded_FollowerRecoversEmptyDisk(t *testing.T) {
	donorSeq := uint64(42)
	snapshotEntries := []LogEntry{
		{Key: "user:1", Value: []byte("Alice")},
		{Key: "user:2", Value: []byte("Bob")},
	}

	mockLeader := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return donorSeq, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			// Deliver in one final chunk
			return onChunk(donorSeq, snapshotEntries, true)
		},
	}

	coord, engine := setupTestCoordinator(t, false, mockLeader)

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("RecoverIfNeeded: %v", err)
	}

	// Verify entries were applied to local storage engine
	val1, err := engine.Get("user:1")
	if err != nil || string(val1) != "Alice" {
		t.Errorf("Get(user:1) = %q, err = %v; want Alice", val1, err)
	}

	val2, err := engine.Get("user:2")
	if err != nil || string(val2) != "Bob" {
		t.Errorf("Get(user:2) = %q, err = %v; want Bob", val2, err)
	}

	// Verify sequence number was updated
	if coord.GetSeqNum() != donorSeq {
		t.Errorf("coord seqnum = %d, want %d", coord.GetSeqNum(), donorSeq)
	}
}

func TestRecoverIfNeeded_ReplayErrorPropagates(t *testing.T) {
	sentinel := errors.New("stream failure")
	mockLeader := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 10, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			return sentinel
		},
	}

	coord, _ := setupTestCoordinator(t, false, mockLeader)

	err := coord.RecoverIfNeeded(context.Background())
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error %v, got %v", sentinel, err)
	}
}

func TestRecoverIfNeeded_LeaderWithLocalData_DoesNotRecover(t *testing.T) {
	followerReplayCalled := false
	mockFollower := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 100, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			followerReplayCalled = true
			return nil
		},
	}

	coord, engine := setupTestCoordinatorWithFollowers(t, true, nil, []NodeClient{mockFollower})
	// Pre-populate leader with local data
	if err := engine.Put("existingKey", []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if followerReplayCalled {
		t.Errorf("leader with existing local data should not recover from followers")
	}
}

func TestRecoverIfNeeded_LeaderEmptyDisk_RecoversFromBestFollower(t *testing.T) {
	f1ReplayCalled := false
	f2ReplayCalled := false

	f1 := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 50, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			f1ReplayCalled = true
			return nil
		},
	}

	donorSeq := uint64(120)
	snapshotEntries := []LogEntry{
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: []byte("v2")},
	}
	f2 := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return donorSeq, nil // follower 2 is more up to date
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			f2ReplayCalled = true
			return onChunk(donorSeq, snapshotEntries, true)
		},
	}

	coord, engine := setupTestCoordinatorWithFollowers(t, true, nil, []NodeClient{f1, f2})

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("RecoverIfNeeded: %v", err)
	}

	if f1ReplayCalled {
		t.Errorf("f1 should not have been chosen as donor (has lower seq)")
	}
	if !f2ReplayCalled {
		t.Errorf("f2 should have been chosen as snapshot donor")
	}

	// Verify entries were applied to leader's local storage engine
	v1, err := engine.Get("k1")
	if err != nil || string(v1) != "v1" {
		t.Errorf("Get(k1) = %q, err = %v; want v1", v1, err)
	}
	v2, err := engine.Get("k2")
	if err != nil || string(v2) != "v2" {
		t.Errorf("Get(k2) = %q, err = %v; want v2", v2, err)
	}

	// Verify leader's sequence number was synced
	if coord.GetSeqNum() != donorSeq {
		t.Errorf("leader seqnum = %d, want %d", coord.GetSeqNum(), donorSeq)
	}
}

func TestRecoverIfNeeded_LeaderEmptyDisk_AllFollowersEmpty(t *testing.T) {
	replayCalled := false
	f1 := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 0, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			replayCalled = true
			return nil
		},
	}

	coord, _ := setupTestCoordinatorWithFollowers(t, true, nil, []NodeClient{f1})

	if err := coord.RecoverIfNeeded(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replayCalled {
		t.Errorf("ReplaySnapshot should not be called when all followers have seq 0")
	}
}

func TestRecoverIfNeeded_LeaderEmptyDisk_FollowerReplayFails(t *testing.T) {
	sentinel := errors.New("follower replay failed")
	f1 := &mockNodeClient{
		getSeqNumFunc: func(ctx context.Context) (uint64, error) {
			return 100, nil
		},
		replaySnapshotFunc: func(ctx context.Context, recoveringSeqNum uint64, onChunk func(donorSeqNum uint64, entries []LogEntry, isLast bool) error) error {
			return sentinel
		},
	}

	coord, _ := setupTestCoordinatorWithFollowers(t, true, nil, []NodeClient{f1})

	err := coord.RecoverIfNeeded(context.Background())
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error %v, got %v", sentinel, err)
	}
}

