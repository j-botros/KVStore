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
	coord := NewCoordinator(engine, router, leaderClient, "", "", isLeader, nil)
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

