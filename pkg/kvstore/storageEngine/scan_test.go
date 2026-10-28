package storageengine

import (
	"errors"
	"testing"
)

// collectScan runs Scan and returns the keys and values in the order they were yielded.
func collectScan(t *testing.T, e *StorageEngine) (keys []string, values [][]byte) {
	t.Helper()
	err := e.Scan(func(key string, value []byte) error {
		keys = append(keys, key)
		cp := make([]byte, len(value))
		copy(cp, value)
		values = append(values, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan returned unexpected error: %v", err)
	}
	return keys, values
}

// TestScan_EmptyEngine verifies Scan on an empty engine returns nothing.
func TestScan_EmptyEngine(t *testing.T) {
	e := newTestEngine(t)
	keys, _ := collectScan(t, e)
	if len(keys) != 0 {
		t.Errorf("empty engine: got %d keys, want 0", len(keys))
	}
}

// TestScan_MemtableOnly verifies Scan returns memtable entries in sorted order.
func TestScan_MemtableOnly(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Put("banana", []byte("b"))
	_ = e.Put("apple", []byte("a"))
	_ = e.Put("cherry", []byte("c"))

	keys, values := collectScan(t, e)
	wantKeys := []string{"apple", "banana", "cherry"}
	wantVals := []string{"a", "b", "c"}
	if len(keys) != len(wantKeys) {
		t.Fatalf("got %d keys %v, want %v", len(keys), keys, wantKeys)
	}
	for i := range wantKeys {
		if keys[i] != wantKeys[i] {
			t.Errorf("keys[%d] = %q, want %q", i, keys[i], wantKeys[i])
		}
		if string(values[i]) != wantVals[i] {
			t.Errorf("values[%d] = %q, want %q", i, values[i], wantVals[i])
		}
	}
}

// TestScan_TombstonesOmitted verifies that deleted keys are not yielded.
func TestScan_TombstonesOmitted(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Put("apple", []byte("a"))
	_ = e.Put("banana", []byte("b"))
	_ = e.Delete("apple")

	keys, _ := collectScan(t, e)
	for _, k := range keys {
		if k == "apple" {
			t.Error("Scan yielded deleted key \"apple\"")
		}
	}
	if len(keys) != 1 || keys[0] != "banana" {
		t.Errorf("got keys %v, want [banana]", keys)
	}
}

// TestScan_SortedOrder verifies that keys are yielded in strict ascending order.
func TestScan_SortedOrder(t *testing.T) {
	e := newTestEngine(t)
	inputs := []string{"mango", "apple", "zebra", "ant", "kiwi"}
	for _, k := range inputs {
		_ = e.Put(k, []byte(k))
	}

	keys, _ := collectScan(t, e)
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("out of order: keys[%d]=%q <= keys[%d]=%q", i, keys[i], i-1, keys[i-1])
		}
	}
}

// TestScan_YieldErrorAbortsEarly verifies that a non-nil error from yield aborts the scan.
func TestScan_YieldErrorAbortsEarly(t *testing.T) {
	e := newTestEngine(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		_ = e.Put(k, []byte(k))
	}

	sentinel := errors.New("stop")
	count := 0
	err := e.Scan(func(key string, value []byte) error {
		count++
		if count == 2 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Scan error = %v, want sentinel", err)
	}
	if count != 2 {
		t.Errorf("yield called %d times, want 2", count)
	}
}

// TestScan_WithFlushedSST verifies merge of memtable + SST data.
func TestScan_WithFlushedSST(t *testing.T) {
	// Use a tiny memCapacity so a Put triggers a flush to SST.
	// The engine memCapacity is set very low; we manually flush to keep the
	// test deterministic without relying on goroutine timing.
	e := newTestEngine(t)

	// Write keys that will end up in an SST after flush.
	_ = e.Put("alpha", []byte("1"))
	_ = e.Put("gamma", []byte("3"))

	// Flush manually: flush() rotates active → immutable → SST.
	if err := e.flush(); err != nil {
		t.Fatalf("manual flush: %v", err)
	}

	// Wait briefly for the flush goroutine if needed (flush is synchronous here).
	// Add new keys to the fresh active memtable.
	_ = e.Put("beta", []byte("2"))
	_ = e.Put("delta", []byte("4"))

	keys, _ := collectScan(t, e)
	want := []string{"alpha", "beta", "delta", "gamma"}
	if len(keys) != len(want) {
		t.Fatalf("got keys %v, want %v", keys, want)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("keys[%d] = %q, want %q", i, keys[i], k)
		}
	}
}

// TestScan_DeduplicateAcrossSources verifies that when the same key exists in
// both memtable and SST, only the version with the higher seq is yielded.
func TestScan_DeduplicateAcrossSources(t *testing.T) {
	e := newTestEngine(t)

	// Write "foo" once, flush it to an SST (old version, lower seq).
	_ = e.Put("foo", []byte("old"))
	if err := e.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Overwrite "foo" in the fresh active memtable (new version, higher seq).
	_ = e.Put("foo", []byte("new"))

	keys, values := collectScan(t, e)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d: %v", len(keys), keys)
	}
	if keys[0] != "foo" {
		t.Errorf("key = %q, want \"foo\"", keys[0])
	}
	if string(values[0]) != "new" {
		t.Errorf("value = %q, want \"new\" (newer memtable version)", values[0])
	}
}

// TestScan_TombstoneInMemtableHidesOlderSSTEntry verifies that a tombstone
// in the memtable suppresses an older value for the same key in the SST.
func TestScan_TombstoneInMemtableHidesOlderSSTEntry(t *testing.T) {
	e := newTestEngine(t)

	_ = e.Put("bar", []byte("v1"))
	if err := e.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Delete "bar" in the memtable.
	_ = e.Delete("bar")

	keys, _ := collectScan(t, e)
	for _, k := range keys {
		if k == "bar" {
			t.Error("Scan yielded \"bar\" despite it being deleted in the memtable")
		}
	}
}

// TestScan_MultipleImmutableMemtables verifies Scan merges the active memtable
// plus multiple immutable memtables correctly.
func TestScan_MultipleImmutableMemtables(t *testing.T) {
	e := newTestEngine(t)

	// First batch → flush to immutable but keep in memory (don't flush to SST yet).
	_ = e.Put("cat", []byte("c"))
	_ = e.Put("elephant", []byte("e"))

	// Rotate active → immutable manually without writing to disk.
	e.mu.Lock()
	old := e.active
	e.immutables = append(e.immutables, old)
	e.active = newMemlog(e.nextFileNumber, e.crcTable)
	e.nextFileNumber++
	e.mu.Unlock()

	// Second batch in the new active memtable.
	_ = e.Put("ant", []byte("a"))
	_ = e.Put("dog", []byte("d"))

	keys, _ := collectScan(t, e)
	want := []string{"ant", "cat", "dog", "elephant"}
	if len(keys) != len(want) {
		t.Fatalf("got keys %v, want %v", keys, want)
	}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("keys[%d] = %q, want %q", i, keys[i], k)
		}
	}
}

