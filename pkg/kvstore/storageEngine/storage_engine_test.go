package storageengine

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStorageEngine_PutAndGet(t *testing.T) {
	e := newTestEngine(t)

	key := "name"
	valPut := []byte("alice")
	if err := e.Put(key, valPut); err != nil {
		t.Fatalf("Put: %v", err)
	}

	keyGet := "name"
	val, err := e.Get(keyGet)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "alice" {
		t.Errorf("Get(\"name\") = %q, want %q", val, "alice")
	}
}

func TestStorageEngine_Get_NotFound(t *testing.T) {
	e := newTestEngine(t)

	key := "nope"
	_, err := e.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Get(\"nope\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestStorageEngine_Delete(t *testing.T) {
	e := newTestEngine(t)

	key1 := "key"
	val1 := []byte("val")
	if err := e.Put(key1, val1); err != nil {
		t.Fatalf("Put: %v", err)
	}
	keyDel := "key"
	if err := e.Delete(keyDel); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	keyGet := "key"
	_, err := e.Get(keyGet)
	if err != ErrKeyNotFound {
		t.Errorf("Get after Delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestStorageEngine_PutOverwrite(t *testing.T) {
	e := newTestEngine(t)

	key1 := "key"
	val1 := []byte("first")
	e.Put(key1, val1)

	key2 := "key"
	val2 := []byte("second")
	e.Put(key2, val2)

	keyGet := "key"
	val, err := e.Get(keyGet)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "second" {
		t.Errorf("Get(\"key\") = %q, want %q", val, "second")
	}
}

func TestStorageEngine_DeleteThenPut(t *testing.T) {
	e := newTestEngine(t)

	key1 := "key"
	val1 := []byte("original")
	e.Put(key1, val1)

	keyDel := "key"
	e.Delete(keyDel)

	key2 := "key"
	val2 := []byte("reborn")
	e.Put(key2, val2)

	keyGet := "key"
	val, err := e.Get(keyGet)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "reborn" {
		t.Errorf("Get(\"key\") = %q, want %q", val, "reborn")
	}
}

func TestStorageEngine_SequenceIncrement(t *testing.T) {
	e := newTestEngine(t)

	initial := e.nextSeq

	key1 := "a"
	val1 := []byte("1")
	e.Put(key1, val1)
	if e.nextSeq != initial+1 {
		t.Errorf("after Put: nextSeq = %d, want %d", e.nextSeq, initial+1)
	}

	keyDel := "a"
	e.Delete(keyDel)
	if e.nextSeq != initial+2 {
		t.Errorf("after Delete: nextSeq = %d, want %d", e.nextSeq, initial+2)
	}

	key2 := "b"
	val2 := []byte("2")
	e.Put(key2, val2)
	if e.nextSeq != initial+3 {
		t.Errorf("after second Put: nextSeq = %d, want %d", e.nextSeq, initial+3)
	}
}

func TestStorageEngine_Flush(t *testing.T) {
	e := newTestEngine(t)

	e.Put("k1", []byte("v1"))
	e.Put("k2", []byte("v2"))

	if err := e.flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify key can still be retrieved after flush to SSTable
	val, err := e.Get("k1")
	if err != nil {
		t.Fatalf("Get(\"k1\") after Flush: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("Get(\"k1\") = %q, want \"v1\"", val)
	}

	// Verify immutables queue is empty after flush completed
	e.mu.RLock()
	immLen := len(e.immutables)
	e.mu.RUnlock()

	if immLen != 0 {
		t.Errorf("len(immutables) = %d, want 0 after Flush", immLen)
	}
}

/* ====================================================================================
	COMPACTION TESTS
==================================================================================== */

func TestStorageEngine_Compact_Basic(t *testing.T) {
	e := newTestEngine(t)

	// Insert data and flush to create an SST in L0
	e.Put("a", []byte("1"))
	e.Put("b", []byte("2"))
	e.Put("c", []byte("3"))
	if err := e.flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	l0 := e.sstables.levels[0]
	if len(l0.sstList) != 1 {
		t.Fatalf("expected 1 SST in L0, got %d", len(l0.sstList))
	}
	srcSst := l0.sstList[0]

	if err := e.compact(srcSst); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify L0 is empty
	if len(l0.sstList) != 0 {
		t.Errorf("expected L0 to be empty after compaction, got %d SSTs", len(l0.sstList))
	}
	if l0.sizeBytes != 0 {
		t.Errorf("expected L0 sizeBytes to be 0, got %d", l0.sizeBytes)
	}

	// Verify L1 has the compacted SST
	l1 := e.sstables.levels[1]
	if len(l1.sstList) != 1 {
		t.Fatalf("expected 1 SST in L1, got %d", len(l1.sstList))
	}
	if l1.sizeBytes == 0 {
		t.Errorf("expected L1 sizeBytes > 0")
	}

	// Verify data is readable
	val, err := e.Get("b")
	if err != nil {
		t.Fatalf("Get('b') failed: %v", err)
	}
	if string(val) != "2" {
		t.Errorf("Get('b') = %q, want '2'", val)
	}
}

func TestStorageEngine_Compact_OverlapMerge(t *testing.T) {
	e := newTestEngine(t)
	crcTab := e.crcTable

	// Force-create Level 1 with an existing SST
	e.sstables.levels = append(e.sstables.levels, newLevel(9999))
	l1 := e.sstables.levels[1]

	l1Entries := []testEntry{
		{key: "a", value: []byte("old-a"), seq: 1},
		{key: "c", value: []byte("old-c"), seq: 2},
		{key: "e", value: []byte("old-e"), seq: 3},
	}
	overlapSst := writeSyntheticSST(t, 1, 100, l1Entries, crcTab)
	l1.insertSorted(overlapSst)
	l1.sizeBytes += overlapSst.sizeBytes

	// Flush an SST to L0 that overlaps and updates 'c'
	e.Put("b", []byte("new-b"))
	e.Put("c", []byte("new-c"))
	e.Put("d", []byte("new-d"))
	if err := e.flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	l0 := e.sstables.levels[0]
	srcSst := l0.sstList[0]

	if err := e.compact(srcSst); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// L0 should be empty
	if len(l0.sstList) != 0 {
		t.Errorf("expected L0 to be empty, got %d SSTs", len(l0.sstList))
	}

	// L1 should have exactly one merged SST (overlapSst should be deleted)
	if len(l1.sstList) != 1 {
		t.Fatalf("expected 1 merged SST in L1, got %d", len(l1.sstList))
	}

	// Verify merged data
	expected := map[string]string{
		"a": "old-a", // from L1
		"b": "new-b", // from L0
		"c": "new-c", // updated from L0
		"d": "new-d", // from L0
		"e": "old-e", // from L1
	}

	for k, want := range expected {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) failed: %v", k, err)
		} else if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
}

func TestStorageEngine_Compact_MultiOutput(t *testing.T) {
	e := newTestEngine(t)

	// Determine exactly how many bytes 2 entries take up
	cw, _ := newCompactionWriter(newSst(999, 1, e.crcTable, e.sstCapacity))
	cw.writeEntry(&entry{key: "k1", value: []byte("v1"), seq: 1})
	cw.writeEntry(&entry{key: "k2", value: []byte("v2"), seq: 2})
	twoEntrySize := uint64(cw.currentBlockBuf.Len())
	cw.outFile.Close()
	os.Remove(cw.filename)

	// Constrain engine's sstCapacity
	e.sstCapacity = twoEntrySize

	// Write 5 entries to L0
	e.Put("k1", []byte("v1"))
	e.Put("k2", []byte("v2"))
	e.Put("k3", []byte("v3"))
	e.Put("k4", []byte("v4"))
	e.Put("k5", []byte("v5"))
	e.flush()

	srcSst := e.sstables.levels[0].sstList[0]
	if err := e.compact(srcSst); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	l1 := e.sstables.levels[1]
	// 5 entries, 2 per file => 3 files expected
	if len(l1.sstList) != 3 {
		t.Fatalf("expected 3 SSTs in L1 due to capacity splitting, got %d", len(l1.sstList))
	}

	// Verify readability across split files
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) failed: %v", k, err)
		} else if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
}

func TestStorageEngine_Compact_Serialization(t *testing.T) {
	e := newTestEngine(t)

	// Flush an SST so we have something to compact
	e.Put("x", []byte("1"))
	e.flush()

	srcSst := e.sstables.levels[0].sstList[0]

	// Pre-set the compacting flag to simulate an in-progress compaction
	e.compacting.Store(true)

	// A second compaction attempt should return nil immediately
	err := e.compact(srcSst)
	if err != nil {
		t.Errorf("concurrent Compact should return nil, got %v", err)
	}

	// L0 should still have the SST (compaction was skipped)
	if len(e.sstables.levels[0].sstList) != 1 {
		t.Errorf("expected L0 unchanged (1 SST), got %d", len(e.sstables.levels[0].sstList))
	}

	// Release the flag and verify real compaction works
	e.compacting.Store(false)
	if err := e.compact(srcSst); err != nil {
		t.Fatalf("Compact after releasing flag: %v", err)
	}

	if len(e.sstables.levels[0].sstList) != 0 {
		t.Errorf("expected L0 empty after real compaction, got %d", len(e.sstables.levels[0].sstList))
	}
}

func TestStorageEngine_Flush_Concurrent(t *testing.T) {
	e := newTestEngine(t)

	// Insert different keys for two separate flushes
	e.Put("a", []byte("1"))
	e.Put("b", []byte("2"))

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// First flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = e.flush()
	}()

	// Give the first flush a moment to rotate
	time.Sleep(10 * time.Millisecond)

	// Insert more data into the new active memlog
	e.Put("c", []byte("3"))
	e.Put("d", []byte("4"))

	// Second flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = e.flush()
	}()

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Flush %d failed: %v", i, err)
		}
	}

	// Both SSTs should be in L0
	e.sstables.mu.RLock()
	l0Count := len(e.sstables.levels[0].sstList)
	e.sstables.mu.RUnlock()

	if l0Count != 2 {
		t.Errorf("expected 2 SSTs in L0 after concurrent flush, got %d", l0Count)
	}

	// All keys should be readable
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := e.Get(k); err != nil {
			t.Errorf("Get(%q) failed after concurrent flush: %v", k, err)
		}
	}
}

func TestStorageEngine_Flush_RotatesMemlog(t *testing.T) {
	e := newTestEngine(t)

	e.Put("rotate", []byte("me"))

	// Capture the active memlog before flush
	e.mu.RLock()
	oldActive := e.active
	e.mu.RUnlock()

	if err := e.flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Active memlog should be a new instance
	e.mu.RLock()
	newActive := e.active
	immLen := len(e.immutables)
	e.mu.RUnlock()

	if newActive == oldActive {
		t.Error("active memlog was not rotated after Flush")
	}

	if immLen != 0 {
		t.Errorf("immutables should be empty after Flush, got %d", immLen)
	}

	// Data should still be readable from the SSTable
	val, err := e.Get("rotate")
	if err != nil {
		t.Fatalf("Get('rotate') after Flush: %v", err)
	}
	if string(val) != "me" {
		t.Errorf("Get('rotate') = %q, want 'me'", val)
	}
}

func TestStorageEngine_Flush_TriggersCompaction(t *testing.T) {
	// Use a tiny L0 capacity so a single flush puts it over the limit
	e := newTestEngine(t)

	// Set L0 capacity to 0 so that any flush triggers compaction (sizeBytes >= capacityBytes → 0 >= 0)
	e.sstables.mu.Lock()
	e.sstables.levels[0].capacityBytes = 1
	e.sstables.growthFactor = 1000000
	e.sstables.mu.Unlock()

	e.Put("trigger", []byte("compact"))

	if err := e.flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Give the background goroutine time to complete compaction
	time.Sleep(200 * time.Millisecond)

	// L0 should be empty (compaction moved the SST to L1)
	e.sstables.mu.RLock()
	l0Count := len(e.sstables.levels[0].sstList)
	l1Exists := len(e.sstables.levels) > 1
	var l1Count int
	if l1Exists {
		l1Count = len(e.sstables.levels[1].sstList)
	}
	e.sstables.mu.RUnlock()

	if l0Count != 0 {
		t.Errorf("expected L0 empty after triggered compaction, got %d SSTs", l0Count)
	}
	if !l1Exists {
		t.Error("expected L1 to exist after triggered compaction, L1 does not exist")
	}
	if l1Count == 0 {
		t.Errorf("expected L1 to have SSTs after triggered compaction, got %d", l1Count)
	}

	// Data should still be readable
	val, err := e.Get("trigger")
	if err != nil {
		t.Fatalf("Get('trigger') after compaction: %v", err)
	}
	if string(val) != "compact" {
		t.Errorf("Get('trigger') = %q, want 'compact'", val)
	}
}

/* ====================================================================================
	AUTO-FLUSH TESTS (flush triggered by Put / Delete filling the memtable)
==================================================================================== */

// waitForL0Count polls until L0 has at least minCount SSTs or the 2-second deadline
// is exceeded. Returns true if the condition was met.
func waitForL0Count(e *StorageEngine, minCount int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e.sstables.mu.RLock()
		n := len(e.sstables.levels[0].sstList)
		e.sstables.mu.RUnlock()
		if n >= minCount {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestStorageEngine_Put_TriggersAutoFlush verifies that a Put that pushes the
// memtable past memCapacity automatically kicks off a background Flush and
// the data is readable after the SST is written.
func TestStorageEngine_Put_TriggersAutoFlush(t *testing.T) {
	// memCapacity = 1 byte: the first Put will always exceed it.
	e := newTestEngineWithMemCapacity(t, 1)

	if err := e.Put("autoflush", []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if !waitForL0Count(e, 1) {
		t.Fatal("timed out: auto-flush did not produce an SST in L0 within 2s")
	}

	// Data must still be readable (from the SST, since memtable was rotated)
	val, err := e.Get("autoflush")
	if err != nil {
		t.Fatalf("Get after auto-flush: %v", err)
	}
	if string(val) != "yes" {
		t.Errorf("Get = %q, want %q", val, "yes")
	}
}

// TestStorageEngine_Delete_TriggersAutoFlush verifies that a Delete that pushes
// the memtable past memCapacity automatically kicks off a background Flush.
// The tombstone ends up in the SST and ErrKeyNotFound is still returned for
// the deleted key.
func TestStorageEngine_Delete_TriggersAutoFlush(t *testing.T) {
	// memCapacity = 1 byte: the first Delete will always exceed it.
	e := newTestEngineWithMemCapacity(t, 1)

	// Delete a never-inserted key: a tombstone node is created, sizeBytes grows.
	if err := e.Delete("ghost"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !waitForL0Count(e, 1) {
		t.Fatal("timed out: auto-flush did not produce an SST in L0 within 2s")
	}

	// The tombstone should still return ErrKeyNotFound
	_, err := e.Get("ghost")
	if err != ErrKeyNotFound {
		t.Errorf("Get deleted key: err = %v, want ErrKeyNotFound", err)
	}
}

// TestStorageEngine_AutoFlush_MultipleEntries verifies that when the memtable
// fills up after several writes (not just the first), the flush still fires.
func TestStorageEngine_AutoFlush_MultipleEntries(t *testing.T) {
	// Each entry for "kN"/"vN" is ENTRY_OVERHEAD_BYTES(21) + 2 + 2 = 25 bytes.
	// Set capacity to 60 bytes so the third Put triggers the flush.
	e := newTestEngineWithMemCapacity(t, 60)

	e.Put("k1", []byte("v1"))
	e.Put("k2", []byte("v2"))

	// After two puts: 50 bytes < 60 — no flush yet.
	e.sstables.mu.RLock()
	earlyCount := len(e.sstables.levels[0].sstList)
	e.sstables.mu.RUnlock()
	if earlyCount != 0 {
		t.Errorf("expected 0 SSTs before threshold, got %d", earlyCount)
	}

	// Third put: 75 bytes >= 60 — flush should trigger.
	e.Put("k3", []byte("v3"))

	if !waitForL0Count(e, 1) {
		t.Fatal("timed out: auto-flush did not fire after crossing memCapacity")
	}

	// All three keys must be readable.
	for i := 1; i <= 3; i++ {
		k := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) after auto-flush: %v", k, err)
		} else if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
}

/* ====================================================================================
	REBUILD ENGINE TESTS
==================================================================================== */

const (
	rebuildMemCap     = uint64(64 * 1024 * 1024)
	rebuildSstCap     = uint64(64 * 1024 * 1024)
	rebuildL0Cap      = uint64(4096)
	rebuildGrowthFact = 10
)

// TestRebuildEngine_EmptyDataDir verifies that rebuildEngine succeeds on a
// completely fresh environment where no data/ directory exists.
func TestRebuildEngine_EmptyDataDir(t *testing.T) {
	setupTestDir(t)

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if e.active == nil {
		t.Fatal("active memlog is nil")
	}
	// maxSstFilenum=0 → newLogNum=1; nextFileNumber=newLogNum+1=2
	if e.active.wal.logNumber != 1 {
		t.Errorf("active.wal.logNumber = %d, want 1", e.active.wal.logNumber)
	}
	if len(e.immutables) != 0 {
		t.Errorf("immutables len = %d, want 0", len(e.immutables))
	}
	if e.nextSeq != 1 {
		t.Errorf("nextSeq = %d, want 1", e.nextSeq)
	}
	if e.nextFileNumber != 2 {
		t.Errorf("nextFileNumber = %d, want 2 (one past active logNumber=1)", e.nextFileNumber)
	}
	if _, err := e.Get("any"); err != ErrKeyNotFound {
		t.Errorf("Get(\"any\") = %v, want ErrKeyNotFound on empty engine", err)
	}
}

// TestRebuildEngine_SstOnly_NoWal verifies recovery when SSTs exist but the
// WAL directory has been deleted (all memtables fully flushed before crash).
func TestRebuildEngine_SstOnly_NoWal(t *testing.T) {
	live := newTestEngine(t)
	keys := []string{"apple", "banana", "cherry"}
	for _, k := range keys {
		if err := live.Put(k, []byte("v:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Simulate "WAL deleted after flush".
	if err := os.RemoveAll("data/wal"); err != nil {
		t.Fatalf("RemoveAll data/wal: %v", err)
	}

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if len(e.immutables) != 0 {
		t.Errorf("immutables len = %d, want 0", len(e.immutables))
	}
	for _, k := range keys {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
		} else if string(val) != "v:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "v:"+k)
		}
	}
}

// TestRebuildEngine_WalOnly_NoSst verifies recovery when only a WAL exists
// and nothing has been flushed to SSTs.
func TestRebuildEngine_WalOnly_NoSst(t *testing.T) {
	live := newTestEngine(t)
	keys := []string{"dog", "cat", "fish"}
	for _, k := range keys {
		if err := live.Put(k, []byte("v:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	// Deliberate: no flush.
	lastWrittenSeq := live.nextSeq - 1

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if len(e.immutables) != 0 {
		t.Errorf("immutables len = %d, want 0 (one WAL → active only)", len(e.immutables))
	}
	for _, k := range keys {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
		} else if string(val) != "v:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "v:"+k)
		}
	}
	if e.nextSeq <= lastWrittenSeq {
		t.Errorf("nextSeq = %d, want > %d", e.nextSeq, lastWrittenSeq)
	}
}

// TestRebuildEngine_SstAndWal verifies recovery when some keys are in SSTs
// (flushed) and others are only in the WAL (unflushed), and that WAL entries
// already covered by SSTs are not double-counted.
func TestRebuildEngine_SstAndWal(t *testing.T) {
	live := newTestEngine(t)

	batchA := []string{"aa", "ab", "ac"}
	for _, k := range batchA {
		if err := live.Put(k, []byte("sst:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batchB := []string{"ba", "bb", "bc"}
	for _, k := range batchB {
		if err := live.Put(k, []byte("wal:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	for _, k := range batchA {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from SST: %v", k, err)
		} else if string(val) != "sst:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "sst:"+k)
		}
	}
	for _, k := range batchB {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from WAL: %v", k, err)
		} else if string(val) != "wal:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "wal:"+k)
		}
	}
}

// TestRebuildEngine_NextSeqIsStrictlyGreater verifies that nextSeq is always
// strictly greater than the maximum seq seen across SSTs and WALs.
func TestRebuildEngine_NextSeqIsStrictlyGreater(t *testing.T) {
	live := newTestEngine(t)
	for _, k := range []string{"p", "q", "r", "s"} {
		live.Put(k, []byte(k))
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	live.Put("t", []byte("t"))
	live.Put("u", []byte("u"))
	lastSeqBeforeCrash := live.nextSeq - 1

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if e.nextSeq <= lastSeqBeforeCrash {
		t.Errorf("nextSeq = %d, want > %d (last seq on disk)", e.nextSeq, lastSeqBeforeCrash)
	}
}

// TestRebuildEngine_NextFileNumberIsStrictlyGreater verifies that
// nextFileNumber is strictly greater than every filenum seen on disk.
func TestRebuildEngine_NextFileNumberIsStrictlyGreater(t *testing.T) {
	live := newTestEngine(t)
	for i := 0; i < 3; i++ {
		live.Put(fmt.Sprintf("key%d", i), []byte("v"))
		if err := live.flush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	lastFilenumBeforeCrash := live.nextFileNumber - 1

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if e.nextFileNumber <= lastFilenumBeforeCrash {
		t.Errorf("nextFileNumber = %d, want > %d", e.nextFileNumber, lastFilenumBeforeCrash)
	}
}

// TestRebuildEngine_MultipleWals_ImmutableOrder verifies that when multiple
// WAL files exist on disk (e.g. after crash before any flush), all but the
// newest become immutables in ascending logNumber order, and all keys are readable.
func TestRebuildEngine_MultipleWals_ImmutableOrder(t *testing.T) {
	// Build the on-disk state manually: three WAL files with disjoint key sets,
	// simulating a crash that left multiple unflushed WALs.
	setupTestDir(t)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	walEntries := []struct {
		logNum uint64
		key    string
		val    string
	}{
		{1, "w0", "v0"},
		{2, "w1", "v1"},
		{3, "w2", "v2"},
	}
	for _, e := range walEntries {
		w := newWal(e.logNum, crcTab)
		if err := w.writeLog(e.key, []byte(e.val), false, e.logNum); err != nil {
			t.Fatalf("writeLog(%q): %v", e.key, err)
		}
	}

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if len(e.immutables) != 2 {
		t.Errorf("immutables len = %d, want 2", len(e.immutables))
	}
	if len(e.immutables) == 2 {
		i0 := e.immutables[0].wal.logNumber
		i1 := e.immutables[1].wal.logNumber
		act := e.active.wal.logNumber
		if !(i0 < i1 && i1 < act) {
			t.Errorf("logNumber order wrong: imm[0]=%d imm[1]=%d active=%d; want ascending", i0, i1, act)
		}
	}
	for _, we := range walEntries {
		val, err := e.Get(we.key)
		if err != nil {
			t.Errorf("Get(%q): %v", we.key, err)
		} else if string(val) != we.val {
			t.Errorf("Get(%q) = %q, want %q", we.key, val, we.val)
		}
	}
}

// TestRebuildEngine_NonLogFilesIgnored verifies that non-<N>.log files inside
// data/wal/ are silently skipped without causing an error.
func TestRebuildEngine_NonLogFilesIgnored(t *testing.T) {
	live := newTestEngine(t)
	live.Put("realkey", []byte("realval"))
	// No flush — one valid WAL file exists.

	os.WriteFile("data/wal/notes.txt", []byte("junk"), 0644)
	os.WriteFile("data/wal/bad-name.log", []byte("junk"), 0644)
	os.Mkdir("data/wal/subdir", 0755)

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}
	if len(e.immutables) != 0 {
		t.Errorf("immutables len = %d, want 0 (non-.log files must be ignored)", len(e.immutables))
	}
	val, err := e.Get("realkey")
	if err != nil {
		t.Errorf("Get(\"realkey\"): %v", err)
	} else if string(val) != "realval" {
		t.Errorf("Get(\"realkey\") = %q, want \"realval\"", val)
	}
}

// TestRebuildEngine_CorruptSst verifies that a corrupt SST (zeroed magic bytes)
// causes rebuildEngine to return a non-nil error.
func TestRebuildEngine_CorruptSst(t *testing.T) {
	live := newTestEngine(t)
	live.Put("k", []byte("v"))
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Find the SST via the live engine's sstables rather than guessing the filenum.
	live.sstables.mu.RLock()
	if len(live.sstables.levels[0].sstList) == 0 {
		live.sstables.mu.RUnlock()
		t.Fatal("expected at least one SST in L0 after flush")
	}
	sstFilenum := live.sstables.levels[0].sstList[0].filenum
	live.sstables.mu.RUnlock()

	sstPath := fmt.Sprintf("data/sstables/level-0/%d.sst", sstFilenum)
	fi, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat SST: %v", err)
	}
	f, err := os.OpenFile(sstPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open SST: %v", err)
	}
	f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, fi.Size()-8)
	f.Close()

	_, err = rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err == nil {
		t.Fatal("expected error for corrupt SST, got nil")
	}
}

// TestRebuildEngine_RoundTrip is the primary end-to-end recovery test:
// write → simulate crash → rebuild → verify all reads → confirm the
// recovered engine accepts new writes without seq/filenum collisions.
func TestRebuildEngine_RoundTrip(t *testing.T) {
	live := newTestEngine(t)

	batchA := map[string]string{"city": "austin", "state": "texas", "country": "usa"}
	for k, v := range batchA {
		if err := live.Put(k, []byte(v)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batchB := map[string]string{"river": "colorado", "lake": "travis"}
	for k, v := range batchB {
		if err := live.Put(k, []byte(v)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	seqBeforeCrash := live.nextSeq - 1
	filenumBeforeCrash := live.nextFileNumber - 1
	live = nil // simulate crash

	e, err := rebuildEngine(rebuildMemCap, rebuildSstCap, rebuildL0Cap, rebuildGrowthFact)
	if err != nil {
		t.Fatalf("rebuildEngine: %v", err)
	}

	for k, want := range batchA {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from SST: %v", k, err)
		} else if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
	for k, want := range batchB {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from WAL: %v", k, err)
		} else if string(val) != want {
			t.Errorf("Get(%q) = %q, want %q", k, val, want)
		}
	}
	if e.nextSeq <= seqBeforeCrash {
		t.Errorf("nextSeq = %d, want > %d", e.nextSeq, seqBeforeCrash)
	}
	if e.nextFileNumber <= filenumBeforeCrash {
		t.Errorf("nextFileNumber = %d, want > %d", e.nextFileNumber, filenumBeforeCrash)
	}

	// Engine must be fully operational post-recovery.
	if err := e.Put("newkey", []byte("newval")); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	val, err := e.Get("newkey")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(val) != "newval" {
		t.Errorf("Get(\"newkey\") = %q, want \"newval\"", val)
	}
}

/* ====================================================================================
	OPEN STORAGE ENGINE TESTS
==================================================================================== */

const (
	openMemCap     = uint64(64 * 1024 * 1024)
	openSstCap     = uint64(64 * 1024 * 1024)
	openL0Cap      = uint64(4096)
	openGrowthFact = 10
)

// TestOpenStorageEngine_NoDataDir_FreshEngine verifies that when no data/
// directory exists, OpenStorageEngine returns a clean, usable engine.
func TestOpenStorageEngine_NoDataDir_FreshEngine(t *testing.T) {
	setupTestDir(t)
	// No data/ directory created — first-run path.

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	if e == nil {
		t.Fatal("returned engine is nil")
	}
	if e.active == nil {
		t.Fatal("active memlog is nil")
	}
	if e.nextSeq != 0 {
		t.Errorf("nextSeq = %d, want 0 on fresh engine", e.nextSeq)
	}
	if _, err := e.Get("any"); err != ErrKeyNotFound {
		t.Errorf("Get(\"any\") = %v, want ErrKeyNotFound", err)
	}
}

// TestOpenStorageEngine_EmptyDataDir_FreshEngine verifies that an empty data/
// directory (no subdirs) routes to rebuildEngine and returns a valid engine
// with no stored data.
func TestOpenStorageEngine_EmptyDataDir_FreshEngine(t *testing.T) {
	setupTestDir(t)
	if err := os.Mkdir("data", 0755); err != nil {
		t.Fatal(err)
	}

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	if e.active == nil {
		t.Fatal("active memlog is nil")
	}
	if len(e.immutables) != 0 {
		t.Errorf("immutables len = %d, want 0", len(e.immutables))
	}
	if _, err := e.Get("any"); err != ErrKeyNotFound {
		t.Errorf("Get(\"any\") = %v, want ErrKeyNotFound", err)
	}
}

// TestOpenStorageEngine_WalOnly_RecoverData verifies recovery when only a WAL
// exists (nothing flushed to SSTs).
func TestOpenStorageEngine_WalOnly_RecoverData(t *testing.T) {
	live := newTestEngine(t)
	keys := []string{"dog", "cat", "fish"}
	for _, k := range keys {
		if err := live.Put(k, []byte("v:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	lastWrittenSeq := live.nextSeq - 1
	live = nil // simulate crash

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	for _, k := range keys {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
		} else if string(val) != "v:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "v:"+k)
		}
	}
	if e.nextSeq <= lastWrittenSeq {
		t.Errorf("nextSeq = %d, want > %d", e.nextSeq, lastWrittenSeq)
	}
}

// TestOpenStorageEngine_SstAndWal_RecoverData verifies recovery when some keys
// are in SSTs (flushed) and others are only in the WAL (unflushed).
func TestOpenStorageEngine_SstAndWal_RecoverData(t *testing.T) {
	live := newTestEngine(t)

	batchA := []string{"aa", "ab", "ac"}
	for _, k := range batchA {
		if err := live.Put(k, []byte("sst:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batchB := []string{"ba", "bb", "bc"}
	for _, k := range batchB {
		if err := live.Put(k, []byte("wal:"+k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	live = nil // simulate crash

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	for _, k := range batchA {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from SST: %v", k, err)
		} else if string(val) != "sst:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "sst:"+k)
		}
	}
	for _, k := range batchB {
		val, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) from WAL: %v", k, err)
		} else if string(val) != "wal:"+k {
			t.Errorf("Get(%q) = %q, want %q", k, val, "wal:"+k)
		}
	}
}

// TestOpenStorageEngine_FreshEngine_IsOperational verifies that a fresh engine
// (no data/ dir) accepts writes and returns correct values.
func TestOpenStorageEngine_FreshEngine_IsOperational(t *testing.T) {
	setupTestDir(t)

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	if err := e.Put("hello", []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := e.Get("hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("Get(\"hello\") = %q, want \"world\"", val)
	}
}

// TestOpenStorageEngine_RecoveredEngine_IsOperational verifies that after
// recovery, the engine accepts new writes without seq or filenum collisions,
// and that a subsequent flush succeeds.
func TestOpenStorageEngine_RecoveredEngine_IsOperational(t *testing.T) {
	live := newTestEngine(t)
	if err := live.Put("existing", []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := live.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	seqBeforeCrash := live.nextSeq - 1
	filenumBeforeCrash := live.nextFileNumber - 1
	live = nil // simulate crash

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("OpenStorageEngine: %v", err)
	}
	// Counters must be strictly greater than anything on disk.
	if e.nextSeq <= seqBeforeCrash {
		t.Errorf("nextSeq = %d, want > %d", e.nextSeq, seqBeforeCrash)
	}
	if e.nextFileNumber <= filenumBeforeCrash {
		t.Errorf("nextFileNumber = %d, want > %d", e.nextFileNumber, filenumBeforeCrash)
	}
	// New writes and flush must succeed without panic or error.
	if err := e.Put("newkey", []byte("newval")); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if err := e.flush(); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	val, err := e.Get("newkey")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(val) != "newval" {
		t.Errorf("Get(\"newkey\") = %q, want \"newval\"", val)
	}
}

// TestOpenStorageEngine_StatError_ReturnsError verifies that errors encountered
// during recovery are propagated back as a non-nil error. We create data/ as a
// regular file (not a directory) so that rebuildEngine's os.ReadDir calls fail.
func TestOpenStorageEngine_StatError_ReturnsError(t *testing.T) {
	setupTestDir(t)
	// Create data as a file, not a directory — os.Stat succeeds (no IsNotExist),
	// so OpenStorageEngine routes to rebuildEngine, which then fails when it
	// tries to ReadDir("data/sstables").
	if err := os.WriteFile("data", []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	e, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err == nil {
		t.Fatal("expected non-nil error when data/ is a file, got nil")
	}
	if e != nil {
		t.Errorf("expected nil engine on error, got non-nil")
	}
}

// TestOpenStorageEngine_Idempotent_MultipleOpens verifies that opening the
// engine twice on the same data directory (clean shutdown between opens)
// correctly recovers flushed data on the second open.
func TestOpenStorageEngine_Idempotent_MultipleOpens(t *testing.T) {
	// First open: fresh engine, write + flush.
	setupTestDir(t)
	e1, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("first OpenStorageEngine: %v", err)
	}
	if err := e1.Put("city", []byte("austin")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e1.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	e1 = nil // clean shutdown

	// Second open: data/ now exists, should recover the flushed SST.
	e2, err := OpenStorageEngine(openMemCap, openSstCap, openL0Cap, openGrowthFact)
	if err != nil {
		t.Fatalf("second OpenStorageEngine: %v", err)
	}
	val, err := e2.Get("city")
	if err != nil {
		t.Fatalf("Get after second open: %v", err)
	}
	if string(val) != "austin" {
		t.Errorf("Get(\"city\") = %q, want \"austin\"", val)
	}
}

/* ====================================================================================
	U6: READ ORDER, RESTART MATRIX, ODD VALUES, KeysTotal
==================================================================================== */

// rotateToImmutable moves the active memlog onto the immutable queue without
// flushing it, so a test can place data in the "immutable memtable" layer.
func rotateToImmutable(e *StorageEngine) {
	e.mu.Lock()
	e.immutables = append(e.immutables, e.active)
	e.active = newMemlog(e.nextFileNumber, e.crcTable)
	e.nextFileNumber++
	e.mu.Unlock()
}

// toL1 flushes the active memtable and compacts the resulting L0 SST into L1.
func toL1(t *testing.T, e *StorageEngine) {
	t.Helper()
	if err := e.flush(); err != nil {
		t.Fatal(err)
	}
	e.sstables.mu.RLock()
	src := e.sstables.levels[0].sstList[0]
	e.sstables.mu.RUnlock()
	if err := e.compact(src); err != nil {
		t.Fatal(err)
	}
}

func scanMap(t *testing.T, e *StorageEngine) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := e.Scan(func(k string, v []byte) error { out[k] = string(v); return nil }); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out
}

// wantValue checks Get and Scan agree on key k: want == "" with gone == true
// means the key must be absent.
func wantValue(t *testing.T, e *StorageEngine, k, want string, gone bool) {
	t.Helper()
	wantValueBug(t, e, k, want, gone, "")
}

// wantValueBug is wantValue with a "BUG B-n (...): " prefix on failure.
func wantValueBug(t *testing.T, e *StorageEngine, k, want string, gone bool, bug string) {
	t.Helper()
	v, err := e.Get(k)
	sv, inScan := scanMap(t, e)[k]
	if gone {
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("%sGet(%q) = (%q, %v), want ErrKeyNotFound", bug, k, v, err)
		}
		if inScan {
			t.Fatalf("%sScan returned deleted key %q = %q", bug, k, sv)
		}
		return
	}
	if err != nil || string(v) != want {
		t.Fatalf("%sGet(%q) = (%q, %v), want %q", bug, k, v, err, want)
	}
	if !inScan || sv != want {
		t.Fatalf("%sScan[%q] = (%q, present=%v), want %q", bug, k, sv, inScan, want)
	}
}

const bugB16 = "BUG B-16 (a tombstone in a memtable does not shadow older values in immutables/SSTs; Get falls through): "

func TestEngine_NewestWinsAcrossLayers(t *testing.T) {
	type layer func(t *testing.T, e *StorageEngine)
	put := func(v string) func(*testing.T, *StorageEngine) {
		return func(t *testing.T, e *StorageEngine) {
			if err := e.Put("k", []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
	}
	del := func(t *testing.T, e *StorageEngine) {
		if err := e.Delete("k"); err != nil {
			t.Fatal(err)
		}
	}
	inL1 := func(w layer) layer { return func(t *testing.T, e *StorageEngine) { w(t, e); toL1(t, e) } }
	inL0 := func(w layer) layer {
		return func(t *testing.T, e *StorageEngine) {
			w(t, e)
			if err := e.flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	inImm := func(w layer) layer { return func(t *testing.T, e *StorageEngine) { w(t, e); rotateToImmutable(e) } }

	cases := []struct {
		name   string
		layers []layer // oldest first
		want   string
		gone   bool
		bug    string
	}{
		{"L0 over L1", []layer{inL1(put("v1")), inL0(put("v2"))}, "v2", false, ""},
		{"newer L0 over older L0", []layer{inL0(put("v1")), inL0(put("v2"))}, "v2", false, ""},
		{"immutable over L0 and L1", []layer{inL1(put("v1")), inL0(put("v2")), inImm(put("v3"))}, "v3", false, ""},
		{"newer immutable over older immutable", []layer{inImm(put("v1")), inImm(put("v2"))}, "v2", false, ""},
		{"active over everything", []layer{inL1(put("v1")), inL0(put("v2")), inImm(put("v3")), put("v4")}, "v4", false, ""},
		{"tombstone in L0 hides L1", []layer{inL1(put("v1")), inL0(del)}, "", true, ""},
		{"tombstone in immutable hides SSTs", []layer{inL1(put("v1")), inL0(put("v2")), inImm(del)}, "", true, bugB16},
		{"tombstone in active hides all", []layer{inL1(put("v1")), inL0(put("v2")), inImm(put("v3")), del}, "", true, bugB16},
		{"put after tombstone in lower layers", []layer{inL1(put("v1")), inL0(del), put("v3")}, "v3", false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t)
			for _, l := range tc.layers {
				l(t, e)
			}
			wantValueBug(t, e, "k", tc.want, tc.gone, tc.bug)
		})
	}
}

func reopen(t *testing.T) *StorageEngine {
	t.Helper()
	e, err := OpenStorageEngine(defaultSstCapacity, defaultSstCapacity, 4096, 10)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return e
}

func TestEngine_RestartMatrix(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T, e *StorageEngine)
		want  map[string]string
		gone  []string
		bug   string
	}{
		{"WAL only", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("1"))
			e.Put("b", []byte("2"))
			e.Delete("b")
		}, map[string]string{"a": "1"}, []string{"b"}, ""},
		{"L0 only", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("1"))
			e.flush()
		}, map[string]string{"a": "1"}, nil, ""},
		{"L1 only", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("1"))
			toL1(t, e)
		}, map[string]string{"a": "1"}, nil, ""},
		{"L1 then L0 overwrite", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("old"))
			toL1(t, e)
			e.Put("a", []byte("new"))
			e.flush()
		}, map[string]string{"a": "new"}, nil, ""},
		{"L0 tombstone over L1", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("old"))
			e.Put("b", []byte("keep"))
			toL1(t, e)
			e.Delete("a")
			e.flush()
		}, map[string]string{"b": "keep"}, []string{"a"}, ""},
		{"WAL tombstone over L0", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("old"))
			e.flush()
			e.Delete("a")
		}, nil, []string{"a"}, bugB16},
		{"WAL overwrite over L0 and L1", func(t *testing.T, e *StorageEngine) {
			e.Put("a", []byte("v1"))
			toL1(t, e)
			e.Put("a", []byte("v2"))
			e.flush()
			e.Put("a", []byte("v3"))
		}, map[string]string{"a": "v3"}, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t)
			tc.build(t, e)
			e2 := reopen(t)
			for k, v := range tc.want {
				wantValue(t, e2, k, v, false)
			}
			for _, k := range tc.gone {
				wantValueBug(t, e2, k, "", true, tc.bug)
			}
			// Writes after restart must win over everything recovered.
			if err := e2.Put("a", []byte("post-restart")); err != nil {
				t.Fatal(err)
			}
			wantValue(t, e2, "a", "post-restart", false)
			if err := e2.flush(); err != nil {
				t.Fatal(err)
			}
			wantValue(t, e2, "a", "post-restart", false)
		})
	}
}

// waitQuiet polls until no flush/compaction is running and the run counters
// have been stable for several consecutive polls, or the deadline passes.
func waitQuiet(t *testing.T, e *StorageEngine, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	stable := 0
	var last [2]uint64
	for time.Now().Before(deadline) {
		e.mu.RLock()
		imm := len(e.immutables)
		e.mu.RUnlock()
		cur := [2]uint64{e.flushRuns.Load(), e.compactionRuns.Load()}
		if imm == 0 && !e.compacting.Load() && cur == last {
			stable++
			if stable >= 5 {
				return
			}
		} else {
			stable = 0
		}
		last = cur
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("engine did not go quiet within %v", d)
}

// After a restart L0 is sorted by start key, not by age, so the next
// flush-triggered compaction can push the NEWEST L0 SST down to L1 while an
// older SST with the same key stays in L0 and shadows it.
func TestEngine_RestartThenCompaction_NewestStillWins(t *testing.T) {
	setupTestDir(t)
	e1, err := OpenStorageEngine(defaultSstCapacity, defaultSstCapacity, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	e1.Put("b", []byte("old")) // older SST: range [b, c]
	e1.Put("c", []byte("x"))
	if err := e1.flush(); err != nil {
		t.Fatal(err)
	}
	e1.Put("a", []byte("y")) // newer SST: range [a, b], smaller start key
	e1.Put("b", []byte("new"))
	if err := e1.flush(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a tiny L0 capacity so the next flush triggers compaction.
	e2, err := OpenStorageEngine(defaultSstCapacity, defaultSstCapacity, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := e2.Get("b"); err != nil || string(v) != "new" {
		t.Fatalf("before compaction Get(b) = (%q, %v), want new", v, err)
	}
	e2.Put("zz", []byte("1"))
	if err := e2.flush(); err != nil {
		t.Fatal(err)
	}
	waitQuiet(t, e2, 5*time.Second)
	if e2.compactionRuns.Load() == 0 {
		t.Fatal("setup: expected a compaction to run")
	}
	if v, err := e2.Get("b"); err != nil || string(v) != "new" {
		t.Fatalf("BUG B-2 (after restart, compaction moves the newest L0 SST down, giving stale reads): "+
			"got Get(b) = (%q, %v), want new", v, err)
	}
}

func TestEngine_OddKeysAndValues(t *testing.T) {
	cases := map[string]struct {
		key string
		val []byte
		bug string
	}{
		"empty value":         {"ev", []byte{}, ""},
		"binary value":        {"bin", []byte{0, 0xff, 0, 1, '\n'}, ""},
		"value over a block":  {"big", bytes.Repeat([]byte("x"), 3*TARGET_BLOCK_SIZE+17), ""},
		"unicode key":         {"ключ-🔑", []byte("u"), ""},
		"key with separators": {"a/b:c\x00d", []byte("s"), ""},
		"empty key":           {"", []byte("empty-key"), "BUG B-17 (the empty key is accepted but lost once it is flushed to an SST): "},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			check := func(e *StorageEngine, stage string) {
				t.Helper()
				v, err := e.Get(tc.key)
				if err != nil || !bytes.Equal(v, tc.val) {
					t.Fatalf("%s%s: Get(%q) = (len %d, %v), want len %d", tc.bug, stage, tc.key, len(v), err, len(tc.val))
				}
			}
			e := newTestEngine(t)
			if err := e.Put(tc.key, tc.val); err != nil {
				t.Fatal(err)
			}
			check(e, "memtable")
			check(reopen(t), "after WAL replay")
			e.flush()
			check(e, "L0")
			check(reopen(t), "after SST reopen")
		})
	}
}

func TestEngine_KeysTotal_InsertAndDelete(t *testing.T) {
	e := newTestEngine(t)
	e.Put("a", []byte("1"))
	e.Put("b", []byte("2"))
	e.Delete("a")
	if got := e.Stats().KeysTotal; got != 1 {
		t.Fatalf("KeysTotal = %d, want 1", got)
	}
}

func TestEngine_KeysTotal_OverwriteCountsOnce(t *testing.T) {
	e := newTestEngine(t)
	for i := 0; i < 3; i++ {
		e.Put("a", []byte(fmt.Sprint(i)))
	}
	if got := e.Stats().KeysTotal; got != 1 {
		t.Fatalf("BUG B-7 (KeysTotal counts every Put, not live keys): got %d after writing one key 3 times, want 1", got)
	}
}

func TestEngine_KeysTotal_DeleteMissingNotNegative(t *testing.T) {
	e := newTestEngine(t)
	e.Delete("never-written")
	if got := e.Stats().KeysTotal; got != 0 {
		t.Fatalf("BUG B-7 (deleting a missing key decrements KeysTotal): got %d, want 0", got)
	}
}

func TestEngine_KeysTotal_SurvivesRestart(t *testing.T) {
	e := newTestEngine(t)
	e.Put("a", []byte("1"))
	e.Put("b", []byte("2"))
	e.flush()
	e.Put("c", []byte("3"))
	if got := reopen(t).Stats().KeysTotal; got != 3 {
		t.Fatalf("BUG B-15 (KeysTotal is not rebuilt on restart): got %d after reopening an engine with 3 live keys, want 3", got)
	}
}

/* ====================================================================================
	U7: ENGINE RACES (B-10). Run with -race -count=20.
==================================================================================== */

// flush of an empty active memtable must be a harmless no-op. A duplicate
// background flush (B-10a) reaches exactly this state.
func TestEngine_FlushEmptyMemtable_DoesNotPanic(t *testing.T) {
	e := newTestEngine(t)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BUG B-14 (flushing an empty memtable panics with a nil dereference in newSstFromMemtable): "+
				"got panic %v, want no-op", r)
		}
	}()
	_ = e.flush()
	e.sstables.mu.RLock()
	n := len(e.sstables.levels[0].sstList)
	e.sstables.mu.RUnlock()
	if n != 0 {
		t.Fatalf("flush of empty memtable created %d SSTs, want 0", n)
	}
}

const childEnv = "KVSTORE_ENGINE_STRESS_CHILD"

// runInChild re-runs the named test in a subprocess so a panic in a
// background goroutine fails only this test instead of the whole binary.
func runInChild(t *testing.T, name string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func firstLineWith(out, sub string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, sub) {
			return strings.TrimSpace(l)
		}
	}
	return ""
}

// Concurrent Puts that cross memCapacity must trigger one flush per full
// memtable: every flushed SST holds at least memCapacity bytes and nothing
// crashes.
func TestEngine_AutoFlush_NoDuplicateFlushes_Stress(t *testing.T) {
	if os.Getenv(childEnv) == "" {
		out, err := runInChild(t, t.Name())
		if err == nil {
			return
		}
		if p := firstLineWith(out, "panic:"); p != "" {
			t.Fatalf("BUG B-10a (a Put over capacity spawns a flush every time, so duplicate flushes run): "+
				"a duplicate flush hit an empty memtable and crashed the process (see B-14): %s", p)
		}
		if b := firstLineWith(out, "BUG B-10a"); b != "" {
			t.Fatal(b)
		}
		t.Fatalf("stress child failed for a reason other than B-10a:\n%s", out)
	}

	const memCap = 200
	setupTestDir(t)
	e := newStorageEngine(memCap, defaultSstCapacity, 1<<40, 10) // compaction effectively off
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := e.Put(fmt.Sprintf("g%d-%03d", g, i), bytes.Repeat([]byte("v"), 20)); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	waitQuiet(t, e, 5*time.Second)

	e.sstables.mu.RLock()
	var small []uint64
	for _, s := range e.sstables.levels[0].sstList {
		if s.sizeBytes < memCap {
			small = append(small, s.sizeBytes)
		}
	}
	total := len(e.sstables.levels[0].sstList)
	e.sstables.mu.RUnlock()
	if len(small) > 0 {
		t.Fatalf("BUG B-10a (a Put over capacity spawns a flush every time, so duplicate flushes run): "+
			"%d of %d L0 SSTs were flushed below memCapacity=%d (sizes %v)", len(small), total, memCap, small)
	}
	for g := 0; g < 4; g++ {
		for i := 0; i < 50; i++ {
			if _, err := e.Get(fmt.Sprintf("g%d-%03d", g, i)); err != nil {
				t.Fatalf("Get(g%d-%03d): %v", g, i, err)
			}
		}
	}
}

// A compaction that is triggered while another compaction runs must not be
// lost: once the in-flight one finishes, L0 must come back under capacity.
// The trigger is delivered synchronously (as flush would, via go e.compact)
// while the compacting flag is held, so the test is deterministic.
func TestEngine_CompactionTriggeredDuringCompaction_NotDropped(t *testing.T) {
	e := newTestEngine(t)
	e.Put("x", []byte("1"))
	if err := e.flush(); err != nil { // L0 cap 4096: no trigger yet
		t.Fatal(err)
	}
	e.sstables.mu.Lock()
	e.sstables.levels[0].capacityBytes = 1 // now L0 is over capacity
	src := e.sstables.levels[0].sstList[0]
	e.sstables.mu.Unlock()

	e.compacting.Store(true)               // a compaction is in flight
	if err := e.compact(src); err != nil { // the trigger arrives
		t.Fatal(err)
	}
	e.compacting.Store(false) // the in-flight compaction finishes

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e.sstables.mu.RLock()
		over := e.sstables.levels[0].sizeBytes >= e.sstables.levels[0].capacityBytes
		e.sstables.mu.RUnlock()
		if !over {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("BUG B-10b (a compaction triggered while another runs is dropped): " +
		"L0 still over capacity 2s after the in-flight compaction ended, want it compacted")
}

// Sequential flushes with a small L0 overlap their background compactions.
// Once the engine is quiet, no level may be left over capacity.
func TestEngine_Compaction_LevelsUnderCapacityWhenQuiet_Stress(t *testing.T) {
	e := newTestEngine(t)
	e.sstables.mu.Lock()
	e.sstables.levels[0].capacityBytes = 150
	e.sstables.mu.Unlock()
	for i := 0; i < 40; i++ {
		for j := 0; j < 3; j++ {
			e.Put(fmt.Sprintf("k%03d", (i*3+j)%60), []byte(fmt.Sprintf("v%d", i)))
		}
		if err := e.flush(); err != nil {
			t.Fatal(err)
		}
	}
	waitQuiet(t, e, 5*time.Second)
	e.sstables.mu.RLock()
	defer e.sstables.mu.RUnlock()
	for i, l := range e.sstables.levels {
		if l.sizeBytes >= l.capacityBytes {
			t.Fatalf("BUG B-10b (compactions are dropped, so levels stay over capacity): "+
				"level %d has %d bytes in %d SSTs at quiescence, capacity %d", i, l.sizeBytes, len(l.sstList), l.capacityBytes)
		}
	}
}

// churn writes and flushes in a loop with a small L0 so compactions run
// constantly, until stop is closed. 60 keys are seeded first.
func churn(t *testing.T, e *StorageEngine, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		for j := 0; j < 3; j++ {
			if err := e.Put(fmt.Sprintf("k%03d", (i*3+j)%60), []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Error(err)
				return
			}
		}
		if err := e.flush(); err != nil {
			t.Error(err)
			return
		}
	}
}

func seedForChurn(t *testing.T) *StorageEngine {
	t.Helper()
	e := newTestEngine(t)
	e.sstables.mu.Lock()
	e.sstables.levels[0].capacityBytes = 150
	e.sstables.mu.Unlock()
	for i := 0; i < 60; i++ {
		e.Put(fmt.Sprintf("k%03d", i), []byte("seed"))
	}
	if err := e.flush(); err != nil {
		t.Fatal(err)
	}
	return e
}

// Scan must never fail or miss live keys while compactions delete old SSTs.
func TestEngine_ScanDuringCompaction_Stress(t *testing.T) {
	e := seedForChurn(t)
	stop, done := make(chan struct{}), make(chan struct{})
	go churn(t, e, stop, done)

	var scanErrs, missing int
	var firstErr error
	for n := 0; n < 150; n++ {
		count := 0
		err := e.Scan(func(string, []byte) error { count++; return nil })
		if err != nil {
			scanErrs++
			if firstErr == nil {
				firstErr = err
			}
		} else if count != 60 {
			missing++
		}
	}
	close(stop)
	<-done
	waitQuiet(t, e, 5*time.Second)
	if scanErrs > 0 {
		t.Fatalf("BUG B-10c (Scan opens SST files a concurrent compaction just deleted): "+
			"%d of 150 scans failed, first: %v; want 0", scanErrs, firstErr)
	}
	if missing > 0 {
		t.Fatalf("%d of 150 scans returned a key count other than 60", missing)
	}
}

// Get must never return an I/O error for a live key while compactions run.
func TestEngine_GetDuringCompaction_Stress(t *testing.T) {
	e := seedForChurn(t)
	stop, done := make(chan struct{}), make(chan struct{})
	go churn(t, e, stop, done)

	var bad int
	var firstErr error
	for n := 0; n < 3000; n++ {
		if _, err := e.Get(fmt.Sprintf("k%03d", n%60)); err != nil {
			bad++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	close(stop)
	<-done
	waitQuiet(t, e, 5*time.Second)
	if bad > 0 {
		t.Fatalf("BUG B-10c (reads open SST files a concurrent compaction just deleted): "+
			"%d of 3000 Gets of live keys failed, first: %v; want 0", bad, firstErr)
	}
}
