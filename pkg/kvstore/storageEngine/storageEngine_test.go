package storageengine

import (
	"fmt"
	"hash/crc32"
	"os"
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
