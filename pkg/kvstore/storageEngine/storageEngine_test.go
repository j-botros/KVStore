package storageengine

import (
	"fmt"
	"hash/crc32"
	"os"
	"testing"
)

// newTestEngine builds a fully functional StorageEngine backed by a temp directory.
// The WAL writes to a real file; the sstables layer is empty (no SST files).
func newTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	setupTestDir(t)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	crcTable := crc32.MakeTable(crc32.Castagnoli)

	return &StorageEngine{
		memCapacity:    4096,
		crcTable:       crcTable,
		nextFileNumber: 1,
		nextSeq:        1,
		active:         newMemlog(1, crcTable),
		immutables:     make([]*memlog, 0),
		sstables: &sstables{
			levels:   []*level{newLevel(0)}, // level 0, no capacity limit yet
			crcTable: crcTable,
		},
	}
}

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

	if err := e.Flush(); err != nil {
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
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	l0 := e.sstables.levels[0]
	if len(l0.sstList) != 1 {
		t.Fatalf("expected 1 SST in L0, got %d", len(l0.sstList))
	}
	srcSst := l0.sstList[0]

	if err := e.Compact(srcSst); err != nil {
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
	// fake size
	overlapSst.sizeBytes = 500
	l1.sizeBytes += overlapSst.sizeBytes

	// Flush an SST to L0 that overlaps and updates 'c'
	e.Put("b", []byte("new-b"))
	e.Put("c", []byte("new-c"))
	e.Put("d", []byte("new-d"))
	if err := e.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	l0 := e.sstables.levels[0]
	srcSst := l0.sstList[0]

	if err := e.Compact(srcSst); err != nil {
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

	// Ensure old size was subtracted out
	if l1.sizeBytes == 500 {
		t.Errorf("L1 sizeBytes was not properly updated, still %d", l1.sizeBytes)
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
	cw, _ := newCompactionWriter(newSst(999, 1, e.crcTable))
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
	e.Flush()

	srcSst := e.sstables.levels[0].sstList[0]
	if err := e.Compact(srcSst); err != nil {
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
