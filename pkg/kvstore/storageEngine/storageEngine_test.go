package storageengine

import (
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
			levels:   1,
			sstList:  make([][]*sst, 1),
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

