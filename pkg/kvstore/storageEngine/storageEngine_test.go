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

	crcTab := crc32.MakeTable(crc32.Castagnoli)
	return &StorageEngine{
		memtable: *newMemtable(4096),
		metadata: metadata{
			nextFileNumber: 1,
			wal:            newWal(1, 0, 4096),
			sstables: &sstables{
				levels:   1,
				sstList:  make([][]*sst, 1),
				crcTable: crcTab,
			},
			crcTable: crcTab,
		},
		nextSeq: 1,
	}
}

func TestStorageEngine_PutAndGet(t *testing.T) {
	e := newTestEngine(t)

	if err := e.Put("name", []byte("alice")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := e.Get("name")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "alice" {
		t.Errorf("Get(\"name\") = %q, want %q", val, "alice")
	}
}

func TestStorageEngine_Get_NotFound(t *testing.T) {
	e := newTestEngine(t)

	_, err := e.Get("nope")
	if err != ErrKeyNotFound {
		t.Errorf("Get(\"nope\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestStorageEngine_Delete(t *testing.T) {
	e := newTestEngine(t)

	if err := e.Put("key", []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Delete("key"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := e.Get("key")
	if err != ErrKeyNotFound {
		t.Errorf("Get after Delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestStorageEngine_PutOverwrite(t *testing.T) {
	e := newTestEngine(t)

	e.Put("key", []byte("first"))
	e.Put("key", []byte("second"))

	val, err := e.Get("key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "second" {
		t.Errorf("Get(\"key\") = %q, want %q", val, "second")
	}
}

func TestStorageEngine_DeleteThenPut(t *testing.T) {
	e := newTestEngine(t)

	e.Put("key", []byte("original"))
	e.Delete("key")
	e.Put("key", []byte("reborn"))

	val, err := e.Get("key")
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

	e.Put("a", []byte("1"))
	if e.nextSeq != initial+1 {
		t.Errorf("after Put: nextSeq = %d, want %d", e.nextSeq, initial+1)
	}

	e.Delete("a")
	if e.nextSeq != initial+2 {
		t.Errorf("after Delete: nextSeq = %d, want %d", e.nextSeq, initial+2)
	}

	e.Put("b", []byte("2"))
	if e.nextSeq != initial+3 {
		t.Errorf("after second Put: nextSeq = %d, want %d", e.nextSeq, initial+3)
	}
}
