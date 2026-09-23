package storageengine

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"testing"
)

func TestNewWal(t *testing.T) {
	logNumber := uint64(5)
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	w := newWal(logNumber, crcTable)

	if w.logNumber != 5 {
		t.Errorf("logNumber = %d, want 5", w.logNumber)
	}
	if w.lastSeq != 0 {
		t.Errorf("lastSeq = %d, want 0 (initial value)", w.lastSeq)
	}
	if w.crcTable == nil {
		t.Error("crcTable is nil")
	}
}

func TestWriteLog_BasicWrite(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	w := newWal(1, crcTable)
	key := "hello"
	val := []byte("world")
	tombstone := false
	seq := uint64(1)
	err := w.writeLog(key, val, tombstone, seq)
	if err != nil {
		t.Fatalf("writeLog: %v", err)
	}

	info, err := os.Stat("data/wal/1.log")
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("log file is empty after writeLog")
	}
}

func TestWriteLog_VerifyFormat(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	w := newWal(1, crcTable)

	key := "key1"
	val := []byte("val1")
	tombstone := false
	seqArg := uint64(42)
	if err := w.writeLog(key, val, tombstone, seqArg); err != nil {
		t.Fatalf("writeLog: %v", err)
	}

	data, err := os.ReadFile("data/wal/1.log")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	r := bytes.NewReader(data)

	// seq (8 bytes)
	var seq uint64
	binary.Read(r, binary.LittleEndian, &seq)
	if seq != 42 {
		t.Errorf("seq = %d, want 42", seq)
	}

	// tombstone (1 byte)
	var tomb byte
	binary.Read(r, binary.LittleEndian, &tomb)
	if tomb != 0 {
		t.Errorf("tombstone = %d, want 0", tomb)
	}

	// key length + key
	var keyLen uint32
	binary.Read(r, binary.LittleEndian, &keyLen)
	if keyLen != 4 {
		t.Errorf("keyLen = %d, want 4", keyLen)
	}
	keyBuf := make([]byte, keyLen)
	io.ReadFull(r, keyBuf)
	if string(keyBuf) != "key1" {
		t.Errorf("key = %q, want %q", keyBuf, "key1")
	}

	// value length + value
	var valLen uint32
	binary.Read(r, binary.LittleEndian, &valLen)
	if valLen != 4 {
		t.Errorf("valLen = %d, want 4", valLen)
	}
	valBuf := make([]byte, valLen)
	io.ReadFull(r, valBuf)
	if string(valBuf) != "val1" {
		t.Errorf("value = %q, want %q", valBuf, "val1")
	}

	// checksum (4 bytes) — verify against the payload preceding it
	var checksum uint32
	binary.Read(r, binary.LittleEndian, &checksum)

	checksumPayload := data[:len(data)-4]
	expected := crc32.Checksum(checksumPayload, w.crcTable)
	if checksum != expected {
		t.Errorf("checksum = %08x, want %08x", checksum, expected)
	}
}

func TestWriteLog_Tombstone(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	w := newWal(1, crcTable)

	key := "delkey"
	val := []byte{}
	tombstone := true
	seqArg := uint64(7)
	if err := w.writeLog(key, val, tombstone, seqArg); err != nil {
		t.Fatalf("writeLog: %v", err)
	}

	data, err := os.ReadFile("data/wal/1.log")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	r := bytes.NewReader(data)

	var seq uint64
	binary.Read(r, binary.LittleEndian, &seq)
	if seq != 7 {
		t.Errorf("seq = %d, want 7", seq)
	}

	var tomb byte
	binary.Read(r, binary.LittleEndian, &tomb)
	if tomb != 1 {
		t.Errorf("tombstone = %d, want 1", tomb)
	}
}

func TestWriteLog_MultipleEntries(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	w := newWal(1, crcTable)

	entries := []struct {
		key   string
		value []byte
		tomb  bool
		seq   uint64
	}{
		{"k1", []byte("v1"), false, 1},
		{"k2", []byte("v2"), false, 2},
		{"k3", []byte{}, true, 3},
	}

	for _, e := range entries {
		key := e.key
		val := e.value
		tomb := e.tomb
		seq := e.seq
		if err := w.writeLog(key, val, tomb, seq); err != nil {
			t.Fatalf("writeLog(%q): %v", e.key, err)
		}
	}

	data, err := os.ReadFile("data/wal/1.log")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	r := bytes.NewReader(data)
	for _, e := range entries {
		var seq uint64
		if err := binary.Read(r, binary.LittleEndian, &seq); err != nil {
			t.Fatalf("reading seq for %q: %v", e.key, err)
		}
		if seq != e.seq {
			t.Errorf("entry %q: seq = %d, want %d", e.key, seq, e.seq)
		}

		var tomb byte
		binary.Read(r, binary.LittleEndian, &tomb)

		var keyLen uint32
		binary.Read(r, binary.LittleEndian, &keyLen)
		keyBuf := make([]byte, keyLen)
		io.ReadFull(r, keyBuf)
		if string(keyBuf) != e.key {
			t.Errorf("entry: key = %q, want %q", keyBuf, e.key)
		}

		var valLen uint32
		binary.Read(r, binary.LittleEndian, &valLen)
		valBuf := make([]byte, valLen)
		if valLen > 0 {
			io.ReadFull(r, valBuf)
		}

		// skip checksum
		var checksum uint32
		binary.Read(r, binary.LittleEndian, &checksum)
	}

	// Should be exactly at EOF
	var extra byte
	if err := binary.Read(r, binary.LittleEndian, &extra); err != io.EOF {
		t.Error("expected EOF after reading all entries")
	}
}

func TestWriteLog_UpdatesLastSeq(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	w := newWal(1, crcTable)

	key1 := "k"
	val1 := []byte("v")
	tombstone1 := false
	seq1 := uint64(10)
	if err := w.writeLog(key1, val1, tombstone1, seq1); err != nil {
		t.Fatalf("writeLog: %v", err)
	}
	if w.lastSeq != 10 {
		t.Errorf("lastSeq = %d, want 10 after first write", w.lastSeq)
	}

	key2 := "k2"
	val2 := []byte("v2")
	tombstone2 := false
	seq2 := uint64(20)
	if err := w.writeLog(key2, val2, tombstone2, seq2); err != nil {
		t.Fatalf("writeLog: %v", err)
	}
	if w.lastSeq != 20 {
		t.Errorf("lastSeq = %d, want 20 after second write", w.lastSeq)
	}
}

/* ====================================================================================
	REPLAY WAL TESTS
==================================================================================== */

// writeWalEntries is a test helper that writes a sequence of entries to a WAL
// file at data/wal/<logNumber>.log using writeLog. The caller must have already
// created the data/wal/ directory via setupTestDir + os.MkdirAll.
func writeWalEntries(t *testing.T, logNumber uint64, entries []struct {
	key       string
	value     []byte
	tombstone bool
	seq       uint64
}, crcTable *crc32.Table) {
	t.Helper()
	w := newWal(logNumber, crcTable)
	for _, e := range entries {
		if err := w.writeLog(e.key, e.value, e.tombstone, e.seq); err != nil {
			t.Fatalf("writeLog(%q): %v", e.key, err)
		}
	}
}

// TestReplayWal_LogNumNotFound verifies that a logNumber with no corresponding
// file on disk returns a non-nil OS error.
func TestReplayWal_LogNumNotFound(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	ml, err := replayWal(999, 0, crcTab)
	if err == nil {
		t.Fatal("expected error for non-existent logNumber, got nil")
	}
	if ml != nil {
		t.Error("expected nil memlog on error")
	}
}

// TestReplayWal_FileNotFound verifies that a logNumber for a file that
// does not exist returns a non-nil OS error.
func TestReplayWal_FileNotFound(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	ml, err := replayWal(99, 0, crcTab)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if ml != nil {
		t.Error("expected nil memlog on error")
	}
}

// TestReplayWal_EmptyWal verifies that a zero-byte WAL file (engine created
// the file but crashed before any write) produces an empty memlog with no error.
func TestReplayWal_EmptyWal(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	// Create an empty file
	f, err := os.Create("data/wal/1.log")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	ml, err := replayWal(1, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}
	if ml.wal.logNumber != 1 {
		t.Errorf("logNumber = %d, want 1", ml.wal.logNumber)
	}
	if ml.wal.lastSeq != 0 {
		t.Errorf("lastSeq = %d, want 0", ml.wal.lastSeq)
	}
	if _, err := ml.memtable.get("any"); err != ErrKeyNotFound {
		t.Errorf("expected empty memtable, got err=%v", err)
	}
}

// TestReplayWal_SingleEntry_Replayed verifies that a single entry with
// seq > maxSeqFromSsts is replayed into the memtable correctly.
func TestReplayWal_SingleEntry_Replayed(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"cat", []byte("meow"), false, 5},
	}, crcTab)

	ml, err := replayWal(1, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	if ml.wal.logNumber != 1 {
		t.Errorf("logNumber = %d, want 1", ml.wal.logNumber)
	}
	if ml.wal.lastSeq != 5 {
		t.Errorf("lastSeq = %d, want 5", ml.wal.lastSeq)
	}
	val, err := ml.memtable.get("cat")
	if err != nil {
		t.Fatalf("get(\"cat\"): %v", err)
	}
	if string(val) != "meow" {
		t.Errorf("get(\"cat\") = %q, want \"meow\"", val)
	}
}

// TestReplayWal_TombstoneEntry verifies that a tombstone entry is replayed as
// a deletion: the key is present in the skip-list as a tombstone, so
// memtable.get() returns ErrKeyNotFound.
func TestReplayWal_TombstoneEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"dead", []byte{}, true, 3},
	}, crcTab)

	ml, err := replayWal(1, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	if ml.wal.lastSeq != 3 {
		t.Errorf("lastSeq = %d, want 3", ml.wal.lastSeq)
	}
	if _, err := ml.memtable.get("dead"); err != ErrKeyNotFound {
		t.Errorf("get(\"dead\") = %v, want ErrKeyNotFound (tombstone)", err)
	}
}

// TestReplayWal_AllEntriesFiltered verifies that when all WAL entries have
// seq <= maxSeqFromSsts, the returned memlog is empty.
func TestReplayWal_AllEntriesFiltered(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"k1", []byte("v1"), false, 1},
		{"k2", []byte("v2"), false, 2},
		{"k3", []byte("v3"), false, 3},
	}, crcTab)

	// maxSeqFromSsts is higher than all entries
	ml, err := replayWal(1, 5, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	if ml.wal.lastSeq != 0 {
		t.Errorf("lastSeq = %d, want 0 (nothing replayed)", ml.wal.lastSeq)
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, err := ml.memtable.get(key); err != ErrKeyNotFound {
			t.Errorf("get(%q) = %v, want ErrKeyNotFound (all filtered)", key, err)
		}
	}
}

// TestReplayWal_PartialFilter verifies that only entries with seq > maxSeqFromSsts
// are replayed; entries at or below the threshold are skipped.
func TestReplayWal_PartialFilter(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"k1", []byte("v1"), false, 1},  // seq 1 <= 5: filtered
		{"k2", []byte("v2"), false, 5},  // seq 5 <= 5: filtered
		{"k3", []byte("v3"), false, 10}, // seq 10 > 5: replayed
	}, crcTab)

	ml, err := replayWal(1, 5, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	if _, err := ml.memtable.get("k1"); err != ErrKeyNotFound {
		t.Errorf("get(\"k1\") = %v, want ErrKeyNotFound (filtered, seq=1)", err)
	}
	if _, err := ml.memtable.get("k2"); err != ErrKeyNotFound {
		t.Errorf("get(\"k2\") = %v, want ErrKeyNotFound (filtered, seq=5)", err)
	}
	val, err := ml.memtable.get("k3")
	if err != nil {
		t.Fatalf("get(\"k3\"): %v", err)
	}
	if string(val) != "v3" {
		t.Errorf("get(\"k3\") = %q, want \"v3\"", val)
	}
	if ml.wal.lastSeq != 10 {
		t.Errorf("lastSeq = %d, want 10", ml.wal.lastSeq)
	}
}

// TestReplayWal_LogNumberPreserved verifies that the logNumber parsed from the
// filename is reflected in the returned memlog, regardless of file contents.
func TestReplayWal_LogNumberPreserved(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 42, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"k", []byte("v"), false, 1},
	}, crcTab)

	ml, err := replayWal(42, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}
	if ml.wal.logNumber != 42 {
		t.Errorf("logNumber = %d, want 42", ml.wal.logNumber)
	}
}

// TestReplayWal_CorruptTailEntry verifies that a truncated/corrupt entry at the
// end of the WAL (simulating a crash mid-write) is silently ignored, while any
// complete entries before it are still replayed.
func TestReplayWal_CorruptTailEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	// Write one complete, valid entry
	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"good", []byte("data"), false, 1},
	}, crcTab)

	// Append garbage bytes to simulate a partial write
	f, err := os.OpenFile("data/wal/1.log", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ml, err := replayWal(1, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	val, err := ml.memtable.get("good")
	if err != nil {
		t.Fatalf("get(\"good\"): %v (complete entry before corrupt tail should be replayed)", err)
	}
	if string(val) != "data" {
		t.Errorf("get(\"good\") = %q, want \"data\"", val)
	}
	if ml.wal.lastSeq != 1 {
		t.Errorf("lastSeq = %d, want 1", ml.wal.lastSeq)
	}
}

// TestReplayWal_RoundTrip writes a realistic sequence of entries via writeLog,
// then replays them with replayWal and verifies every key has the correct value.
func TestReplayWal_RoundTrip(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)
	if err := os.MkdirAll("data/wal", 0755); err != nil {
		t.Fatal(err)
	}

	writeWalEntries(t, 1, []struct {
		key       string
		value     []byte
		tombstone bool
		seq       uint64
	}{
		{"a", []byte("va"), false, 1},
		{"b", []byte("vb"), false, 2},
		{"c", []byte{}, true, 3},  // tombstone
		{"d", []byte("vd"), false, 4},
	}, crcTab)

	ml, err := replayWal(1, 0, crcTab)
	if err != nil {
		t.Fatalf("replayWal: %v", err)
	}

	if ml.wal.lastSeq != 4 {
		t.Errorf("lastSeq = %d, want 4", ml.wal.lastSeq)
	}

	cases := []struct {
		key     string
		want    string
		deleted bool
	}{
		{"a", "va", false},
		{"b", "vb", false},
		{"c", "", true},  // tombstoned
		{"d", "vd", false},
	}
	for _, tc := range cases {
		val, err := ml.memtable.get(tc.key)
		if tc.deleted {
			if err != ErrKeyNotFound {
				t.Errorf("get(%q) = %v, want ErrKeyNotFound (tombstone)", tc.key, err)
			}
		} else {
			if err != nil {
				t.Errorf("get(%q): %v", tc.key, err)
			} else if string(val) != tc.want {
				t.Errorf("get(%q) = %q, want %q", tc.key, val, tc.want)
			}
		}
	}
}

// containsStr is a local helper to avoid importing strings in wal_test.go.
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
