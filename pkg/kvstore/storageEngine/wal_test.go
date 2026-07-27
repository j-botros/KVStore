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
	w := newWal(5, 10, 2048)

	if w.logNumber != 5 {
		t.Errorf("logNumber = %d, want 5", w.logNumber)
	}
	if w.lastSeq != 10 {
		t.Errorf("lastSeq = %d, want 10", w.lastSeq)
	}
	if w.capacityBytes != 2048 {
		t.Errorf("capacityBytes = %d, want 2048", w.capacityBytes)
	}
	if w.crcTable == nil {
		t.Error("crcTable is nil")
	}
}

func TestWriteLog_BasicWrite(t *testing.T) {
	setupTestDir(t)
	os.MkdirAll("data/wal", 0755)

	w := newWal(1, 0, 4096)
	err := w.writeLog("hello", []byte("world"), false, 1)
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

	w := newWal(1, 0, 4096)
	if err := w.writeLog("key1", []byte("val1"), false, 42); err != nil {
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

	w := newWal(1, 0, 4096)
	if err := w.writeLog("delkey", []byte{}, true, 7); err != nil {
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

	w := newWal(1, 0, 4096)

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
		if err := w.writeLog(e.key, e.value, e.tomb, e.seq); err != nil {
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

	w := newWal(1, 0, 4096)

	if err := w.writeLog("k", []byte("v"), false, 10); err != nil {
		t.Fatalf("writeLog: %v", err)
	}
	if w.lastSeq != 10 {
		t.Errorf("lastSeq = %d, want 10 after first write", w.lastSeq)
	}

	if err := w.writeLog("k2", []byte("v2"), false, 20); err != nil {
		t.Fatalf("writeLog: %v", err)
	}
	if w.lastSeq != 20 {
		t.Errorf("lastSeq = %d, want 20 after second write", w.lastSeq)
	}
}
