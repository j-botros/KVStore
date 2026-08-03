package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
)

// setupTestDir creates a temp directory and changes the working directory to it.
// The original working directory is restored via t.Cleanup.
// This is required because WAL and SSTable code use relative paths
// like "data/wal/" and "data/sstables/".
func setupTestDir(t *testing.T) {
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
}

// testEntry represents a single key-value entry for writing synthetic SST files.
type testEntry struct {
	key       string
	value     []byte
	seq       uint64
	tombstone bool
}

// writeSyntheticSST writes a minimal valid SST file at
// data/sstables/level-{level}/{filenum}.sst inside the current working directory.
// All entries are placed in a single data block.
// Entries MUST be provided in ascending key order.
// Returns an *sst struct with the index, bloom filter, and CRC table populated.
func writeSyntheticSST(t *testing.T, level int, filenum uint64, entries []testEntry, crcTab *crc32.Table) *sst {
	t.Helper()

	dir := fmt.Sprintf("data/sstables/level-%d", level)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	filename := fmt.Sprintf("%s/%d.sst", dir, filenum)

	// ── Data block ──────────────────────────────────────────────
	dataBuf := new(bytes.Buffer)
	for _, e := range entries {
		entryBuf := new(bytes.Buffer)
		binary.Write(entryBuf, binary.LittleEndian, e.seq)
		if e.tombstone {
			entryBuf.WriteByte(1)
		} else {
			entryBuf.WriteByte(0)
		}
		binary.Write(entryBuf, binary.LittleEndian, uint32(len(e.key)))
		entryBuf.Write([]byte(e.key))
		binary.Write(entryBuf, binary.LittleEndian, uint32(len(e.value)))
		entryBuf.Write(e.value)
		checksum := crc32.Checksum(entryBuf.Bytes(), crcTab)
		dataBuf.Write(entryBuf.Bytes())
		binary.Write(dataBuf, binary.LittleEndian, checksum)
	}

	dataBlockOffset := uint64(0)
	dataBlockLength := uint64(dataBuf.Len())

	// ── Index (single block spanning all entries) ───────────────
	indexOffset := dataBlockLength
	indexBuf := new(bytes.Buffer)
	lastKey := entries[len(entries)-1].key
	binary.Write(indexBuf, binary.LittleEndian, uint32(len(lastKey)))
	indexBuf.Write([]byte(lastKey))
	binary.Write(indexBuf, binary.LittleEndian, dataBlockOffset)
	binary.Write(indexBuf, binary.LittleEndian, dataBlockLength)
	indexLength := uint64(indexBuf.Len())

	// ── Bloom filter (pass-through: numHashes defaults to 0) ───
	bloomOffset := indexOffset + indexLength
	bf := newBloomFilter(uint64(len(entries)))
	bloomBuf := bf.bitstring
	bloomLength := uint64(len(bloomBuf))

	// ── Footer ──────────────────────────────────────────────────
	footerBuf := new(bytes.Buffer)
	binary.Write(footerBuf, binary.LittleEndian, indexOffset)
	binary.Write(footerBuf, binary.LittleEndian, indexLength)
	binary.Write(footerBuf, binary.LittleEndian, bloomOffset)
	binary.Write(footerBuf, binary.LittleEndian, bloomLength)
	binary.Write(footerBuf, binary.LittleEndian, FOOTER_MAGIC)

	// ── Assemble and write file ─────────────────────────────────
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := f.Write(dataBuf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(indexBuf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bloomBuf); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(footerBuf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// ── Return in-memory sst struct ─────────────────────────────
	idx := &index{
		newBlock(lastKey, dataBlockOffset, dataBlockLength, ""),
	}

	return &sst{
		filenum:     filenum,
		level:       level,
		lastSeq:     entries[len(entries)-1].seq,
		startKey:    entries[0].key,
		endKey:      entries[len(entries)-1].key,
		index:       idx,
		bloomFilter: bf,
		crcTable:    crcTab,
	}
}

// newTestMemtable builds a *memtable from the provided entries.
// Entries MUST be provided in ascending key order.
func newTestMemtable(t *testing.T, entries []testEntry) *memtable {
	t.Helper()
	m := newMemtable()
	for _, e := range entries {
		if e.tombstone {
			m.delete(e.key, e.seq)
		} else {
			m.insert(e.key, e.value, e.seq)
		}
	}
	return m
}
