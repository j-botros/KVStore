package storageengine

import (
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"testing"
)

/* ====================================================================================
	NEW SST TESTS
==================================================================================== */

// TestNewSst_Metadata checks that the returned *sst has the correct scalar fields.
func TestNewSst_Metadata(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 3},
		{key: "cherry", value: []byte("dark-red"), seq: 2},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	if s.filenum != 1 {
		t.Errorf("filenum = %d, want 1", s.filenum)
	}
	if s.level != 0 {
		t.Errorf("level = %d, want 0", s.level)
	}
	if s.startKey != "apple" {
		t.Errorf("startKey = %q, want %q", s.startKey, "apple")
	}
	if s.endKey != "cherry" {
		t.Errorf("endKey = %q, want %q", s.endKey, "cherry")
	}
	if s.lastSeq != 3 {
		t.Errorf("lastSeq = %d, want 3 (max seq across all entries)", s.lastSeq)
	}
}

// TestNewSst_IndexBuilt checks that the index is populated.
func TestNewSst_IndexBuilt(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte("a"), seq: 1},
		{key: "beta", value: []byte("b"), seq: 2},
		{key: "gamma", value: []byte("c"), seq: 3},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	if s.index == nil {
		t.Fatal("index is nil")
	}
	if len(*s.index) == 0 {
		t.Fatal("index has no blocks")
	}

	// Last block must have the last key as its lastKey.
	last := (*s.index)[len(*s.index)-1]
	if last.lastKey != "gamma" {
		t.Errorf("last block lastKey = %q, want %q", last.lastKey, "gamma")
	}
}

// TestNewSst_BloomFilter checks that the bloom filter is populated for inserted keys.
func TestNewSst_BloomFilter(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "dog", value: []byte("woof"), seq: 1},
		{key: "cat", value: []byte("meow"), seq: 2},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	if s.bloomFilter == nil {
		t.Fatal("bloomFilter is nil")
	}
	// Keys that were inserted must not be reported as absent.
	if s.bloomFilter.keyNotPresent("dog") {
		t.Error("bloom filter reports \"dog\" not present, want present")
	}
	if s.bloomFilter.keyNotPresent("cat") {
		t.Error("bloom filter reports \"cat\" not present, want present")
	}
}

// TestNewSst_FileCreated checks that the SST file actually exists on disk after newSst.
func TestNewSst_FileCreated(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "key", value: []byte("val"), seq: 1},
	}
	m := newTestMemtable(t, entries)

	if _, err := newSstFromMemtable(7, m, crcTab); err != nil {
		t.Fatalf("newSst: %v", err)
	}

	if _, err := os.Stat("data/sstables/level-0/7.sst"); os.IsNotExist(err) {
		t.Error("expected SST file to exist on disk, but it does not")
	}
}

// TestNewSst_RoundTrip writes an SST via newSst then reads back every key via search.
func TestNewSst_RoundTrip(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 2},
		{key: "cherry", value: []byte("dark-red"), seq: 3},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	for _, e := range entries {
		result, err := s.search(e.key)
		if err != nil {
			t.Errorf("search(%q): %v", e.key, err)
			continue
		}
		if result.tombstone {
			t.Errorf("search(%q): tombstone = true, want false", e.key)
		}
		if string(result.value) != string(e.value) {
			t.Errorf("search(%q): value = %q, want %q", e.key, result.value, e.value)
		}
		if result.seq != e.seq {
			t.Errorf("search(%q): seq = %d, want %d", e.key, result.seq, e.seq)
		}
	}
}

// TestNewSst_RoundTrip_Tombstone verifies that a tombstone entry round-trips correctly.
func TestNewSst_RoundTrip_Tombstone(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "dead", value: []byte{}, seq: 5, tombstone: true},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	result, err := s.search("dead")
	if err != nil {
		t.Fatalf("search(\"dead\"): %v", err)
	}
	if !result.tombstone {
		t.Error("tombstone = false, want true")
	}
	if result.seq != 5 {
		t.Errorf("seq = %d, want 5", result.seq)
	}
}

// TestNewSst_MultiBlock verifies that entries exceeding TARGET_BLOCK_SIZE produce
// more than one index block, and that search still finds every key correctly.
func TestNewSst_MultiBlock(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Build enough entries to exceed at least one 4 KB block.
	// Each entry is ~100 bytes; 50 entries ≈ 5 KB → at least 2 blocks.
	var entries []testEntry
	for i := 0; i < 50; i++ {
		key := string(rune('a'+i%26)) + strings.Repeat("x", 10)
		// Keys must be unique and sorted; use a fixed-width format.
		key = string([]byte{byte('a' + i/26), byte('a' + i%26)}) + strings.Repeat("x", 8)
		entries = append(entries, testEntry{
			key:   key,
			value: []byte(strings.Repeat("v", 80)),
			seq:   uint64(i + 1),
		})
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	if s.index == nil || len(*s.index) < 2 {
		t.Errorf("expected at least 2 index blocks for large memtable, got %d", len(*s.index))
	}

	// Spot-check: first and last entries must be readable.
	first := entries[0]
	result, err := s.search(first.key)
	if err != nil {
		t.Errorf("search(%q): %v", first.key, err)
	} else if string(result.value) != string(first.value) {
		t.Errorf("search(%q) = %q, want %q", first.key, result.value, first.value)
	}

	last := entries[len(entries)-1]
	result, err = s.search(last.key)
	if err != nil {
		t.Errorf("search(%q): %v", last.key, err)
	} else if string(result.value) != string(last.value) {
		t.Errorf("search(%q) = %q, want %q", last.key, result.value, last.value)
	}
}

// TestNewSst_FooterRoundTrip writes an SST via newSst then calls readFooter to
// verify the footer, index, and bloom filter are all written and parsed correctly.
func TestNewSst_FooterRoundTrip(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 2},
		{key: "cherry", value: []byte("dark-red"), seq: 3},
	}
	m := newTestMemtable(t, entries)

	s, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSst: %v", err)
	}

	// Read the footer back from the file written by newSst.
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", s.level, s.filenum)
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("open sst file: %v", err)
	}
	defer f.Close()
	idx, bf, _, err := readFooter(f)
	if err != nil {
		t.Fatalf("readFooter: %v", err)
	}

	// Index must be non-empty and its last block must cover the last key.
	if idx == nil || len(*idx) == 0 {
		t.Fatal("readFooter returned empty index")
	}
	last := (*idx)[len(*idx)-1]
	if last.lastKey != "cherry" {
		t.Errorf("index last block lastKey = %q, want %q", last.lastKey, "cherry")
	}

	// Bloom filter must be present and report inserted keys as potentially present.
	if bf == nil {
		t.Fatal("readFooter returned nil bloom filter")
	}
	for _, e := range entries {
		if bf.keyNotPresent(e.key) {
			t.Errorf("bloom filter reports %q not present after readFooter, want present", e.key)
		}
	}
}

func TestNewBlock(t *testing.T) {
	lastKey := "zKey"
	offset := uint64(100)
	length := uint64(200)
	prevKey := "aKey"
	b := newBlock(lastKey, offset, length, prevKey)

	if b.lastKey != "zKey" {
		t.Errorf("lastKey = %q, want %q", b.lastKey, "zKey")
	}
	if b.offset != 100 {
		t.Errorf("offset = %d, want 100", b.offset)
	}
	if b.length != 200 {
		t.Errorf("length = %d, want 200", b.length)
	}
	if b.prevBlockKey != "aKey" {
		t.Errorf("prevBlockKey = %q, want %q", b.prevBlockKey, "aKey")
	}
}

func TestIndex_GetDatablock_Found(t *testing.T) {
	idx := index{
		newBlock("cherry", 0, 100, ""),
		newBlock("mango", 100, 150, "cherry"),
		newBlock("zebra", 250, 200, "mango"),
	}

	tests := []struct {
		key            string
		expectedOffset uint64
		expectedLength uint64
	}{
		{"apple", 0, 100},   // "" < "apple" <= "cherry"
		{"cherry", 0, 100},  // "" < "cherry" <= "cherry"
		{"date", 100, 150},  // "cherry" < "date" <= "mango"
		{"mango", 100, 150}, // "cherry" < "mango" <= "mango"
		{"pear", 250, 200},  // "mango" < "pear" <= "zebra"
		{"zebra", 250, 200}, // "mango" < "zebra" <= "zebra"
	}

	for _, tt := range tests {
		key := tt.key
		offset, length, err := idx.getDatablock(key)
		if err != nil {
			t.Errorf("getDatablock(%q): unexpected error: %v", tt.key, err)
			continue
		}
		if offset != tt.expectedOffset || length != tt.expectedLength {
			t.Errorf("getDatablock(%q) = (%d, %d), want (%d, %d)",
				tt.key, offset, length, tt.expectedOffset, tt.expectedLength)
		}
	}
}

func TestIndex_GetDatablock_NotFound(t *testing.T) {
	idx := index{
		newBlock("cherry", 0, 100, ""),
		newBlock("mango", 100, 150, "cherry"),
	}

	// Key past the last block range.
	key := "zzz"
	_, _, err := idx.getDatablock(key)
	if err != ErrKeyNotFound {
		t.Errorf("getDatablock(\"zzz\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestIndex_GetDatablock_FirstBlock(t *testing.T) {
	idx := index{
		newBlock("dog", 0, 50, ""),
		newBlock("fox", 50, 50, "dog"),
	}

	// First block has prevBlockKey = ""; any key > "" and <= "dog" should hit it.
	key := "ant"
	offset, length, err := idx.getDatablock(key)
	if err != nil {
		t.Fatalf("getDatablock(\"ant\"): %v", err)
	}
	if offset != 0 || length != 50 {
		t.Errorf("getDatablock(\"ant\") = (%d, %d), want (0, 50)", offset, length)
	}
}

/* ====================================================================================
	SST SEARCH TESTS
==================================================================================== */

func TestSst_Search_Found(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 2},
		{key: "cherry", value: []byte("dark-red"), seq: 3},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	result, err := s.search("banana")
	if err != nil {
		t.Fatalf("search(\"banana\"): %v", err)
	}
	if result.tombstone {
		t.Error("tombstone = true, want false")
	}
	if string(result.value) != "yellow" {
		t.Errorf("value = %q, want %q", result.value, "yellow")
	}
	if result.seq != 2 {
		t.Errorf("seq = %d, want 2", result.seq)
	}
}

func TestSst_Search_FirstEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte("first"), seq: 1},
		{key: "beta", value: []byte("second"), seq: 2},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	result, err := s.search("alpha")
	if err != nil {
		t.Fatalf("search(\"alpha\"): %v", err)
	}
	if string(result.value) != "first" {
		t.Errorf("value = %q, want %q", result.value, "first")
	}
}

func TestSst_Search_LastEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte("first"), seq: 1},
		{key: "beta", value: []byte("second"), seq: 2},
		{key: "gamma", value: []byte("third"), seq: 3},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	result, err := s.search("gamma")
	if err != nil {
		t.Fatalf("search(\"gamma\"): %v", err)
	}
	if string(result.value) != "third" {
		t.Errorf("value = %q, want %q", result.value, "third")
	}
	if result.seq != 3 {
		t.Errorf("seq = %d, want 3", result.seq)
	}
}

func TestSst_Search_Tombstone(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte{}, seq: 5, tombstone: true},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	result, err := s.search("alpha")
	if err != nil {
		t.Fatalf("search(\"alpha\"): %v", err)
	}
	if !result.tombstone {
		t.Error("tombstone = false, want true")
	}
	if result.seq != 5 {
		t.Errorf("seq = %d, want 5", result.seq)
	}
}

func TestSst_Search_NotFound(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte("a"), seq: 1},
		{key: "beta", value: []byte("b"), seq: 2},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	// "aqua" is within the index range (> "" and <= "beta")
	// but does not exist as an entry in the data block.
	_, err := s.search("aqua")
	if err != ErrKeyNotFound {
		t.Errorf("search(\"aqua\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestSst_Search_BadChecksum(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "hello", value: []byte("world"), seq: 1},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	// Corrupt a byte in the value region of the data block.
	// Layout: seq(8) + tomb(1) + keyLen(4) + key("hello"=5) + valLen(4) + value("world"=5)
	// Value starts at offset 22; flip a bit there.
	filename := "data/sstables/level-0/1.sst"
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 22); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], 22); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = s.search("hello")
	if err != ErrBadData {
		t.Errorf("search after corruption: err = %v, want ErrBadData", err)
	}
}

/* ====================================================================================
	READ FOOTER TESTS
==================================================================================== */

func TestSst_ReadFooter(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "foo", value: []byte("bar"), seq: 10},
		{key: "qux", value: []byte("baz"), seq: 11},
	}
	writeSyntheticSST(t, 0, 1, entries, crcTab)

	f, err := os.Open("data/sstables/level-0/1.sst")
	if err != nil {
		t.Fatalf("open sst file: %v", err)
	}
	defer f.Close()
	idx, bf, _, err := readFooter(f)
	if err != nil {
		t.Fatalf("readFooter: %v", err)
	}

	// Verify index: single block with lastKey = "qux"
	if len(*idx) != 1 {
		t.Fatalf("index length = %d, want 1", len(*idx))
	}
	if (*idx)[0].lastKey != "qux" {
		t.Errorf("index[0].lastKey = %q, want %q", (*idx)[0].lastKey, "qux")
	}
	if (*idx)[0].prevBlockKey != "" {
		t.Errorf("index[0].prevBlockKey = %q, want \"\"", (*idx)[0].prevBlockKey)
	}

	// Verify bloom filter
	if bf == nil {
		t.Fatal("bloom filter is nil")
	}
	if len(bf.bitstring) == 0 {
		t.Error("bloom filter bitstring is empty")
	}
}

func TestSst_ReadFooter_BadMagic(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "a", value: []byte("b"), seq: 1},
	}
	writeSyntheticSST(t, 0, 1, entries, crcTab)

	// Corrupt the magic bytes (last 8 bytes of the file).
	filename := "data/sstables/level-0/1.sst"
	fi, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, fi.Size()-8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	f2, err := os.Open("data/sstables/level-0/1.sst")
	if err != nil {
		t.Fatalf("open sst file: %v", err)
	}
	defer f2.Close()
	_, _, _, err = readFooter(f2)
	if err != ErrBadFile {
		t.Errorf("readFooter with bad magic: err = %v, want ErrBadFile", err)
	}
}

/* ====================================================================================
	REPLAY SST TESTS
==================================================================================== */

// TestReplaySst_RoundTrip writes an SST via newSstFromMemtable and then replays
// it, checking that all metadata matches.
func TestReplaySst_RoundTrip(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 5},
		{key: "cherry", value: []byte("dark-red"), seq: 3},
	}
	m := newTestMemtable(t, entries)
	s, err := newSstFromMemtable(7, m, crcTab)
	if err != nil {
		t.Fatalf("newSstFromMemtable: %v", err)
	}

	replayed, err := replaySst("data/sstables/level-0/7.sst", crcTab)
	if err != nil {
		t.Fatalf("replaySst: %v", err)
	}

	if replayed.filenum != s.filenum {
		t.Errorf("filenum = %d, want %d", replayed.filenum, s.filenum)
	}
	if replayed.level != s.level {
		t.Errorf("level = %d, want %d", replayed.level, s.level)
	}
	if replayed.startKey != s.startKey {
		t.Errorf("startKey = %q, want %q", replayed.startKey, s.startKey)
	}
	if replayed.endKey != s.endKey {
		t.Errorf("endKey = %q, want %q", replayed.endKey, s.endKey)
	}
	if replayed.lastSeq != s.lastSeq {
		t.Errorf("lastSeq = %d, want %d", replayed.lastSeq, s.lastSeq)
	}
	if replayed.sizeBytes != s.sizeBytes {
		t.Errorf("sizeBytes = %d, want %d", replayed.sizeBytes, s.sizeBytes)
	}
	if replayed.index == nil || len(*replayed.index) == 0 {
		t.Fatal("replayed index is nil or empty")
	}
	if replayed.bloomFilter == nil {
		t.Fatal("replayed bloom filter is nil")
	}
}

// TestReplaySst_KeysSearchable verifies that the replayed sst can serve reads.
func TestReplaySst_KeysSearchable(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "dog", value: []byte("bark"), seq: 2},
		{key: "fox", value: []byte("quick"), seq: 4},
	}
	m := newTestMemtable(t, entries)
	_, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSstFromMemtable: %v", err)
	}

	replayed, err := replaySst("data/sstables/level-0/1.sst", crcTab)
	if err != nil {
		t.Fatalf("replaySst: %v", err)
	}

	for _, e := range entries {
		res, err := replayed.search(e.key)
		if err != nil {
			t.Errorf("search(%q) after replay: %v", e.key, err)
			continue
		}
		if string(res.value) != string(e.value) {
			t.Errorf("search(%q) = %q, want %q", e.key, res.value, e.value)
		}
	}
}

// TestReplaySst_SingleEntry checks the edge case of an SST with exactly one entry.
// startKey and endKey must both equal that single key.
func TestReplaySst_SingleEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "only", value: []byte("one"), seq: 42},
	}
	m := newTestMemtable(t, entries)
	_, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSstFromMemtable: %v", err)
	}

	replayed, err := replaySst("data/sstables/level-0/1.sst", crcTab)
	if err != nil {
		t.Fatalf("replaySst: %v", err)
	}

	if replayed.startKey != "only" {
		t.Errorf("startKey = %q, want %q", replayed.startKey, "only")
	}
	if replayed.endKey != "only" {
		t.Errorf("endKey = %q, want %q", replayed.endKey, "only")
	}
	if replayed.lastSeq != 42 {
		t.Errorf("lastSeq = %d, want 42", replayed.lastSeq)
	}
}

// TestReplaySst_TombstoneEntry verifies that a tombstone entry is replayed and
// reflected in lastSeq. search() returns the raw entry with tombstone=true;
// tombstone interpretation is the caller's responsibility (see sstables.get).
func TestReplaySst_TombstoneEntry(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alive", value: []byte("yes"), seq: 1},
		{key: "dead", value: []byte{}, seq: 9, tombstone: true},
	}
	m := newTestMemtable(t, entries)
	_, err := newSstFromMemtable(1, m, crcTab)
	if err != nil {
		t.Fatalf("newSstFromMemtable: %v", err)
	}

	replayed, err := replaySst("data/sstables/level-0/1.sst", crcTab)
	if err != nil {
		t.Fatalf("replaySst: %v", err)
	}

	// lastSeq must reflect the tombstone's seq (9 > 1)
	if replayed.lastSeq != 9 {
		t.Errorf("lastSeq = %d, want 9", replayed.lastSeq)
	}

	// search() returns the raw entry; tombstone flag must be set
	e, err := replayed.search("dead")
	if err != nil {
		t.Fatalf("search(\"dead\"): %v", err)
	}
	if !e.tombstone {
		t.Error("tombstone = false, want true")
	}
	if e.seq != 9 {
		t.Errorf("seq = %d, want 9", e.seq)
	}
}

// TestReplaySst_HigherLevelPath verifies that replaySst correctly parses level
// and filenum from a deeper level path (e.g. level-2/5.sst).
func TestReplaySst_HigherLevelPath(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "m", value: []byte("mid"), seq: 7},
		{key: "z", value: []byte("end"), seq: 8},
	}
	// Write a synthetic SST at level 2, filenum 5
	writeSyntheticSST(t, 2, 5, entries, crcTab)

	replayed, err := replaySst("data/sstables/level-2/5.sst", crcTab)
	if err != nil {
		t.Fatalf("replaySst: %v", err)
	}

	if replayed.level != 2 {
		t.Errorf("level = %d, want 2", replayed.level)
	}
	if replayed.filenum != 5 {
		t.Errorf("filenum = %d, want 5", replayed.filenum)
	}
}

// TestReplaySst_BadPath verifies that an unrecognised path format returns an error.
func TestReplaySst_BadPath(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	_, err := replaySst("not-a-valid/path/file.sst", crcTab)
	if err == nil {
		t.Error("expected error for bad path format, got nil")
	}
	if !strings.Contains(err.Error(), "could not parse") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestReplaySst_FileNotFound verifies that a missing file returns an OS error.
func TestReplaySst_FileNotFound(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	_, err := replaySst("data/sstables/level-0/999.sst", crcTab)
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
}

// TestReplaySst_CorruptMagic verifies that a file with a corrupt footer
// returns ErrBadFile wrapped in the replaySst error.
func TestReplaySst_CorruptMagic(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "a", value: []byte("b"), seq: 1},
	}
	writeSyntheticSST(t, 0, 1, entries, crcTab)

	// Corrupt the last 8 bytes (magic bytes)
	fi, err := os.Stat("data/sstables/level-0/1.sst")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile("data/sstables/level-0/1.sst", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, fi.Size()-8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = replaySst("data/sstables/level-0/1.sst", crcTab)
	if err == nil {
		t.Fatal("expected error for corrupt magic, got nil")
	}
	if !strings.Contains(err.Error(), ErrBadFile.Error()) {
		t.Errorf("unexpected error: %v", err)
	}
}

/* ====================================================================================
	SSTABLES (MULTI-SST) GET TESTS
==================================================================================== */

func TestSstables_Get_FromLevel0(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "fox", value: []byte("quick"), seq: 1},
		{key: "rabbit", value: []byte("fluffy"), seq: 2},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	ss := &sstables{
		levels:   []*level{{sstList: []*sst{s}}},
		crcTable: crcTab,
	}

	key := "rabbit"
	val, err := ss.get(key)
	if err != nil {
		t.Fatalf("get(\"rabbit\"): %v", err)
	}
	if string(val) != "fluffy" {
		t.Errorf("get(\"rabbit\") = %q, want %q", val, "fluffy")
	}
}

func TestSstables_Get_NotFound(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "only", value: []byte("one"), seq: 1},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	ss := &sstables{
		levels:   []*level{{sstList: []*sst{s}}},
		crcTable: crcTab,
	}

	key := "missing"
	_, err := ss.get(key)
	if err != ErrKeyNotFound {
		t.Errorf("get(\"missing\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestSstables_Get_TombstoneLevel0(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Level 1: key exists with a value.
	entriesL1 := []testEntry{
		{key: "key", value: []byte("alive"), seq: 1},
	}
	sL1 := writeSyntheticSST(t, 1, 1, entriesL1, crcTab)

	// Level 0: tombstone for the same key with a higher seq.
	entriesL0 := []testEntry{
		{key: "key", value: []byte{}, seq: 2, tombstone: true},
	}
	sL0 := writeSyntheticSST(t, 0, 2, entriesL0, crcTab)

	ss := &sstables{
		levels: []*level{
			{sstList: []*sst{sL0}}, // level 0
			{sstList: []*sst{sL1}}, // level 1
		},
		crcTable: crcTab,
	}

	key := "key"
	_, err := ss.get(key)
	if err != ErrKeyNotFound {
		t.Errorf("get(\"key\"): err = %v, want ErrKeyNotFound (level-0 tombstone should mask level-1 value)", err)
	}
}

func TestSstables_Get_Level0SeqPrecedence(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Two SSTs at level 0 with the same key but different sequences.
	entriesOld := []testEntry{
		{key: "key", value: []byte("old"), seq: 1},
	}
	sOld := writeSyntheticSST(t, 0, 1, entriesOld, crcTab)

	entriesNew := []testEntry{
		{key: "key", value: []byte("new"), seq: 5},
	}
	sNew := writeSyntheticSST(t, 0, 2, entriesNew, crcTab)

	ss := &sstables{
		levels:   []*level{{sstList: []*sst{sOld, sNew}}},
		crcTable: crcTab,
	}

	key := "key"
	val, err := ss.get(key)
	if err != nil {
		t.Fatalf("get(\"key\"): %v", err)
	}
	if string(val) != "new" {
		t.Errorf("get(\"key\") = %q, want %q (higher seq should win)", val, "new")
	}
}

/* ====================================================================================
	COMPACTION TESTS
==================================================================================== */

func TestCompact_NoOverlap(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Source SST (Level 0)
	srcEntries := []testEntry{
		{key: "a", value: []byte("1"), seq: 1},
		{key: "b", value: []byte("2"), seq: 2},
		{key: "c", value: []byte("3"), seq: 3},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	// Compaction source with no overlapping SSTs
	src, err := newCompactionSrc(srcSst, []*sst{}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	// Target SST — large capacity so all entries fit
	newSst := newSst(200, 1, crcTab, defaultSstCapacity)

	err = newSst.compact(src, false)
	if err != ErrCompactionDone {
		t.Fatalf("compact returned %v, want ErrCompactionDone", err)
	}

	// Verify new SST contents
	for _, e := range srcEntries {
		res, err := newSst.search(e.key)
		if err != nil {
			t.Errorf("search(%q): %v", e.key, err)
		} else if string(res.value) != string(e.value) {
			t.Errorf("search(%q) = %q, want %q", e.key, res.value, e.value)
		}
	}
}

func TestCompact_WithOverlap_UpdateAndNewKeys(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Source SST (Level 0)
	srcEntries := []testEntry{
		{key: "apple", value: []byte("red-new"), seq: 5},
		{key: "banana", value: []byte("yellow"), seq: 4},
		{key: "cherry", value: []byte("dark-new"), seq: 6},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	// Overlapping SST (Level 1)
	overlapEntries := []testEntry{
		{key: "apple", value: []byte("red-old"), seq: 1},
		{key: "cherry", value: []byte("dark-old"), seq: 2},
		{key: "date", value: []byte("brown"), seq: 3},
	}
	overlapSst := writeSyntheticSST(t, 1, 101, overlapEntries, crcTab)

	src, err := newCompactionSrc(srcSst, []*sst{overlapSst}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	newSst := newSst(200, 1, crcTab, defaultSstCapacity)

	err = newSst.compact(src, false)
	if err != ErrCompactionDone {
		t.Fatalf("compact returned %v, want ErrCompactionDone", err)
	}

	expected := map[string]string{
		"apple":  "red-new",  // updated from L0
		"banana": "yellow",   // new from L0
		"cherry": "dark-new", // updated from L0
		"date":   "brown",    // kept from L1
	}

	for k, want := range expected {
		res, err := newSst.search(k)
		if err != nil {
			t.Errorf("search(%q): %v", k, err)
		} else if string(res.value) != want {
			t.Errorf("search(%q) = %q, want %q", k, res.value, want)
		}
	}
}

func TestCompact_CapacitySplit(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	srcEntries := []testEntry{
		{key: "k1", value: []byte("v1"), seq: 1},
		{key: "k2", value: []byte("v2"), seq: 2},
		{key: "k3", value: []byte("v3"), seq: 3},
		{key: "k4", value: []byte("v4"), seq: 4},
		{key: "k5", value: []byte("v5"), seq: 5},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	src, err := newCompactionSrc(srcSst, []*sst{}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	// Determine exactly how many bytes 2 entries take up
	cw, _ := newCompactionWriter(newSst(900, 1, crcTab, defaultSstCapacity))
	cw.writeEntry(&entry{key: "k1", value: []byte("v1"), seq: 1})
	cw.writeEntry(&entry{key: "k2", value: []byte("v2"), seq: 2})
	twoEntrySize := uint64(cw.currentBlockBuf.Len())
	cw.outFile.Close()
	os.Remove(cw.filename)

	// SST 1: Should fill up after 2 entries
	sst1 := newSst(200, 1, crcTab, twoEntrySize)

	err = sst1.compact(src, false)
	if err != ErrCompactionFull {
		t.Fatalf("sst1.compact returned %v, want ErrCompactionFull", err)
	}

	// SST 2: Should pick up the remaining 3 entries
	sst2 := newSst(201, 1, crcTab, defaultSstCapacity)

	err = sst2.compact(src, false)
	if err != ErrCompactionDone {
		t.Fatalf("sst2.compact returned %v, want ErrCompactionDone", err)
	}

	// Verify splitting
	if _, err := sst1.search("k2"); err != nil {
		t.Errorf("sst1 missing k2: %v", err)
	}
	if _, err := sst1.search("k3"); err != ErrKeyNotFound {
		t.Errorf("sst1 shouldn't have k3")
	}

	if _, err := sst2.search("k3"); err != nil {
		t.Errorf("sst2 missing k3: %v", err)
	}
	if _, err := sst2.search("k2"); err != ErrKeyNotFound {
		t.Errorf("sst2 shouldn't have k2")
	}
}

func TestCompact_TombstonePruning(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// tombstone for "deleted", normal entry for "kept"
	srcEntries := []testEntry{
		{key: "deleted", value: []byte{}, seq: 5, tombstone: true},
		{key: "kept", value: []byte("data"), seq: 6},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	// L1 also has an older version of "deleted"
	overlapEntries := []testEntry{
		{key: "deleted", value: []byte("old-data"), seq: 1},
	}
	overlapSst := writeSyntheticSST(t, 1, 101, overlapEntries, crcTab)

	src, err := newCompactionSrc(srcSst, []*sst{overlapSst}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	newSst := newSst(200, 1, crcTab, defaultSstCapacity)

	// isLastLevel = true
	err = newSst.compact(src, true)
	if err != ErrCompactionDone {
		t.Fatalf("compact returned %v, want ErrCompactionDone", err)
	}

	// "deleted" should be entirely pruned because isLastLevel=true and it shadowed the L1 entry
	if _, err := newSst.search("deleted"); err != ErrKeyNotFound {
		t.Errorf("expected 'deleted' to be pruned (ErrKeyNotFound), got %v", err)
	}

	res, err := newSst.search("kept")
	if err != nil {
		t.Errorf("expected 'kept' to be present, got %v", err)
	} else if string(res.value) != "data" {
		t.Errorf("search('kept') = %q, want 'data'", res.value)
	}
}

/* ====================================================================================
	ADDITIONAL COMPACTION TESTS
==================================================================================== */

func TestCompactionWriter_Abort_CleansFile(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	s := newSst(300, 1, crcTab, defaultSstCapacity)
	cw, err := newCompactionWriter(s)
	if err != nil {
		t.Fatalf("newCompactionWriter: %v", err)
	}

	// Write some data so a file definitely exists
	cw.writeEntry(&entry{key: "x", value: []byte("y"), seq: 1})
	filename := cw.filename

	// Verify file exists before abort
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatal("expected SST file to exist before abort")
	}

	cw.abort()

	// File should be deleted after abort
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Errorf("expected SST file to be deleted after abort, got err = %v", err)
	}
}

func TestCompactionSrc_AdvanceOrder(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Stream A (source SST)
	srcEntries := []testEntry{
		{key: "b", value: []byte("B"), seq: 2},
		{key: "d", value: []byte("D"), seq: 4},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	// Stream B (overlapping SST)
	overlapEntries := []testEntry{
		{key: "a", value: []byte("A"), seq: 1},
		{key: "c", value: []byte("C"), seq: 3},
		{key: "e", value: []byte("E"), seq: 5},
	}
	overlapSst := writeSyntheticSST(t, 1, 101, overlapEntries, crcTab)

	src, err := newCompactionSrc(srcSst, []*sst{overlapSst}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	// Collect all entries via advance()
	var keys []string
	for {
		err := src.advance()
		if err == ErrCompactionDone {
			break
		}
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
		keys = append(keys, src.pending.key)
	}

	// Entries must be in sorted key order
	expectedOrder := []string{"a", "b", "c", "d", "e"}
	if len(keys) != len(expectedOrder) {
		t.Fatalf("advance produced %d entries, want %d", len(keys), len(expectedOrder))
	}
	for i, k := range keys {
		if k != expectedOrder[i] {
			t.Errorf("advance[%d] = %q, want %q", i, k, expectedOrder[i])
		}
	}
}

func TestSstables_Flush_AddsToL0(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	ss := newSstables(4096, 10, crcTab)

	entries := []testEntry{
		{key: "foo", value: []byte("bar"), seq: 2},
		{key: "hello", value: []byte("world"), seq: 1},
	}
	m := newTestMemtable(t, entries)

	if err := ss.flush(1, m); err != nil {
		t.Fatalf("flush: %v", err)
	}

	ss.mu.RLock()
	l0 := ss.levels[0]
	ss.mu.RUnlock()

	if len(l0.sstList) != 1 {
		t.Fatalf("expected 1 SST in L0, got %d", len(l0.sstList))
	}
	if l0.sizeBytes == 0 {
		t.Error("expected L0 sizeBytes > 0")
	}

	// Verify the SST is searchable
	val, err := ss.get("hello")
	if err != nil {
		t.Fatalf("get('hello'): %v", err)
	}
	if string(val) != "world" {
		t.Errorf("get('hello') = %q, want 'world'", val)
	}
}

func TestCompact_OverlapMultipleSSTs(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Source SST (L0) spanning keys a-f
	srcEntries := []testEntry{
		{key: "b", value: []byte("B-new"), seq: 10},
		{key: "e", value: []byte("E-new"), seq: 11},
	}
	srcSst := writeSyntheticSST(t, 0, 100, srcEntries, crcTab)

	// Overlapping SST 1 (L1): covers a-c
	over1Entries := []testEntry{
		{key: "a", value: []byte("A-old"), seq: 1},
		{key: "b", value: []byte("B-old"), seq: 2},
		{key: "c", value: []byte("C-old"), seq: 3},
	}
	overSst1 := writeSyntheticSST(t, 1, 101, over1Entries, crcTab)

	// Overlapping SST 2 (L1): covers d-f
	over2Entries := []testEntry{
		{key: "d", value: []byte("D-old"), seq: 4},
		{key: "e", value: []byte("E-old"), seq: 5},
		{key: "f", value: []byte("F-old"), seq: 6},
	}
	overSst2 := writeSyntheticSST(t, 1, 102, over2Entries, crcTab)

	src, err := newCompactionSrc(srcSst, []*sst{overSst1, overSst2}, crcTab)
	if err != nil {
		t.Fatalf("newCompactionSrc: %v", err)
	}
	defer src.close()

	newSst := newSst(200, 1, crcTab, defaultSstCapacity)
	err = newSst.compact(src, false)
	if err != ErrCompactionDone {
		t.Fatalf("compact returned %v, want ErrCompactionDone", err)
	}

	// Verify merged results
	expected := map[string]string{
		"a": "A-old",
		"b": "B-new", // updated from L0
		"c": "C-old",
		"d": "D-old",
		"e": "E-new", // updated from L0
		"f": "F-old",
	}

	for k, want := range expected {
		res, err := newSst.search(k)
		if err != nil {
			t.Errorf("search(%q): %v", k, err)
		} else if string(res.value) != want {
			t.Errorf("search(%q) = %q, want %q", k, res.value, want)
		}
	}
}

/* ====================================================================================
	REBUILD SSTABLES TESTS
==================================================================================== */

// TestRebuildSstables_NoDataDir verifies that rebuildSstables succeeds on a
// completely fresh engine where data/sstables/ does not exist yet.
func TestRebuildSstables_NoDataDir(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	ssts, maxSeq, maxFilenum, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}
	if len(ssts.levels) != 1 {
		t.Errorf("levels = %d, want 1 (only L0 from newSstables)", len(ssts.levels))
	}
	if len(ssts.levels[0].sstList) != 0 {
		t.Errorf("L0 sstList len = %d, want 0", len(ssts.levels[0].sstList))
	}
	if maxSeq != 0 {
		t.Errorf("maxSeq = %d, want 0", maxSeq)
	}
	if maxFilenum != 0 {
		t.Errorf("maxFilenum = %d, want 0", maxFilenum)
	}
}

// TestRebuildSstables_EmptyLevel0Dir verifies that an existing but empty
// data/sstables/level-0/ directory produces no SSTs and zero maxSeq/maxFilenum.
func TestRebuildSstables_EmptyLevel0Dir(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	if err := os.MkdirAll("data/sstables/level-0", 0755); err != nil {
		t.Fatal(err)
	}

	ssts, maxSeq, maxFilenum, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}
	if len(ssts.levels[0].sstList) != 0 {
		t.Errorf("L0 sstList len = %d, want 0", len(ssts.levels[0].sstList))
	}
	if maxSeq != 0 {
		t.Errorf("maxSeq = %d, want 0", maxSeq)
	}
	if maxFilenum != 0 {
		t.Errorf("maxFilenum = %d, want 0", maxFilenum)
	}
}

// TestRebuildSstables_SingleSstL0 verifies a single SST written to L0 is
// correctly replayed: metadata fields and a live search both match.
func TestRebuildSstables_SingleSstL0(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "cat", value: []byte("meow"), seq: 3},
		{key: "dog", value: []byte("bark"), seq: 7},
	}
	m := newTestMemtable(t, entries)
	_, err := newSstFromMemtable(7, m, crcTab)
	if err != nil {
		t.Fatalf("newSstFromMemtable: %v", err)
	}

	ssts, maxSeq, maxFilenum, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}

	if len(ssts.levels[0].sstList) != 1 {
		t.Fatalf("L0 sstList len = %d, want 1", len(ssts.levels[0].sstList))
	}
	s := ssts.levels[0].sstList[0]
	if s.filenum != 7 {
		t.Errorf("filenum = %d, want 7", s.filenum)
	}
	if ssts.levels[0].sizeBytes == 0 {
		t.Error("L0 sizeBytes = 0, want > 0")
	}
	if maxFilenum != 7 {
		t.Errorf("maxFilenum = %d, want 7", maxFilenum)
	}
	if maxSeq != 7 {
		t.Errorf("maxSeq = %d, want 7", maxSeq)
	}

	// Verify the replayed SST can serve reads
	entry, err := s.search("dog")
	if err != nil {
		t.Fatalf("search(\"dog\"): %v", err)
	}
	if string(entry.value) != "bark" {
		t.Errorf("search(\"dog\") = %q, want \"bark\"", entry.value)
	}
}

// TestRebuildSstables_MultipleSstsL0 writes 3 SSTs with out-of-order key ranges
// and verifies that sortByStartKey orders them correctly and sizeBytes accumulates.
func TestRebuildSstables_MultipleSstsL0(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Write SSTs with deliberately interleaved key ranges and out-of-order filenums.
	writeSyntheticSST(t, 0, 3, []testEntry{{key: "m", value: []byte("1"), seq: 10}, {key: "z", value: []byte("2"), seq: 11}}, crcTab)
	writeSyntheticSST(t, 0, 1, []testEntry{{key: "a", value: []byte("3"), seq: 1}, {key: "e", value: []byte("4"), seq: 2}}, crcTab)
	writeSyntheticSST(t, 0, 2, []testEntry{{key: "f", value: []byte("5"), seq: 5}, {key: "l", value: []byte("6"), seq: 6}}, crcTab)

	ssts, maxSeq, maxFilenum, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}

	if len(ssts.levels[0].sstList) != 3 {
		t.Fatalf("L0 sstList len = %d, want 3", len(ssts.levels[0].sstList))
	}

	// Verify sorted order by startKey
	list := ssts.levels[0].sstList
	for i := 1; i < len(list); i++ {
		if list[i].startKey <= list[i-1].startKey {
			t.Errorf("sstList not sorted: [%d].startKey=%q <= [%d].startKey=%q",
				i, list[i].startKey, i-1, list[i-1].startKey)
		}
	}

	// sizeBytes must be the sum of all three SSTs' individual sizeBytes
	expectedSize := list[0].sizeBytes + list[1].sizeBytes + list[2].sizeBytes
	if ssts.levels[0].sizeBytes != expectedSize {
		t.Errorf("L0 sizeBytes = %d, want %d", ssts.levels[0].sizeBytes, expectedSize)
	}

	if maxFilenum != 3 {
		t.Errorf("maxFilenum = %d, want 3", maxFilenum)
	}
	if maxSeq != 11 {
		t.Errorf("maxSeq = %d, want 11", maxSeq)
	}
}

// TestRebuildSstables_MultipleLevel verifies that SSTs on L0 and L1 are placed
// into the correct level slices and that each level's capacity follows the growth factor.
func TestRebuildSstables_MultipleLevel(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	const l0Cap = uint64(4096)
	const growthFactor = 10

	writeSyntheticSST(t, 0, 1, []testEntry{{key: "a", value: []byte("v"), seq: 1}}, crcTab)
	writeSyntheticSST(t, 1, 2, []testEntry{{key: "z", value: []byte("v"), seq: 2}}, crcTab)

	ssts, maxSeq, maxFilenum, err := rebuildSstables(l0Cap, growthFactor, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}

	if len(ssts.levels) < 2 {
		t.Fatalf("levels len = %d, want >= 2", len(ssts.levels))
	}
	if len(ssts.levels[0].sstList) != 1 {
		t.Errorf("L0 sstList len = %d, want 1", len(ssts.levels[0].sstList))
	}
	if len(ssts.levels[1].sstList) != 1 {
		t.Errorf("L1 sstList len = %d, want 1", len(ssts.levels[1].sstList))
	}

	// Verify capacity geometry: L1 = L0 * growthFactor
	wantL1Cap := l0Cap * uint64(growthFactor)
	if ssts.levels[1].capacityBytes != wantL1Cap {
		t.Errorf("L1 capacityBytes = %d, want %d", ssts.levels[1].capacityBytes, wantL1Cap)
	}

	if maxFilenum != 2 {
		t.Errorf("maxFilenum = %d, want 2", maxFilenum)
	}
	if maxSeq != 2 {
		t.Errorf("maxSeq = %d, want 2", maxSeq)
	}
}

// TestRebuildSstables_MaxSeqAndFilenumTracking verifies that maxSeq and maxFilenum
// reflect the true maximum across all SSTs, not just the last one processed.
func TestRebuildSstables_MaxSeqAndFilenumTracking(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	// Highest seq lives in the middle SST (filenum 5); highest filenum is 10.
	writeSyntheticSST(t, 0, 10, []testEntry{{key: "a", value: []byte("v"), seq: 3}}, crcTab)
	writeSyntheticSST(t, 0, 5, []testEntry{{key: "g", value: []byte("v"), seq: 99}}, crcTab)
	writeSyntheticSST(t, 0, 8, []testEntry{{key: "n", value: []byte("v"), seq: 7}}, crcTab)

	_, maxSeq, maxFilenum, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}
	if maxFilenum != 10 {
		t.Errorf("maxFilenum = %d, want 10", maxFilenum)
	}
	if maxSeq != 99 {
		t.Errorf("maxSeq = %d, want 99", maxSeq)
	}
}

// TestRebuildSstables_NonSstFilesIgnored verifies that non-.sst files and
// subdirectories inside a level directory are silently skipped.
func TestRebuildSstables_NonSstFilesIgnored(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	writeSyntheticSST(t, 0, 1, []testEntry{{key: "k", value: []byte("v"), seq: 1}}, crcTab)

	// Add a non-.sst file and a subdirectory in the same level directory.
	if err := os.WriteFile("data/sstables/level-0/tmp.log", []byte("junk"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("data/sstables/level-0/subdir", 0755); err != nil {
		t.Fatal(err)
	}

	ssts, _, _, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}
	if len(ssts.levels[0].sstList) != 1 {
		t.Errorf("L0 sstList len = %d, want 1 (non-.sst files must be ignored)", len(ssts.levels[0].sstList))
	}
}

// TestRebuildSstables_CorruptSst verifies that a corrupt SST file (bad magic)
// causes rebuildSstables to return a non-nil error wrapping ErrBadFile.
func TestRebuildSstables_CorruptSst(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	writeSyntheticSST(t, 0, 1, []testEntry{{key: "k", value: []byte("v"), seq: 1}}, crcTab)

	// Corrupt the magic bytes (last 8 bytes of the file).
	fi, err := os.Stat("data/sstables/level-0/1.sst")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile("data/sstables/level-0/1.sst", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 0}, fi.Size()-8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, _, _, err = rebuildSstables(4096, 10, crcTab)
	if err == nil {
		t.Fatal("expected error for corrupt SST, got nil")
	}
	if !strings.Contains(err.Error(), ErrBadFile.Error()) {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRebuildSstables_RoundTrip_Reads writes SSTs, rebuilds from disk, then
// verifies that ssts.get() returns correct values for known keys and
// ErrKeyNotFound for an absent key.
func TestRebuildSstables_RoundTrip_Reads(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entriesA := []testEntry{
		{key: "apple", value: []byte("red"), seq: 1},
		{key: "banana", value: []byte("yellow"), seq: 2},
	}
	entriesB := []testEntry{
		{key: "cherry", value: []byte("dark-red"), seq: 3},
		{key: "date", value: []byte("brown"), seq: 4},
	}
	mA := newTestMemtable(t, entriesA)
	mB := newTestMemtable(t, entriesB)
	if _, err := newSstFromMemtable(1, mA, crcTab); err != nil {
		t.Fatalf("newSstFromMemtable A: %v", err)
	}
	if _, err := newSstFromMemtable(2, mB, crcTab); err != nil {
		t.Fatalf("newSstFromMemtable B: %v", err)
	}

	ssts, _, _, err := rebuildSstables(4096, 10, crcTab)
	if err != nil {
		t.Fatalf("rebuildSstables: %v", err)
	}

	// All written keys must be readable
	for _, e := range append(entriesA, entriesB...) {
		val, err := ssts.get(e.key)
		if err != nil {
			t.Errorf("get(%q): %v", e.key, err)
		} else if string(val) != string(e.value) {
			t.Errorf("get(%q) = %q, want %q", e.key, val, e.value)
		}
	}

	// An absent key must return ErrKeyNotFound
	if _, err := ssts.get("zucchini"); err != ErrKeyNotFound {
		t.Errorf("get(\"zucchini\") = %v, want ErrKeyNotFound", err)
	}
}
