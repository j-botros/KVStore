package storageengine

import (
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
	if s.lastSeq != 2 {
		t.Errorf("lastSeq = %d, want 2 (seq of last key cherry)", s.lastSeq)
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
	idx, bf, err := s.readFooter()
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

	s := &sst{filenum: 1, level: 0, crcTable: crcTab}
	idx, bf, err := s.readFooter()
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

	s := &sst{filenum: 1, level: 0, crcTable: crcTab}
	_, _, err = s.readFooter()
	if err != ErrBadFile {
		t.Errorf("readFooter with bad magic: err = %v, want ErrBadFile", err)
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
