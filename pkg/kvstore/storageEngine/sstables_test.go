package storageengine

import (
	"hash/crc32"
	"os"
	"testing"
)

/* ====================================================================================
	BLOCK / INDEX TESTS
==================================================================================== */

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

	key := "banana"
	val, isTombstone, seq, err := s.search(key)
	if err != nil {
		t.Fatalf("search(\"banana\"): %v", err)
	}
	if isTombstone {
		t.Error("isTombstone = true, want false")
	}
	if string(val) != "yellow" {
		t.Errorf("value = %q, want %q", val, "yellow")
	}
	if seq != 2 {
		t.Errorf("seq = %d, want 2", seq)
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

	key := "alpha"
	val, _, _, err := s.search(key)
	if err != nil {
		t.Fatalf("search(\"alpha\"): %v", err)
	}
	if string(val) != "first" {
		t.Errorf("value = %q, want %q", val, "first")
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

	key := "gamma"
	val, _, seq, err := s.search(key)
	if err != nil {
		t.Fatalf("search(\"gamma\"): %v", err)
	}
	if string(val) != "third" {
		t.Errorf("value = %q, want %q", val, "third")
	}
	if seq != 3 {
		t.Errorf("seq = %d, want 3", seq)
	}
}

func TestSst_Search_Tombstone(t *testing.T) {
	setupTestDir(t)
	crcTab := crc32.MakeTable(crc32.Castagnoli)

	entries := []testEntry{
		{key: "alpha", value: []byte{}, seq: 5, tombstone: true},
	}
	s := writeSyntheticSST(t, 0, 1, entries, crcTab)

	key := "alpha"
	_, isTombstone, seq, err := s.search(key)
	if err != nil {
		t.Fatalf("search(\"alpha\"): %v", err)
	}
	if !isTombstone {
		t.Error("isTombstone = false, want true")
	}
	if seq != 5 {
		t.Errorf("seq = %d, want 5", seq)
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
	key := "aqua"
	_, _, _, err := s.search(key)
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

	key := "hello"
	_, _, _, err = s.search(key)
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
		levels:   1,
		sstList:  [][]*sst{{s}},
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
		levels:   1,
		sstList:  [][]*sst{{s}},
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
		levels:   2,
		sstList:  [][]*sst{{sL0}, {sL1}},
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
		levels:   1,
		sstList:  [][]*sst{{sOld, sNew}},
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
