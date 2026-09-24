package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/* ====================================================================================
	SSTABLES CLASS
==================================================================================== */

// level represents a collection of SSTables that belong to a specific tier (level) in the LSM tree.
// It is used in the sstables manager to organize SSTables and enforce capacity limits during compactions.
type level struct {
	sstList       []*sst
	capacityBytes uint64
	sizeBytes     uint64
}

// newLevel creates and initializes a new level for SSTables.
//
// Parameters:
//   - capacityBytes (uint64): The maximum capacity in bytes for this level.
//
// Returns:
//   - *level: A pointer to the newly created level.
//
// Errors:
//   - None
func newLevel(capacityBytes uint64) *level {
	return &level{
		sstList:       make([]*sst, 0),
		capacityBytes: capacityBytes,
		sizeBytes:     0,
	}
}

// Use this when adding a single SST during compaction to keep L1+ sorted.
// insertSorted inserts an SSTable into the level while maintaining sort order by start key.
//
// Parameters:
//   - s (*sst): A pointer to the SSTable to insert.
//
// Returns:
//   - None
//
// Errors:
//   - None
func (l *level) insertSorted(s *sst) {
	i := sort.Search(len(l.sstList), func(i int) bool {
		return l.sstList[i].startKey >= s.startKey
	})
	l.sstList = append(l.sstList, nil)
	copy(l.sstList[i+1:], l.sstList[i:])
	l.sstList[i] = s
}

// Use this once after bulk-loading SSTs from disk on startup.
// sortByStartKey sorts all SSTables in the level by their start key.
//
// Parameters:
//   - None
//
// Returns:
//   - None
//
// Errors:
//   - None
func (l *level) sortByStartKey() {
	sort.Slice(l.sstList, func(i, j int) bool {
		return l.sstList[i].startKey < l.sstList[j].startKey
	})
}

// sstables represents the manager for all Sorted String Tables (SSTables) in the LSM tree.
// It is used in the storage engine to handle reads from disk, flushes from the memtable, and background compactions.
type sstables struct {
	levels       []*level
	l0Capacity   uint64
	growthFactor int
	crcTable     *crc32.Table
	mu           sync.RWMutex
}

// newSstables creates and initializes a new sstables manager.
//
// Parameters:
//   - l0Capacity (uint64): The capacity of Level 0 in bytes.
//   - growthFactor (int): The growth factor for capacities of subsequent levels.
//   - crcTable (*crc32.Table): The CRC table used for checksum calculations.
//
// Returns:
//   - *sstables: A pointer to the newly created sstables manager.
//
// Errors:
//   - None
func newSstables(l0Capacity uint64, growthFactor int, crcTable *crc32.Table) *sstables {
	sstables := &sstables{
		levels:       make([]*level, 1),
		l0Capacity:   l0Capacity,
		growthFactor: growthFactor,
		crcTable:     crcTable,
	}

	sstables.levels[0] = newLevel(l0Capacity)

	return sstables
}

// get searches for the value associated with the given key across all SSTables.
//
// Parameters:
//   - key (string): The key to search for.
//
// Returns:
//   - value ([]byte): The value associated with the key if found, otherwise nil.
//   - err (error): An error if the key is not found or if an issue occurs during reading.
//
// Errors:
//   - ErrKeyNotFound: Thrown when the key does not exist or has a tombstone in the SSTables.
//   - Other errors from underlying file reads or searches.
func (sstables *sstables) get(key string) (value []byte, err error) {
	// Step 1: snapshot candidate SST pointers under a brief read lock.
	// Holding the lock only for pointer copies, not for any disk I/O.
	sstables.mu.RLock()

	// Level 0: SSTs may overlap — collect all whose key range covers the target.
	var l0Candidates []*sst
	for _, s := range sstables.levels[0].sstList {
		if key >= s.startKey && key <= s.endKey {
			l0Candidates = append(l0Candidates, s)
		}
	}

	// Level 1+: SSTs are non-overlapping — collect the first match per level.
	var lvlCandidates []*sst
	for _, lvl := range sstables.levels[1:] {
		for _, s := range lvl.sstList {
			if key >= s.startKey && key <= s.endKey {
				lvlCandidates = append(lvlCandidates, s)
				break // non-overlapping: first match is the only match
			}
		}
	}

	sstables.mu.RUnlock()

	// Step 2: read each candidate file individually under its own per-SST lock

	// Level 0: search all candidates and pick the entry with the highest seq.
	seq := uint64(0)
	tombstone := false
	for _, s := range l0Candidates {
		s.mu.RLock()
		entry, err := s.search(key)
		s.mu.RUnlock()

		if err == ErrKeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}

		if entry.seq >= seq {
			seq = entry.seq
			value = entry.value
			tombstone = entry.tombstone
		}
	}

	if tombstone {
		return nil, ErrKeyNotFound
	} else if value != nil {
		return value, nil
	}

	// Level 1+: first match wins.
	for _, c := range lvlCandidates {
		c.mu.RLock()
		entry, err := c.search(key)
		c.mu.RUnlock()

		if err == ErrKeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}

		if entry.tombstone {
			return nil, ErrKeyNotFound
		}
		return entry.value, nil
	}

	return nil, ErrKeyNotFound
}

// flush converts a memtable into an SSTable and adds it to Level 0.
//
// Parameters:
//   - filenum (uint64): The file number for the new SSTable.
//   - memtable (*memtable): The memtable to flush.
//
// Returns:
//   - error: An error if flushing fails, otherwise nil.
//
// Errors:
//   - Throws errors if file creation, writing, or synchronization fails during newSstFromMemtable.
func (sstables *sstables) flush(filenum uint64, memtable *memtable) error {
	// I/O phase: no lock held — allows concurrent reads and other flushes.
	newSst, err := newSstFromMemtable(filenum, memtable, sstables.crcTable)
	if err != nil {
		return err
	}

	// List mutation: brief exclusive lock to append the new SST
	sstables.mu.Lock()
	l0 := sstables.levels[0]
	l0.sstList = append(l0.sstList, newSst)
	l0.sizeBytes += newSst.sizeBytes
	sstables.mu.Unlock()
	return nil
}

/* ====================================================================================
	SST CLASS & INDEX/BLOCK CLASSES
==================================================================================== */

// sst represents a single Sorted String Table (SSTable) file and its metadata.
// It is used in the storage engine to represent on-disk immutable key-value data, including its bloom filter and index.
type sst struct {
	// SST file data
	filenum   uint64
	level     int
	lastSeq   uint64
	startKey  string
	endKey    string
	capacity  uint64
	sizeBytes uint64

	// Index
	index *index

	// Bloom filter
	bloomFilter *bloomFilter

	// Checksum table
	crcTable *crc32.Table

	mu sync.RWMutex
}

const (
	// Used to validate SSTable
	FOOTER_MAGIC = uint64(0x4c55564c49414e41)
	// Index offset + Index length + Bloom offset + Bloom length + Footer magic
	FOOTER_SIZE = 8 + 8 + 8 + 8 + 8

	// Data block size (KB)
	TARGET_BLOCK_SIZE = 4 * 1024 // 4 KB
)

// newSst creates an empty SSTable struct in memory.
//
// Parameters:
//   - filenum (uint64): The file number for this SSTable.
//   - lvl (int): The level this SSTable belongs to.
//   - crcTable (*crc32.Table): The CRC table for checksums.
//   - capacity (uint64): The capacity for this SSTable.
//
// Returns:
//   - *sst: A pointer to the newly created SSTable struct.
//
// Errors:
//   - None
func newSst(filenum uint64, lvl int, crcTable *crc32.Table, capacity uint64) *sst {
	sst := &sst{
		filenum:  filenum,
		level:    lvl,
		crcTable: crcTable,
		capacity: capacity,
	}

	return sst
}

// newSstFromMemtable writes the contents of a memtable to a new SSTable file on disk.
//
// Parameters:
//   - filenum (uint64): The file number to assign to the new SSTable.
//   - memtable (*memtable): The memtable containing the data to persist.
//   - crcTable (*crc32.Table): The CRC table to use for checksums.
//
// Returns:
//   - *sst: A pointer to the SSTable struct created from the memtable.
//   - error: An error if file creation or writing fails.
//
// Errors:
//   - Throws errors if creating directories, opening the file, writing data, or syncing to disk fails.
func newSstFromMemtable(filenum uint64, memtable *memtable, crcTable *crc32.Table) (*sst, error) {
	sst := &sst{
		filenum:  filenum,
		level:    0,
		crcTable: crcTable,
	}

	// Create bloom filter
	bf := newBloomFilter(memtable.numKeys)
	sst.bloomFilter = bf

	// Create file
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", sst.level, sst.filenum)

	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sstFile, err := os.OpenFile(
		filename,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}
	defer sstFile.Close()

	lastSeq := uint64(0)
	curr := memtable.head.next[0]
	startKey := curr.key
	var endKey string

	// fileBuffer accumulates all blocks before the single final Write+Sync
	fileBuffer := new(bytes.Buffer)

	// currentBlockBuf accumulates entries for the current in-progress block
	currentBlockBuf := new(bytes.Buffer)
	currentBlockOffset := uint64(0)

	// prevBlockKey tracks the lastKey of the previous block for index binary search
	prevBlockKey := ""

	idx := make(index, 0)

	flushBlock := func() {
		fileBuffer.Write(currentBlockBuf.Bytes())

		idx = append(idx, newBlock(
			endKey,
			currentBlockOffset,
			uint64(currentBlockBuf.Len()),
			prevBlockKey,
		))

		prevBlockKey = endKey
		currentBlockOffset += uint64(currentBlockBuf.Len())
		currentBlockBuf = new(bytes.Buffer)
	}

	for curr != nil {
		// Add key to bloom filter
		sst.bloomFilter.setBloomBits(curr.key)

		// Format: seq(8) tombstone(1) keyLength(4) key valueLength(4) value checksum(4)
		entryBuf := new(bytes.Buffer)

		// Write: seq (8 bytes)
		binary.Write(entryBuf, binary.LittleEndian, curr.seq)

		// Write: tombstone (1 byte)
		if curr.tombstone {
			entryBuf.WriteByte(1)
		} else {
			entryBuf.WriteByte(0)
		}

		// Write: key length (4 bytes) + key
		keyBytes := []byte(curr.key)
		binary.Write(entryBuf, binary.LittleEndian, uint32(len(keyBytes)))
		entryBuf.Write(keyBytes)

		// Write: value length (4 bytes) + value
		binary.Write(entryBuf, binary.LittleEndian, uint32(len(curr.value)))
		entryBuf.Write(curr.value)

		// Write: checksum (4 bytes) over the entry bytes written so far
		checksum := crc32.Checksum(entryBuf.Bytes(), crcTable)
		binary.Write(entryBuf, binary.LittleEndian, checksum)

		currentBlockBuf.Write(entryBuf.Bytes())

		lastSeq = max(lastSeq, curr.seq)
		endKey = curr.key
		curr = curr.next[0]

		// Flush full block to fileBuffer and record its index entry
		if currentBlockBuf.Len() >= TARGET_BLOCK_SIZE {
			flushBlock()
		}
	}

	// Flush the final partial block (if any entries remain)
	if currentBlockBuf.Len() > 0 {
		flushBlock()
	}

	sst.index = &idx

	// --- Index section ---
	// Format per entry: keyLength(4) key offset(8) length(8)
	indexOffset := uint64(fileBuffer.Len())
	for _, block := range idx {
		keyBytes := []byte(block.lastKey)
		binary.Write(fileBuffer, binary.LittleEndian, uint32(len(keyBytes)))
		fileBuffer.Write(keyBytes)
		binary.Write(fileBuffer, binary.LittleEndian, block.offset)
		binary.Write(fileBuffer, binary.LittleEndian, block.length)
	}
	indexLength := uint64(fileBuffer.Len()) - indexOffset

	// --- Bloom filter section ---
	bloomOffset := uint64(fileBuffer.Len())
	fileBuffer.Write(bf.bitstring)
	bloomLength := uint64(fileBuffer.Len()) - bloomOffset

	// --- Footer section (40 bytes) ---
	// indexOffset(8) indexLength(8) bloomOffset(8) bloomLength(8) magic(8)
	binary.Write(fileBuffer, binary.LittleEndian, indexOffset)
	binary.Write(fileBuffer, binary.LittleEndian, indexLength)
	binary.Write(fileBuffer, binary.LittleEndian, bloomOffset)
	binary.Write(fileBuffer, binary.LittleEndian, bloomLength)
	binary.Write(fileBuffer, binary.LittleEndian, uint64(FOOTER_MAGIC))

	// Write entire SSTable to disk in one call, then sync once
	if _, err := sstFile.Write(fileBuffer.Bytes()); err != nil {
		sstFile.Close()
		os.Remove(filename)
		return nil, err
	}

	if err := sstFile.Sync(); err != nil {
		sstFile.Close()
		os.Remove(filename)
		return nil, err
	}

	sst.lastSeq = max(sst.lastSeq, lastSeq)
	sst.startKey = startKey
	sst.endKey = endKey
	sst.sizeBytes = indexOffset

	return sst, nil
}

// search looks for a key within the specific SSTable.
//
// Parameters:
//   - key (string): The key to search for.
//
// Returns:
//   - e (*entry): A pointer to the entry if found.
//   - err (error): An error if the key is not found or a read error occurs.
//
// Errors:
//   - ErrKeyNotFound: Thrown if the key is not in the bloom filter or not found in the block.
//   - Throws errors if there are issues opening the file or reading the block from disk.
func (sst *sst) search(key string) (e *entry, err error) {
	// Check bloom filter
	if sst.bloomFilter.keyNotPresent(key) {
		return nil, ErrKeyNotFound
	}

	// Find data block in SST from index
	blockOffset, blockLength, err := sst.index.getDatablock(key)
	if err != nil {
		return nil, err
	}

	// Read from file
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", sst.level, sst.filenum)

	sstFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer sstFile.Close()

	r := io.NewSectionReader(sstFile, int64(blockOffset), int64(blockLength))
	for {
		e, err := readEntry(r, sst.crcTable)
		if err == ErrEntryNotFound {
			break
		} else if err != nil {
			return nil, err
		}

		if key == e.key {
			return e, nil
		}
	}

	return nil, ErrKeyNotFound
}

/* ====================================================================================
	COMPACTION WRITER
==================================================================================== */

// compactionWriter holds all mutable state for writing a single output SST during compaction
// compactionWriter represents a temporary writer used during the compaction process.
// It is used in the storage engine to construct a new SSTable file, its index, and bloom filter as entries are merged.
type compactionWriter struct {
	sst      *sst
	outFile  *os.File
	filename string

	fileBuffer         *bytes.Buffer
	currentBlockBuf    *bytes.Buffer
	currentBlockOffset uint64
	prevBlockKey       string

	idx      index
	bf       *bloomFilter
	startKey string
	endKey   string
	lastSeq  uint64
	numKeys  uint64
}

// newCompactionWriter creates a writer to build a new SSTable during compaction.
//
// Parameters:
//   - s (*sst): The SSTable metadata to write into.
//
// Returns:
//   - *compactionWriter: A pointer to the initialized compaction writer.
//   - error: An error if file creation fails.
//
// Errors:
//   - Throws errors if directory creation or file opening fails.
func newCompactionWriter(s *sst) (*compactionWriter, error) {
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", s.level, s.filenum)
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &compactionWriter{
		sst:             s,
		outFile:         f,
		filename:        filename,
		fileBuffer:      new(bytes.Buffer),
		currentBlockBuf: new(bytes.Buffer),
		idx:             make(index, 0),
		bf:              newBloomFilter(0),
	}, nil
}

// flushBlock writes the current in-memory block to the file buffer and adds it to the index.
//
// Parameters:
//   - None
//
// Returns:
//   - None
//
// Errors:
//   - None
func (cw *compactionWriter) flushBlock() {
	cw.fileBuffer.Write(cw.currentBlockBuf.Bytes())
	cw.idx = append(cw.idx, newBlock(
		cw.endKey,
		cw.currentBlockOffset,
		uint64(cw.currentBlockBuf.Len()),
		cw.prevBlockKey,
	))
	cw.prevBlockKey = cw.endKey
	cw.currentBlockOffset += uint64(cw.currentBlockBuf.Len())
	cw.currentBlockBuf = new(bytes.Buffer)
}

// writeEntry appends a single entry to the current block buffer.
//
// Parameters:
//   - e (*entry): The entry to write.
//
// Returns:
//   - None
//
// Errors:
//   - None
func (cw *compactionWriter) writeEntry(e *entry) {
	cw.bf.setBloomBits(e.key)

	entryBuf := new(bytes.Buffer)
	binary.Write(entryBuf, binary.LittleEndian, e.seq)
	if e.tombstone {
		entryBuf.WriteByte(1)
	} else {
		entryBuf.WriteByte(0)
	}
	keyBytes := []byte(e.key)
	binary.Write(entryBuf, binary.LittleEndian, uint32(len(keyBytes)))
	entryBuf.Write(keyBytes)
	binary.Write(entryBuf, binary.LittleEndian, uint32(len(e.value)))
	entryBuf.Write(e.value)
	checksum := crc32.Checksum(entryBuf.Bytes(), cw.sst.crcTable)
	binary.Write(entryBuf, binary.LittleEndian, checksum)

	cw.currentBlockBuf.Write(entryBuf.Bytes())

	if cw.numKeys == 0 {
		cw.startKey = e.key
	}
	cw.endKey = e.key
	cw.lastSeq = max(cw.lastSeq, e.seq)
	cw.numKeys++

	if cw.currentBlockBuf.Len() >= TARGET_BLOCK_SIZE {
		cw.flushBlock()
	}
}

// finalize flushes remaining data, writes index/footer, and closes the output SSTable file.
//
// Parameters:
//   - None
//
// Returns:
//   - error: An error if writing or syncing to disk fails.
//
// Errors:
//   - Throws errors if the file Write, Sync, or Close operations fail.
func (cw *compactionWriter) finalize() error {
	if cw.currentBlockBuf.Len() > 0 {
		cw.flushBlock()
	}

	// Update sst metadata
	cw.sst.bloomFilter = cw.bf
	cw.sst.index = &cw.idx
	cw.sst.startKey = cw.startKey
	cw.sst.endKey = cw.endKey
	cw.sst.lastSeq = max(cw.sst.lastSeq, cw.lastSeq)
	cw.sst.sizeBytes = uint64(cw.fileBuffer.Len()) // data bytes before index/bloom/footer

	// Index section: keyLen(4) key offset(8) length(8) per block
	indexOffset := uint64(cw.fileBuffer.Len())
	for _, block := range cw.idx {
		keyBytes := []byte(block.lastKey)
		binary.Write(cw.fileBuffer, binary.LittleEndian, uint32(len(keyBytes)))
		cw.fileBuffer.Write(keyBytes)
		binary.Write(cw.fileBuffer, binary.LittleEndian, block.offset)
		binary.Write(cw.fileBuffer, binary.LittleEndian, block.length)
	}
	indexLength := uint64(cw.fileBuffer.Len()) - indexOffset

	// Bloom filter section
	bloomOffset := uint64(cw.fileBuffer.Len())
	cw.fileBuffer.Write(cw.bf.bitstring)
	bloomLength := uint64(cw.fileBuffer.Len()) - bloomOffset

	// Footer: indexOffset(8) indexLength(8) bloomOffset(8) bloomLength(8) magic(8)
	binary.Write(cw.fileBuffer, binary.LittleEndian, indexOffset)
	binary.Write(cw.fileBuffer, binary.LittleEndian, indexLength)
	binary.Write(cw.fileBuffer, binary.LittleEndian, bloomOffset)
	binary.Write(cw.fileBuffer, binary.LittleEndian, bloomLength)
	binary.Write(cw.fileBuffer, binary.LittleEndian, uint64(FOOTER_MAGIC))

	if _, err := cw.outFile.Write(cw.fileBuffer.Bytes()); err != nil {
		return err
	}
	if err := cw.outFile.Sync(); err != nil {
		return err
	}
	return cw.outFile.Close()
}

// abort cancels the compaction process, closes the file, and deletes the incomplete file.
//
// Parameters:
//   - None
//
// Returns:
//   - None
//
// Errors:
//   - None
func (cw *compactionWriter) abort() {
	cw.outFile.Close()
	os.Remove(cw.filename)
}

/* ====================================================================================
	COMPACTION SOURCE
==================================================================================== */

// compactionSrc implements a 2-pointer merge between:
//   - Stream A: the single source SST from the level being compacted
//   - Stream B: the ordered list of overlapping SSTs from the next level
//
// Because SSTs within the same level never overlap, at most one entry from each
// stream can be the current minimum — no k-way scan is needed.
// compactionSrc represents a source iterator for merging two streams of entries during compaction.
// It is used in the storage engine to efficiently merge a source SSTable with overlapping SSTables from the next level.
type compactionSrc struct {
	// Stream A: source SST from lvlIdx
	srcSst  *sst
	srcFile *os.File
	srcRdr  *io.SectionReader
	srcHead *entry // nil = Stream A exhausted

	// Stream B: overlapping SSTs from lvlIdx+1 (sorted by startKey)
	overlapping []*sst
	overlapIdx  int
	overlapFile *os.File
	overlapRdr  *io.SectionReader
	overlapHead *entry // nil = current overlap SST exhausted

	pending  *entry // result of the most recent advance() call
	crcTable *crc32.Table
}

// newCompactionSrc creates a source iterator to merge an SSTable with overlapping SSTables.
//
// Parameters:
//   - srcSst (*sst): The primary source SSTable.
//   - overlapping ([]*sst): A list of overlapping SSTables from the next level.
//   - crcTable (*crc32.Table): The CRC table used for checksum verification.
//
// Returns:
//   - *compactionSrc: A pointer to the newly created compaction source.
//   - error: An error if opening or reading the source files fails.
//
// Errors:
//   - Throws errors if opening files or reading the initial entries from disk fails.
func newCompactionSrc(srcSst *sst, overlapping []*sst, crcTable *crc32.Table) (*compactionSrc, error) {
	src := &compactionSrc{
		srcSst:      srcSst,
		overlapping: overlapping,
		crcTable:    crcTable,
	}

	// Acquire per-SST read locks for all source files
	srcSst.mu.RLock()
	for _, s := range overlapping {
		s.mu.RLock()
	}

	// Open Stream A
	srcFilename := fmt.Sprintf("data/sstables/level-%d/%d.sst", srcSst.level, srcSst.filenum)
	f, err := os.Open(srcFilename)
	if err != nil {
		srcSst.mu.RUnlock()
		for _, s := range overlapping {
			s.mu.RUnlock()
		}
		return nil, err
	}

	src.srcFile = f
	lastBlk := (*srcSst.index)[len(*srcSst.index)-1]
	src.srcRdr = io.NewSectionReader(f, 0, int64(lastBlk.offset+lastBlk.length))
	src.srcHead, err = readEntry(src.srcRdr, crcTable)

	if err == ErrEntryNotFound {
		src.srcHead = nil
	} else if err != nil {
		src.srcFile.Close()
		srcSst.mu.RUnlock()
		for _, s := range overlapping {
			s.mu.RUnlock()
		}
		return nil, err
	}

	// Open Stream B (first overlapping SST, if any)
	if len(overlapping) > 0 {
		if err := src.openOverlap(0); err != nil {
			src.srcFile.Close()
			srcSst.mu.RUnlock()
			for _, s := range overlapping {
				s.mu.RUnlock()
			}
			return nil, err
		}
	}

	return src, nil
}

// openOverlap opens the overlapping SST at index i and pre-fetches its first entry.
//
// Parameters:
//   - i (int): The index of the overlapping SSTable in the list.
//
// Returns:
//   - error: An error if file opening or reading fails.
//
// Errors:
//   - Throws errors if the overlapping file cannot be opened or if reading the first entry fails.
func (src *compactionSrc) openOverlap(i int) error {
	if src.overlapFile != nil {
		src.overlapFile.Close()
		src.overlapFile = nil
	}

	s := src.overlapping[i]
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", s.level, s.filenum)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	src.overlapFile = f
	src.overlapIdx = i

	lastBlk := (*s.index)[len(*s.index)-1]
	src.overlapRdr = io.NewSectionReader(f, 0, int64(lastBlk.offset+lastBlk.length))

	e, err := readEntry(src.overlapRdr, src.crcTable)
	if err == ErrEntryNotFound {
		src.overlapHead = nil
	} else if err != nil {
		return err
	} else {
		src.overlapHead = e
	}
	return nil
}

// nextOverlapEntry reads the next entry from the current overlap SST.
// When it is exhausted, it transparently advances to the next overlap SST.
// Returns nil when all overlap SSTs are exhausted.
//
// Parameters:
//   - None
//
// Returns:
//   - *entry: The next entry from the overlapping SSTables, or nil if none are left.
//
// Errors:
//   - None (errors during file reading may lead to exhausting the current overlap prematurely).
func (src *compactionSrc) nextOverlapEntry() *entry {
	e, err := readEntry(src.overlapRdr, src.crcTable)
	if err == nil {
		return e
	}
	// Current overlap SST exhausted — open the next one
	for next := src.overlapIdx + 1; next < len(src.overlapping); next++ {
		if err := src.openOverlap(next); err != nil {
			return nil
		}
		// openOverlap already set overlapHead to the first entry.
		// Return that entry and load the following one into overlapHead.
		if src.overlapHead != nil {
			first := src.overlapHead
			next, err := readEntry(src.overlapRdr, src.crcTable)
			if err != nil {
				src.overlapHead = nil
			} else {
				src.overlapHead = next
			}
			return first
		}
	}
	return nil
}

// advance selects the next entry to write via 2-pointer comparison.
// Sets src.pending and returns ErrCompactionDone when both streams are exhausted.
//
// Parameters:
//   - None
//
// Returns:
//   - error: ErrCompactionDone if both source streams are completely exhausted.
//
// Errors:
//   - ErrCompactionDone: Thrown to indicate no more entries are left to process.
func (src *compactionSrc) advance() error {
	a := src.srcHead
	b := src.overlapHead

	if a == nil && b == nil {
		src.pending = nil
		return ErrCompactionDone
	}

	if b == nil || (a != nil && a.key < b.key) {
		// Stream A wins
		src.pending = a
		e, err := readEntry(src.srcRdr, src.crcTable)
		if err != nil {
			src.srcHead = nil
		} else {
			src.srcHead = e
		}
	} else if a == nil || b.key < a.key {
		// Stream B wins
		src.pending = b
		src.overlapHead = src.nextOverlapEntry()
	} else {
		// Key tie: keep higher seq, discard the other
		if a.seq >= b.seq {
			src.pending = a
			// Advance Stream A
			e, err := readEntry(src.srcRdr, src.crcTable)
			if err != nil {
				src.srcHead = nil
			} else {
				src.srcHead = e
			}
			// Discard Stream B's duplicate
			src.overlapHead = src.nextOverlapEntry()
		} else {
			src.pending = b
			// Advance Stream B
			src.overlapHead = src.nextOverlapEntry()
			// Discard Stream A's duplicate
			e, err := readEntry(src.srcRdr, src.crcTable)
			if err != nil {
				src.srcHead = nil
			} else {
				src.srcHead = e
			}
		}
	}

	return nil
}

// close closes open file descriptors and releases read locks on the SSTables used for compaction.
//
// Parameters:
//   - None
//
// Returns:
//   - None
//
// Errors:
//   - None
func (src *compactionSrc) close() {
	if src.srcFile != nil {
		src.srcFile.Close()
	}
	if src.overlapFile != nil {
		src.overlapFile.Close()
	}
	// Release per-SST read locks now that all file I/O is done.
	// This allows a waiting Compact(delete phase) sst.mu.Lock() to proceed.
	src.srcSst.mu.RUnlock()
	for _, s := range src.overlapping {
		s.mu.RUnlock()
	}
}

// compact merges Stream A (srcSst) and Stream B (overlapping SSTs) into newSst.
// isLastLevel controls tombstone pruning: tombstones are dropped at the last level
// since there are no deeper levels to shadow.
//
// Parameters:
//   - src (*compactionSrc): The compaction source iterator providing sorted entries.
//   - isLastLevel (bool): Indicates if the compaction is merging into the last level (for tombstone pruning).
//
// Returns:
//   - error: ErrCompactionDone, ErrCompactionFull, or an I/O error.
//
// Errors:
//   - ErrCompactionDone: Thrown when all source entries are successfully written.
//   - ErrCompactionFull: Thrown when the new SSTable reaches its maximum capacity.
//   - Throws other errors for file I/O or processing failures.
func (newSst *sst) compact(src *compactionSrc, isLastLevel bool) error {
	cw, err := newCompactionWriter(newSst)
	if err != nil {
		return err
	}

	for {
		// Check capacity before writing the next entry
		written := uint64(cw.fileBuffer.Len()) + uint64(cw.currentBlockBuf.Len())
		if written >= newSst.capacity {
			if err := cw.finalize(); err != nil {
				cw.abort()
				return err
			}
			return ErrCompactionFull
		}

		if err := src.advance(); err == ErrCompactionDone {
			if err2 := cw.finalize(); err2 != nil {
				cw.abort()
				return err2
			}
			return ErrCompactionDone
		} else if err != nil {
			cw.abort()
			return err
		}

		// Tombstone pruning: at the last level there are no deeper levels to
		// shadow, so a tombstone entry serves no purpose and can be dropped.
		if isLastLevel && src.pending.tombstone {
			continue
		}

		cw.writeEntry(src.pending)
	}
}

// block represents metadata about a data block within an SSTable.
// It is used in the SSTable index to keep track of block offsets, lengths, and key ranges for binary searching.
type block struct {
	lastKey      string
	offset       uint64
	length       uint64
	prevBlockKey string
}
type index []*block

// newBlock creates a block metadata struct for an SSTable index.
//
// Parameters:
//   - lastKey (string): The highest key present in this block.
//   - offset (uint64): The file offset where this block starts.
//   - length (uint64): The length of this block in bytes.
//   - prevBlockKey (string): The highest key in the immediately preceding block.
//
// Returns:
//   - *block: A pointer to the newly created block metadata.
//
// Errors:
//   - None
func newBlock(lastKey string, offset uint64, length uint64, prevBlockKey string) *block {
	return &block{
		lastKey:      lastKey,
		offset:       offset,
		length:       length,
		prevBlockKey: prevBlockKey,
	}
}

// getDatablock searches the index to find the offset and length of the block containing the key.
//
// Parameters:
//   - key (string): The key to locate in the index.
//
// Returns:
//   - offset (uint64): The starting byte offset of the block.
//   - length (uint64): The length in bytes of the block.
//   - err (error): An error if the key cannot possibly be in any block based on the index.
//
// Errors:
//   - ErrKeyNotFound: Thrown when the given key is outside the bounds of all blocks in the index.
func (index *index) getDatablock(key string) (offset uint64, length uint64, err error) {
	l := 0
	r := len(*index) - 1

	for l <= r {
		m := (r + l) / 2

		if key <= (*index)[m].lastKey && key > (*index)[m].prevBlockKey {
			return (*index)[m].offset, (*index)[m].length, nil
		} else if key > (*index)[m].lastKey {
			l = m + 1
		} else {
			r = m - 1
		}
	}

	return 0, 0, ErrKeyNotFound
}

// readFooter reads the footer, index, and bloom filter from an open SST file.
// The file offset is not assumed; the function seeks to the footer itself.
//
// Parameters:
//   - sstFile (*os.File): An open file descriptor to the SSTable file.
//
// Returns:
//   - idx (*index): A pointer to the parsed index list.
//   - bf (*bloomFilter): A pointer to the parsed bloom filter.
//   - indexOffset (uint64): The file offset where the index begins (and data blocks end).
//   - err (error): An error if reading or parsing the footer fails.
//
// Errors:
//   - ErrBadFile: Thrown if the magic number at the end of the file is incorrect.
//   - Throws errors if seeking or reading data from the file fails.
func readFooter(sstFile *os.File) (idx *index, bf *bloomFilter, indexOffset uint64, err error) {
	_, err = sstFile.Seek(-FOOTER_SIZE, io.SeekEnd)
	if err != nil {
		return nil, nil, 0, err
	}

	buf := make([]byte, FOOTER_SIZE)
	_, err = io.ReadFull(sstFile, buf)
	if err != nil {
		return nil, nil, 0, err
	}

	// Verify magic
	magic := binary.LittleEndian.Uint64(buf[32:40])
	if magic != FOOTER_MAGIC {
		return nil, nil, 0, ErrBadFile
	}

	// Instantiate index
	indexOffset = binary.LittleEndian.Uint64(buf[0:8])
	indexLength := binary.LittleEndian.Uint64(buf[8:16])

	slice := make(index, 0)
	idx = &slice
	r := io.NewSectionReader(sstFile, int64(indexOffset), int64(indexLength))

	prevBlockKey := ""
	for {
		var keyLength uint32
		err = binary.Read(r, binary.LittleEndian, &keyLength)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, 0, err
		}

		keyBuf := make([]byte, keyLength)
		_, err = io.ReadFull(r, keyBuf)
		if err != nil {
			return nil, nil, 0, err
		}
		lastKey := string(keyBuf)

		var offset uint64
		err = binary.Read(r, binary.LittleEndian, &offset)
		if err != nil {
			return nil, nil, 0, err
		}

		var length uint64
		err = binary.Read(r, binary.LittleEndian, &length)
		if err != nil {
			return nil, nil, 0, err
		}

		block := newBlock(lastKey, offset, length, prevBlockKey)
		*idx = append(*idx, block)
		prevBlockKey = lastKey
	}

	// Instantiate bloom filter
	bloomOffset := binary.LittleEndian.Uint64(buf[16:24])
	bloomLength := binary.LittleEndian.Uint64(buf[24:32])

	bfBuf := make([]byte, bloomLength)
	_, err = sstFile.ReadAt(bfBuf, int64(bloomOffset))
	if err != nil {
		return nil, nil, 0, err
	}

	bf = newBloomFilter(bloomLength)
	bf.bitstring = bfBuf

	return idx, bf, indexOffset, nil
}

// replaySst opens an SST file on disk and reconstructs the in-memory sst struct
// without reading any data blocks.
//
// Parameters:
//   - fpath (string): The file path of the SSTable.
//   - crcTable (*crc32.Table): The CRC table for checksums.
//
// Returns:
//   - *sst: A pointer to the reconstructed SSTable struct.
//   - error: An error if parsing or reading the file fails.
//
// Errors:
//   - Throws errors if the file path cannot be parsed, opening the file fails, reading the footer fails, or the index is empty.
func replaySst(fpath string, crcTable *crc32.Table) (*sst, error) {
	// --- Parse level and filenum from fpath ---
	// Expected format: "data/sstables/level-N/F.sst"
	var lvl int
	var filenum uint64
	_, err := fmt.Sscanf(
		filepath.Base(filepath.Dir(fpath))+"/"+filepath.Base(fpath),
		"level-%d/%d.sst",
		&lvl, &filenum,
	)
	if err != nil {
		return nil, fmt.Errorf("replaySst: could not parse level/filenum from path %q: %w", fpath, err)
	}

	// --- Open file ---
	sstFile, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer sstFile.Close()

	// --- Read footer ---
	idx, bf, indexOffset, err := readFooter(sstFile)
	if err != nil {
		return nil, fmt.Errorf("replaySst %q: %w", fpath, err)
	}
	if len(*idx) == 0 {
		return nil, fmt.Errorf("replaySst %q: index is empty", fpath)
	}

	// --- Derive startKey and endKey from index ---
	firstBlock := (*idx)[0]
	lastBlock := (*idx)[len(*idx)-1]
	endKey := lastBlock.lastKey

	// Scan first block to find its first key (= overall startKey)
	firstBlockRdr := io.NewSectionReader(sstFile, int64(firstBlock.offset), int64(firstBlock.length))
	firstEntry, err := readEntry(firstBlockRdr, crcTable)
	if err != nil {
		return nil, fmt.Errorf("replaySst %q: could not read first entry: %w", fpath, err)
	}
	startKey := firstEntry.key

	// --- Derive lastSeq by scanning all data blocks ---
	lastSeq := uint64(0)
	for _, blk := range *idx {
		r := io.NewSectionReader(sstFile, int64(blk.offset), int64(blk.length))
		for {
			e, err := readEntry(r, crcTable)
			if err == ErrEntryNotFound {
				break
			} else if err != nil {
				return nil, fmt.Errorf("replaySst %q: error scanning data block: %w", fpath, err)
			}
			if e.seq > lastSeq {
				lastSeq = e.seq
			}
		}
	}

	return &sst{
		filenum:     filenum,
		level:       lvl,
		lastSeq:     lastSeq,
		startKey:    startKey,
		endKey:      endKey,
		capacity:    0, // unknown at replay time; not needed for read path
		sizeBytes:   indexOffset,
		index:       idx,
		bloomFilter: bf,
		crcTable:    crcTable,
	}, nil
}

// rebuildSstables scans the data/sstables/ directory tree and reconstructs the
// full in-memory sstables state from every SST file found on disk.
//
// Parameters:
//   - l0Capacity (uint64): Capacity in bytes for Level 0.
//   - growthFactor (int): Multiplier for capacity of subsequent levels.
//   - crcTable (*crc32.Table): The CRC table for checksums.
//
// Returns:
//   - ssts (*sstables): The rebuilt sstables instance.
//   - maxSeq (uint64): The highest sequence number seen across all SSTables.
//   - maxFilenum (uint64): The highest file number seen.
//   - err (error): Any I/O or parse error encountered during rebuild.
//
// Errors:
//   - Throws errors if reading directories fails or if parsing any individual SSTable file fails.
func rebuildSstables(l0Capacity uint64, growthFactor int, crcTable *crc32.Table) (ssts *sstables, maxSeq uint64, maxFilenum uint64, err error) {
	ssts = newSstables(l0Capacity, growthFactor, crcTable)

	// Determine max level by scanning data/sstables/
	entries, err := os.ReadDir("data/sstables")
	maxLvl := -1
	if err == nil {
		for _, de := range entries {
			if de.IsDir() && strings.HasPrefix(de.Name(), "level-") {
				if lvl, err := strconv.Atoi(strings.TrimPrefix(de.Name(), "level-")); err == nil {
					if lvl > maxLvl {
						maxLvl = lvl
					}
				}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, 0, 0, fmt.Errorf("rebuildSstables: reading data/sstables: %w", err)
	}

	// Iterate from 0 to maxLvl
	for lvl := 0; lvl <= maxLvl; lvl++ {
		dir := fmt.Sprintf("data/sstables/level-%d", lvl)

		lvlEntries, err := os.ReadDir(dir)
		if os.IsNotExist(err) { // Skip missing directories
			continue
		} else if err != nil {
			return nil, 0, 0, fmt.Errorf("rebuildSstables: reading %s: %w", dir, err)
		}

		// Ensure the levels slice is large enough for this level.
		for len(ssts.levels) <= lvl {
			// Each successive level has growthFactor * the previous level's capacity.
			prevCapacity := ssts.levels[len(ssts.levels)-1].capacityBytes
			ssts.levels = append(ssts.levels, newLevel(prevCapacity*uint64(growthFactor)))
		}

		lvlObj := ssts.levels[lvl]

		for _, de := range lvlEntries {
			if de.IsDir() || filepath.Ext(de.Name()) != ".sst" {
				continue
			}

			fpath := filepath.Join(dir, de.Name())
			s, err := replaySst(fpath, crcTable)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("rebuildSstables: %w", err)
			}

			lvlObj.sstList = append(lvlObj.sstList, s)
			lvlObj.sizeBytes += s.sizeBytes

			if s.lastSeq > maxSeq {
				maxSeq = s.lastSeq
			}
			if s.filenum > maxFilenum {
				maxFilenum = s.filenum
			}
		}

		lvlObj.sortByStartKey()
	}

	return ssts, maxSeq, maxFilenum, nil
}

// entry represents a single deserialized key-value pair read from an SSTable.
// It is used internally when reading from SSTables during searches, replays, and compactions.
type entry struct {
	seq       uint64
	tombstone bool
	key       string
	value     []byte
}

// readEntry parses a single key-value entry from a section of a file.
//
// Parameters:
//   - r (*io.SectionReader): The reader positioned at the start of an entry.
//   - crcTable (*crc32.Table): The CRC table used to verify the entry's checksum.
//
// Returns:
//   - *entry: A pointer to the parsed entry.
//   - error: An error if reading fails, EOF is reached, or the checksum is invalid.
//
// Errors:
//   - ErrEntryNotFound: Thrown when EOF is encountered immediately when trying to read.
//   - ErrBadData: Thrown when the calculated checksum does not match the stored checksum.
//   - Throws other I/O errors if reading from the reader fails.
func readEntry(r *io.SectionReader, crcTable *crc32.Table) (*entry, error) {
	buf := new(bytes.Buffer)

	// Read: seq (8 bytes)
	var seq uint64

	err := binary.Read(r, binary.LittleEndian, &seq)
	if err == io.EOF {
		return nil, ErrEntryNotFound
	} else if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, seq)
	if err != nil {
		return nil, err
	}

	// Read: tombstone (1 byte)
	var tombstone byte

	err = binary.Read(r, binary.LittleEndian, &tombstone)
	if err != nil {
		return nil, err
	}

	err = buf.WriteByte(tombstone)
	if err != nil {
		return nil, err
	}

	// Read: key length (4 bytes)
	var keyLength uint32

	err = binary.Read(r, binary.LittleEndian, &keyLength)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, keyLength)
	if err != nil {
		return nil, err
	}

	// Read: key
	keyBuf := make([]byte, keyLength)
	_, err = io.ReadFull(r, keyBuf)
	if err != nil {
		return nil, err
	}
	buf.Write(keyBuf)
	key := string(keyBuf)

	// Read: value length (4 bytes)
	var valLength uint32

	err = binary.Read(r, binary.LittleEndian, &valLength)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.LittleEndian, valLength)
	if err != nil {
		return nil, err
	}

	// Read: value
	value := make([]byte, valLength)
	_, err = io.ReadFull(r, value)
	if err != nil {
		return nil, err
	}
	buf.Write(value)

	// Read: checksum (4 bytes)
	var checksum uint32

	err = binary.Read(r, binary.LittleEndian, &checksum)
	if err != nil {
		return nil, err
	}

	expected := crc32.Checksum(buf.Bytes(), crcTable)
	if checksum != expected {
		return nil, ErrBadData
	}

	return &entry{
		seq:       seq,
		tombstone: tombstone == 1,
		key:       key,
		value:     value,
	}, nil
}
