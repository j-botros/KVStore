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
)

/* ====================================================================================
	SSTABLES CLASS
==================================================================================== */

type level struct {
	sstList       []*sst
	capacityBytes uint64
	sizeBytes     uint64
}

func newLevel(capacityBytes uint64) *level {
	return &level{
		sstList:       make([]*sst, 0),
		capacityBytes: capacityBytes,
		sizeBytes:     0,
	}
}

// Use this when adding a single SST during compaction to keep L1+ sorted.
func (l *level) insertSorted(s *sst) {
	i := sort.Search(len(l.sstList), func(i int) bool {
		return l.sstList[i].startKey >= s.startKey
	})
	l.sstList = append(l.sstList, nil)
	copy(l.sstList[i+1:], l.sstList[i:])
	l.sstList[i] = s
}

// Use this once after bulk-loading SSTs from disk on startup.
func (l *level) sortByStartKey() {
	sort.Slice(l.sstList, func(i, j int) bool {
		return l.sstList[i].startKey < l.sstList[j].startKey
	})
}

type sstables struct {
	levels       []*level
	l0Capacity   uint64
	growthFactor int
	crcTable     *crc32.Table
}

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

func (sstables *sstables) get(key string) (value []byte, err error) {
	// Level 0: SSTs may overlap, search all and pick highest seq
	seq := uint64(0)
	tombstone := false
	for _, sst := range sstables.levels[0].sstList {
		if key >= sst.startKey && key <= sst.endKey {
			entry, err := sst.search(key)

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
	}

	if tombstone {
		return nil, ErrKeyNotFound
	} else if value != nil {
		return value, nil
	}

	// Level 1+: SSTs are non-overlapping; first match wins
	for _, lvl := range sstables.levels[1:] {
		for _, sst := range lvl.sstList {
			if key >= sst.startKey && key <= sst.endKey {
				entry, err := sst.search(key)

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
		}
	}

	return nil, ErrKeyNotFound
}

func (sstables *sstables) flush(filenum uint64, memtable *memtable) error {
	newSst, err := newSstFromMemtable(filenum, memtable, sstables.crcTable)
	if err != nil {
		return err
	}

	l0 := sstables.levels[0]
	l0.sstList = append(l0.sstList, newSst)
	l0.sizeBytes += memtable.sizeBytes
	return nil
}

/* ====================================================================================
	SST CLASS & INDEX/BLOCK CLASSES
==================================================================================== */

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
}

const (
	// Used to validate SSTable
	FOOTER_MAGIC = uint64(0x4c55564c49414e41)
	// Index offset + Index length + Bloom offset + Bloom length + Footer magic
	FOOTER_SIZE = 8 + 8 + 8 + 8 + 8

	// Data block size (KB)
	TARGET_BLOCK_SIZE = 4 * 1024 // 4 KB
)

func newSst(filenum uint64, lvl int, crcTable *crc32.Table) *sst {
	sst := &sst{
		filenum:  filenum,
		level:    lvl,
		crcTable: crcTable,
	}

	return sst
}

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

		lastSeq = curr.seq
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

	sst.lastSeq = lastSeq
	sst.startKey = startKey
	sst.endKey = endKey

	return sst, nil
}

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

// compactionWriter holds all mutable state for writing a single output SST during
// compaction. Its methods are compiled once (not re-created as closures per call).
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
	cw.lastSeq = e.seq
	cw.numKeys++

	if cw.currentBlockBuf.Len() >= TARGET_BLOCK_SIZE {
		cw.flushBlock()
	}
}

func (cw *compactionWriter) finalize() error {
	if cw.currentBlockBuf.Len() > 0 {
		cw.flushBlock()
	}

	// Update sst metadata
	cw.sst.bloomFilter = cw.bf
	cw.sst.index = &cw.idx
	cw.sst.startKey = cw.startKey
	cw.sst.endKey = cw.endKey
	cw.sst.lastSeq = cw.lastSeq
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
type compactionSrc struct {
	// Stream A: source SST from lvlIdx
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

func newCompactionSrc(srcSst *sst, overlapping []*sst, crcTable *crc32.Table) (*compactionSrc, error) {
	src := &compactionSrc{
		overlapping: overlapping,
		crcTable:    crcTable,
	}

	// Open Stream A
	srcFilename := fmt.Sprintf("data/sstables/level-%d/%d.sst", srcSst.level, srcSst.filenum)
	f, err := os.Open(srcFilename)
	if err != nil {
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
		return nil, err
	}

	// Open Stream B (first overlapping SST, if any)
	if len(overlapping) > 0 {
		if err := src.openOverlap(0); err != nil {
			src.srcFile.Close()
			return nil, err
		}
	}

	return src, nil
}

// openOverlap opens the overlapping SST at index i and pre-fetches its first entry.
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

func (src *compactionSrc) close() {
	if src.srcFile != nil {
		src.srcFile.Close()
	}
	if src.overlapFile != nil {
		src.overlapFile.Close()
	}
}

// compact merges Stream A (srcSst) and Stream B (overlapping SSTs) into newSst.
// isLastLevel controls tombstone pruning: tombstones are dropped at the last level
// since there are no deeper levels to shadow.
//
// Returns:
//   - ErrCompactionDone  — all source entries written; newSst is finalized on disk.
//   - ErrCompactionFull  — newSst reached capacity; finalized on disk, sources remain.
//   - other error        — I/O or data error; partially-written file is removed.
func (newSst *sst) compact(src *compactionSrc, isLastLevel bool) error {
	cw, err := newCompactionWriter(newSst)
	if err != nil {
		return err
	}

	for {
		// Check capacity before writing the next entry
		written := uint64(cw.fileBuffer.Len()) + uint64(cw.currentBlockBuf.Len())
		if newSst.capacity > 0 && written >= newSst.capacity {
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



type block struct {
	lastKey      string
	offset       uint64
	length       uint64
	prevBlockKey string
}
type index []*block

func newBlock(lastKey string, offset uint64, length uint64, prevBlockKey string) *block {
	return &block{
		lastKey:      lastKey,
		offset:       offset,
		length:       length,
		prevBlockKey: prevBlockKey,
	}
}

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

func (sst *sst) readFooter() (idx *index, bf *bloomFilter, err error) {
	// Read from file
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", sst.level, sst.filenum)

	sstFile, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer sstFile.Close()

	_, err = sstFile.Seek(-FOOTER_SIZE, io.SeekEnd)
	if err != nil {
		return nil, nil, err
	}

	buf := make([]byte, FOOTER_SIZE)
	_, err = io.ReadFull(sstFile, buf)
	if err != nil {
		return nil, nil, err
	}

	// Verify magic
	magic := binary.LittleEndian.Uint64(buf[32:40])
	if magic != FOOTER_MAGIC {
		return nil, nil, ErrBadFile
	}

	// Instantiate index
	indexOffset := binary.LittleEndian.Uint64(buf[0:8])
	indexLength := binary.LittleEndian.Uint64(buf[8:16])

	slice := make(index, 0)
	idx = &slice
	r := io.NewSectionReader(sstFile, int64(indexOffset), int64(indexLength))

	var prevBlockKey string
	prevBlockKey = ""
	for {
		var keyLength uint32
		err = binary.Read(r, binary.LittleEndian, &keyLength)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}

		keyBuf := make([]byte, keyLength)
		_, err = io.ReadFull(r, keyBuf)
		if err != nil {
			return nil, nil, err
		}
		lastKey := string(keyBuf)

		var offset uint64
		err = binary.Read(r, binary.LittleEndian, &offset)
		if err != nil {
			return nil, nil, err
		}

		var length uint64
		err = binary.Read(r, binary.LittleEndian, &length)
		if err != nil {
			return nil, nil, err
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
		return nil, nil, err
	}

	bf = newBloomFilter(bloomLength)
	bf.bitstring = bfBuf

	return idx, bf, nil
}

type entry struct {
	seq       uint64
	tombstone bool
	key       string
	value     []byte
}

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
