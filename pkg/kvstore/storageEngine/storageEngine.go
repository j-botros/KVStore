package storageengine

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// StorageEngine represents the main database engine handling all key-value operations.
// It is the primary structure used to coordinate writes to the WAL and memtable, reads from memtables and SSTables, and background compactions.
type StorageEngine struct {
	memCapacity uint64
	sstCapacity uint64 // fixed max data bytes per output SST
	crcTable    *crc32.Table

	nextFileNumber uint64
	nextSeq        uint64

	active     *memlog
	immutables []*memlog
	sstables   *sstables

	mu         sync.RWMutex // guards active, immutables, nextSeq, nextFileNumber
	compacting atomic.Bool  // serializes Compact; allows concurrent Flush calls
}

// newStorageEngine creates a fresh storage engine with empty data directories.
//
// Parameters:
//   - memCapacity (uint64): Maximum size of the memtable before flushing.
//   - sstCapacity (uint64): Maximum size of each SSTable.
//   - l0Capacity (uint64): Maximum capacity of Level 0 SSTables.
//   - growthFactor (int): The capacity multiplier for subsequent SSTable levels.
//
// Returns:
//   - *StorageEngine: A pointer to the newly initialized storage engine.
//
// Errors:
//   - None
func newStorageEngine(memCapacity uint64, sstCapacity uint64, l0Capacity uint64, growthFactor int) *StorageEngine {
	// Ensure data directories exist for a completely fresh start
	os.MkdirAll("data/wal", 0755)
	os.MkdirAll("data/sstables", 0755)

	crcTable := crc32.MakeTable(crc32.Castagnoli)

	nextFileNumber := uint64(0)
	ml := newMemlog(nextFileNumber, crcTable)
	nextFileNumber++

	return &StorageEngine{
		memCapacity: memCapacity,
		sstCapacity: sstCapacity,
		crcTable:    crcTable,

		nextFileNumber: nextFileNumber,
		nextSeq:        0,

		active:     ml,
		immutables: make([]*memlog, 0),
		sstables:   newSstables(l0Capacity, growthFactor, crcTable),

		// mu and compacting are ready to use with their zero values (unlocked and false)
	}
}

// OpenStorageEngine is the primary entry point for server startup. If a data/
// directory already exists on disk it calls rebuildEngine to recover prior
// state; otherwise it calls newStorageEngine for a clean start. The caller
// should always use this instead of newStorageEngine directly.
//
// Parameters:
//   - memCapacity (uint64): Maximum size of the memtable.
//   - sstCapacity (uint64): Maximum size of each SSTable.
//   - l0Capacity (uint64): Maximum capacity of Level 0.
//   - growthFactor (int): Level capacity multiplier.
//
// Returns:
//   - *StorageEngine: A pointer to the opened storage engine.
//   - error: An error if opening or recovering the engine fails.
//
// Errors:
//   - Throws errors if stating the data directory fails or if rebuilding the engine encounters an error.
func OpenStorageEngine(memCapacity uint64, sstCapacity uint64, l0Capacity uint64, growthFactor int) (*StorageEngine, error) {
	_, err := os.Stat("data")
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("OpenStorageEngine: stat data dir: %w", err)
	}

	if os.IsNotExist(err) {
		// First run — no prior state.
		return newStorageEngine(memCapacity, sstCapacity, l0Capacity, growthFactor), nil
	}

	// Existing data directory found — attempt recovery.
	return rebuildEngine(memCapacity, sstCapacity, l0Capacity, growthFactor)
}

// rebuildEngine recovers the state of the storage engine from existing SSTables and WAL files.
//
// Parameters:
//   - memCapacity (uint64): Maximum size of the memtable.
//   - sstCapacity (uint64): Maximum size of each SSTable.
//   - l0Capacity (uint64): Maximum capacity of Level 0.
//   - growthFactor (int): Level capacity multiplier.
//
// Returns:
//   - *StorageEngine: A pointer to the rebuilt storage engine.
//   - error: An error if reconstruction fails at any step.
//
// Errors:
//   - Throws errors if reading the WAL directory fails, rebuilding SSTables fails, or replaying WALs fails.
func rebuildEngine(memCapacity uint64, sstCapacity uint64, l0Capacity uint64, growthFactor int) (*StorageEngine, error) {
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	// Step 1: Recover SSTable state.
	ssts, maxSstSeq, maxSstFilenum, err := rebuildSstables(l0Capacity, growthFactor, crcTable)
	if err != nil {
		return nil, fmt.Errorf("rebuildEngine: rebuildSstables: %w", err)
	}

	// Step 2: Scan WAL directory for *.log files.
	walDir := "data/wal"
	des, err := os.ReadDir(walDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("rebuildEngine: reading %s: %w", walDir, err)
	}

	var logNums []uint64
	for _, de := range des {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".log") {
			continue
		}
		logNum, err := strconv.ParseUint(strings.TrimSuffix(de.Name(), ".log"), 10, 64)
		if err != nil {
			continue // skip files that don't match <N>.log
		}
		logNums = append(logNums, logNum)
	}

	// Sort from oldest to newest so immutables end up in the right order.
	sort.Slice(logNums, func(i, j int) bool { return logNums[i] < logNums[j] })

	// Step 3: Replay each WAL file.
	var memlogs []*memlog
	for _, logNum := range logNums {
		ml, err := replayWal(logNum, maxSstSeq, crcTable)
		if err != nil {
			return nil, fmt.Errorf("rebuildEngine: replayWal(%d): %w", logNum, err)
		}
		memlogs = append(memlogs, ml)
	}

	// Step 4: Assign active and immutables.
	//
	// If no WAL files exist (e.g. first run after SST-only state), create a
	// fresh active memlog whose logNumber is one past the highest seen filenum.
	var active *memlog
	var immutables []*memlog
	maxWalLogNum := uint64(0)
	maxWalSeq := uint64(0)

	if len(memlogs) == 0 {
		// No WALs on disk; start a brand-new active memlog.
		newLogNum := maxSstFilenum + 1
		active = newMemlog(newLogNum, crcTable)
		maxWalLogNum = newLogNum
	} else {
		// All but the last WAL are immutable; the last (newest) is active.
		immutables = memlogs[:len(memlogs)-1]
		active = memlogs[len(memlogs)-1]
		maxWalLogNum = logNums[len(logNums)-1]
		for _, ml := range memlogs {
			if ml.wal.lastSeq > maxWalSeq {
				maxWalSeq = ml.wal.lastSeq
			}
		}
	}

	// Step 5: Derive counters.
	//
	// nextSeq must be strictly greater than every seq seen on disk.
	// nextFileNumber must be strictly greater than every filenum seen on disk.
	nextSeq := max(maxSstSeq, maxWalSeq) + 1
	nextFileNumber := max(maxSstFilenum, maxWalLogNum) + 1

	if immutables == nil {
		immutables = make([]*memlog, 0)
	}

	return &StorageEngine{
		memCapacity: memCapacity,
		sstCapacity: sstCapacity,
		crcTable:    crcTable,

		nextFileNumber: nextFileNumber,
		nextSeq:        nextSeq,

		active:     active,
		immutables: immutables,
		sstables:   ssts,

		// mu and compacting are ready to use with their zero values
	}, nil
}

// memlog represents a paired in-memory store (memtable) and its corresponding Write-Ahead Log (WAL).
// It is used in the storage engine to manage the active mutable data and immutable data before it is flushed to disk.
type memlog struct {
	memtable *memtable
	wal      *wal
}

// newMemlog creates a pair containing a new memtable and its associated WAL.
//
// Parameters:
//   - logNumber (uint64): The log number assigned to the WAL file.
//   - crcTable (*crc32.Table): The CRC table used for WAL checksums.
//
// Returns:
//   - *memlog: A pointer to the newly created memlog struct.
//
// Errors:
//   - None
func newMemlog(logNumber uint64, crcTable *crc32.Table) *memlog {
	return &memlog{
		memtable: newMemtable(),
		wal:      newWal(logNumber, crcTable),
	}
}

/* ====================================================================================
	STORAGE (CRUD) METHODS
==================================================================================== */

// Get retrieves the value associated with a key from the storage engine.
//
// Parameters:
//   - key (string): The key to fetch.
//
// Returns:
//   - value ([]byte): The value associated with the key if found.
//   - err (error): An error if the key does not exist or a read failure occurs.
//
// Errors:
//   - ErrKeyNotFound: Thrown if the key is not in the active memtable, immutables, or SSTables.
//   - Throws other errors from underlying I/O reads.
func (e *StorageEngine) Get(key string) (value []byte, err error) {
	// Concurrent Reads allowed; No Writes during Reads
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Search active Memtable for key-value
	value, err = e.active.memtable.get(key)
	if err == nil {
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Search immutable Memtables from newest to oldest
	for i := len(e.immutables) - 1; i >= 0; i-- {
		value, err = e.immutables[i].memtable.get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFound {
			return nil, err
		}
	}

	// Search SSTables for key-value
	value, err = e.sstables.get(key)
	return value, err
}

// Put inserts or updates a key-value pair in the storage engine.
//
// Parameters:
//   - key (string): The key to insert.
//   - value ([]byte): The data to associate with the key.
//
// Returns:
//   - error: An error if the write operation fails.
//
// Errors:
//   - Throws errors if writing to the WAL fails.
func (e *StorageEngine) Put(key string, value []byte) error {
	// No concurrent Writes allowed; No Reads during Writes
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.nextSeq
	// Update WAL
	err := e.active.wal.writeLog(key, value, false, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Push to Memtable
	e.active.memtable.insert(key, value, seq)

	// Flush if Memtable is full
	if e.active.memtable.sizeBytes >= e.memCapacity {
		go func() { _ = e.flush() }()
	}

	return nil
}

// Delete removes a key from the storage engine by inserting a tombstone marker.
//
// Parameters:
//   - key (string): The key to delete.
//
// Returns:
//   - error: An error if the deletion operation fails.
//
// Errors:
//   - Throws errors if writing the tombstone to the WAL fails.
func (e *StorageEngine) Delete(key string) error {
	// No concurrent Writes allowed; No Reads during Writes
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.nextSeq
	// Update WAL
	err := e.active.wal.writeLog(key, []byte{}, true, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Delete from Memtable
	e.active.memtable.delete(key, seq)

	// Flush if Memtable is full
	if e.active.memtable.sizeBytes >= e.memCapacity {
		go func() { _ = e.flush() }()
	}

	return nil
}

/* ====================================================================================
	BACKGROUND METHODS
==================================================================================== */

// flush moves the active memtable to immutables, writes it to an SSTable, and cleans up the WAL.
//
// Parameters:
//   - None
//
// Returns:
//   - error: An error if flushing the SSTable to disk fails.
//
// Errors:
//   - Throws errors if the SSTable file write fails during flush.
func (e *StorageEngine) flush() error {
	// Step 1: Rotate active memlog to immutables under write lock
	e.mu.Lock()
	ml := e.active
	e.immutables = append(e.immutables, ml)

	fileNum := e.nextFileNumber
	e.nextFileNumber++
	e.active = newMemlog(e.nextFileNumber, e.crcTable)
	e.nextFileNumber++
	e.mu.Unlock()

	// Step 2: Serialize the frozen memtable to disk (no lock held)
	err := e.sstables.flush(fileNum, ml.memtable)
	if err != nil {
		log.Printf("flush error: failed to write SSTable for filenum %d: %v", fileNum, err)
		return err
	}

	// Step 3: Remove the now-flushed memlog from immutables under write lock
	e.mu.Lock()
	for i, current := range e.immutables {
		if current == ml {
			e.immutables[i] = nil // prevent memory leak
			e.immutables = append(e.immutables[:i], e.immutables[i+1:]...)
			break
		}
	}
	e.mu.Unlock()

	// Step 4: Delete the WAL file (no lock needed; file is private to this flush)
	walFile := fmt.Sprintf("data/wal/%d.log", ml.wal.logNumber)
	_ = os.Remove(walFile)

	// Step 5: Check if L0 is over capacity and trigger background compaction
	e.sstables.mu.RLock()
	shouldCompact := e.sstables.levels[0].sizeBytes >= e.sstables.levels[0].capacityBytes
	var compactSrc *sst
	if shouldCompact && len(e.sstables.levels[0].sstList) > 0 {
		compactSrc = e.sstables.levels[0].sstList[0]
	}
	e.sstables.mu.RUnlock()

	if compactSrc != nil {
		go func() { _ = e.compact(compactSrc) }()
	}

	return nil
}

// compact merges an SSTable with overlapping SSTables from the next level to reclaim space and order data.
//
// Parameters:
//   - srcSst (*sst): The SSTable to compact into the next level.
//
// Returns:
//   - error: An error if the compaction fails.
//
// Errors:
//   - ErrLvlNotFound: Thrown if the source SSTable's level doesn't exist.
//   - Throws errors for I/O issues during file opening, reading, or writing.
func (e *StorageEngine) compact(srcSst *sst) error {
	// Serialization guard: only one compaction at a time
	if !e.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer e.compacting.Store(false)

	// Step 1: Snapshot metadata under a brief exclusive lock
	e.sstables.mu.Lock()
	lvlIdx := srcSst.level
	if lvlIdx >= len(e.sstables.levels) {
		e.sstables.mu.Unlock()
		return ErrLvlNotFound
	}
	if lvlIdx+1 >= len(e.sstables.levels) {
		curCapacity := e.sstables.levels[lvlIdx].capacityBytes
		nextCapacity := curCapacity * uint64(e.sstables.growthFactor)
		e.sstables.levels = append(e.sstables.levels, newLevel(nextCapacity))
	}
	nextLvl := e.sstables.levels[lvlIdx+1]
	isLastLevel := lvlIdx+1 == len(e.sstables.levels)-1
	overlapping := make([]*sst, 0)
	for _, s := range nextLvl.sstList {
		if s.startKey <= srcSst.endKey && s.endKey >= srcSst.startKey {
			overlapping = append(overlapping, s)
		}
	}
	e.sstables.mu.Unlock()

	// Step 2: Merge-read phase — heavy I/O, no sstables lock held
	src, err := newCompactionSrc(srcSst, overlapping, e.crcTable)
	if err != nil {
		return err
	}
	defer func() {
		if src != nil {
			src.close()
		}
	}()

	newSsts := make([]*sst, 0)
	for {
		// Brief exclusive lock to claim a file number, then release immediately
		e.mu.Lock()
		fileNum := e.nextFileNumber
		e.nextFileNumber++
		e.mu.Unlock()

		out := newSst(fileNum, lvlIdx+1, e.crcTable, e.sstCapacity)

		err := out.compact(src, isLastLevel)
		if err == ErrCompactionDone {
			newSsts = append(newSsts, out)
			break
		} else if err == ErrCompactionFull {
			newSsts = append(newSsts, out)
			continue
		} else {
			return err
		}
	}

	// Step 3: Atomically swap old SSTs out and new SSTs in under a brief exclusive lock
	e.sstables.mu.Lock()

	for _, out := range newSsts {
		nextLvl.insertSorted(out)
		nextLvl.sizeBytes += out.sizeBytes
	}

	curLvl := e.sstables.levels[lvlIdx]
	for i, s := range curLvl.sstList {
		if s == srcSst {
			curLvl.sstList = append(curLvl.sstList[:i], curLvl.sstList[i+1:]...)
			curLvl.sizeBytes -= srcSst.sizeBytes
			break
		}
	}
	for _, s := range overlapping {
		for i, ns := range nextLvl.sstList {
			if ns == s {
				nextLvl.sstList = append(nextLvl.sstList[:i], nextLvl.sstList[i+1:]...)
				nextLvl.sizeBytes -= s.sizeBytes
				break
			}
		}
	}

	shouldCompactNext := nextLvl.sizeBytes >= nextLvl.capacityBytes
	var nextSrc *sst
	if shouldCompactNext && len(nextLvl.sstList) > 0 {
		nextSrc = nextLvl.sstList[0]
	}
	e.sstables.mu.Unlock()

	// Release read locks and files before taking write locks for deletion
	src.close()
	src = nil

	// Step 4: Delete old SST files under per-SST exclusive lock
	allOld := append([]*sst{srcSst}, overlapping...)
	for _, s := range allOld {
		s.mu.Lock()
		os.Remove(fmt.Sprintf("data/sstables/level-%d/%d.sst", s.level, s.filenum))
		s.mu.Unlock()
	}

	// Step 5: Cascade compaction to the next level if it's now over capacity
	if nextSrc != nil {
		go func() { _ = e.compact(nextSrc) }()
	}

	return nil
}
