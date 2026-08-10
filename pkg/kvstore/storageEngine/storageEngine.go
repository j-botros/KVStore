package storageengine

import (
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
)

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

type memlog struct {
	memtable *memtable
	wal      *wal
}

func newMemlog(logNumber uint64, crcTable *crc32.Table) *memlog {
	return &memlog{
		memtable: newMemtable(),
		wal:      newWal(logNumber, crcTable),
	}
}

/* ====================================================================================
	STORAGE (CRUD) METHODS
==================================================================================== */

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
	return nil
}

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
	return nil
}

/* ====================================================================================
	BACKGROUND METHODS
==================================================================================== */

func (e *StorageEngine) Flush() error {
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
		return err
	}

	// Step 3: Remove the now-flushed memlog from immutables under write lock
	e.mu.Lock()
	if len(e.immutables) > 0 {
		e.immutables[0] = nil // prevent memory leak
		e.immutables = e.immutables[1:]
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
		go func() { _ = e.Compact(compactSrc) }()
	}

	return nil
}

func (e *StorageEngine) Compact(srcSst *sst) error {
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
		go func() { _ = e.Compact(nextSrc) }()
	}

	return nil
}
