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

	mu       sync.RWMutex
	flushing atomic.Bool
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
	// Reject concurrent flush attempts immediately
	if !e.flushing.CompareAndSwap(false, true) {
		return nil
	}
	defer e.flushing.Store(false)

	// Step 1: Rotate active memlog to immutables under write lock
	e.mu.Lock()
	ml := e.active
	e.immutables = append(e.immutables, ml)

	fileNum := e.nextFileNumber
	e.nextFileNumber++
	e.active = newMemlog(e.nextFileNumber, e.crcTable)
	e.nextFileNumber++
	e.mu.Unlock()

	// Step 2: Flush to SSTable on disk without holding the mutex (allows concurrent reads/writes)
	err := e.sstables.flush(fileNum, ml.memtable)
	if err != nil {
		return err
	}

	// Step 3: Remove flushed memlog from immutables and clean up WAL under write lock
	e.mu.Lock()
	if len(e.immutables) > 0 {
		e.immutables[0] = nil // Avoid memory leak
		e.immutables = e.immutables[1:]
	}
	e.mu.Unlock()

	// Delete WAL file after successful flush
	walFile := fmt.Sprintf("data/wal/%d.log", ml.wal.logNumber)
	_ = os.Remove(walFile)

	return nil
}

func (e *StorageEngine) Compact(srcSst *sst) error {
	lvlIdx := srcSst.level

	if lvlIdx >= len(e.sstables.levels) {
		return ErrLvlNotFound
	}

	// Create next level if it doesn't exist yet
	if lvlIdx+1 >= len(e.sstables.levels) {
		curCapacity := e.sstables.levels[lvlIdx].capacityBytes
		nextCapacity := curCapacity * uint64(e.sstables.growthFactor)
		e.sstables.levels = append(e.sstables.levels, newLevel(nextCapacity))
	}
	nextLvl := e.sstables.levels[lvlIdx+1]
	isLastLevel := lvlIdx+1 == len(e.sstables.levels)-1

	// Find all SSTs in nextLvl whose key range overlaps with srcSst
	overlapping := make([]*sst, 0)
	for _, s := range nextLvl.sstList {
		if s.startKey <= srcSst.endKey && s.endKey >= srcSst.startKey {
			overlapping = append(overlapping, s)
		}
	}

	src, err := newCompactionSrc(srcSst, overlapping, e.crcTable)
	if err != nil {
		return err
	}
	defer src.close()

	// Loop: produce output SSTs until all source entries are consumed
	for {
		out := newSst(e.nextFileNumber, lvlIdx+1, e.crcTable)
		out.capacity = e.sstCapacity
		e.nextFileNumber++

		err := out.compact(src, isLastLevel)
		if err == ErrCompactionDone {
			nextLvl.insertSorted(out)
			nextLvl.sizeBytes += out.sizeBytes
			break
		} else if err == ErrCompactionFull {
			nextLvl.insertSorted(out)
			nextLvl.sizeBytes += out.sizeBytes
			continue
		} else {
			return err
		}
	}

	// Remove srcSst from its level
	curLvl := e.sstables.levels[lvlIdx]
	for i, s := range curLvl.sstList {
		if s == srcSst {
			curLvl.sstList = append(curLvl.sstList[:i], curLvl.sstList[i+1:]...)
			curLvl.sizeBytes -= srcSst.sizeBytes
			break
		}
	}

	// Remove all overlapping SSTs from nextLvl
	for _, s := range overlapping {
		for i, ns := range nextLvl.sstList {
			if ns == s {
				nextLvl.sstList = append(nextLvl.sstList[:i], nextLvl.sstList[i+1:]...)
				nextLvl.sizeBytes -= s.sizeBytes
				break
			}
		}
	}

	return nil
}

