package storageengine

import (
	"hash/crc32"
)

type StorageEngine struct {
	memtable      *memtable
	memCapacity   uint64
	fullMemtables []*memtable
	disk          *disk
	nextSeq       uint64
}

type disk struct {
	nextFileNumber uint64
	wal            *wal
	sstables       *sstables
	crcTable       *crc32.Table
}

/* ====================================================================================
	STORAGE (CRUD) METHODS
==================================================================================== */

func (e *StorageEngine) Get(key string) (value []byte, err error) {
	// Search Memtable for key-value
	value, err = e.memtable.get(key)
	if err == nil {
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Search SSTables for key-value
	value, err = e.disk.sstables.get(key)
	return value, err
}

func (e *StorageEngine) Put(key string, value []byte) error {
	seq := e.nextSeq
	// Update WAL
	err := e.disk.wal.writeLog(key, value, false, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Push to Memtable
	e.memtable.insert(key, value, seq)
	return nil
}

func (e *StorageEngine) Delete(key string) error {
	seq := e.nextSeq
	// Update WAL
	err := e.disk.wal.writeLog(key, []byte{}, true, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Delete from Memtable
	e.memtable.delete(key, seq)
	return nil
}

/* ====================================================================================
	BACKGROUND METHODS
==================================================================================== */

func (e *StorageEngine) FlushMemtable() error {
	// TODO: Create new Memtable to accept writes during flush process
	oldMem := e.memtable
	e.fullMemtables = append(e.fullMemtables, oldMem)
	e.memtable = newMemtable(e.memCapacity)

	err := e.disk.sstables.flush(e.disk.nextFileNumber, oldMem, e.disk.crcTable)
	if err != nil {
		return err
	}

	e.disk.nextFileNumber++
	return nil
}
