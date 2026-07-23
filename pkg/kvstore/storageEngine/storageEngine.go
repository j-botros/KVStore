package storageengine

import (
	"hash/crc32"
)

type StorageEngine struct {
	memtable memtable
	metadata metadata
	nextSeq  uint64
}

type metadata struct {
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
	value, err = e.memtable.Get(key)
	if err == nil {
		return value, err
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Search SSTables for key-value
	value, err = e.metadata.sstables.get(key)
	return value, err
}

func (e *StorageEngine) Put(key string, value []byte) error {
	seq := e.nextSeq
	// Update WAL
	err := e.metadata.wal.writeLog(key, value, false, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Push to Memtable
	e.memtable.Insert(key, value, seq)
	return nil
}

func (e *StorageEngine) Delete(key string) error {
	seq := e.nextSeq
	// Update WAL
	err := e.metadata.wal.writeLog(key, []byte{}, true, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Delete from Memtable
	e.memtable.Delete(key, seq)
	return nil
}
