package storageengine

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strconv"
)

type StorageEngine struct {
	memtable Memtable
	metadata metadata
	nextSeq  uint64
}

/* ====================================================================================
	STORAGE (CRUD) METHODS
==================================================================================== */

func (e *StorageEngine) Get(key string) ([]byte, error) {
	// Search Memtable for key-value
	value, err := e.memtable.Get(key)
	if err == nil {
		return value, err
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Search SSTables for key-value
	value, err = e.metadata.Sstables.Get(key)
	return value, err
}

func (e *StorageEngine) Put(key string, value []byte) error {
	seq := e.nextSeq
	// Update WAL
	err := e.metadata.Wal.WriteLog(key, value, false, seq)
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
	err := e.metadata.Wal.WriteLog(key, []byte{}, true, seq)
	if err != nil {
		return err
	}
	e.nextSeq++

	// Delete from Memtable
	e.memtable.Delete(key, seq)
	return nil
}

/* ====================================================================================
	METADATA AND FILE MANIPULATION
==================================================================================== */

type metadata struct {
	NextFileNumber uint64
	Wal            wal
	Sstables       sstables
}

type wal struct {
	LogNumber     uint64
	LastSeq       uint64
	capacityBytes uintptr
	crcTable      *crc32.Table
}

func NewWal(logNumber uint64, lastSeq uint64, capacityBytes uintptr) *wal {
	return &wal{
		LogNumber:     logNumber,
		LastSeq:       lastSeq,
		capacityBytes: capacityBytes,
		crcTable:      crc32.MakeTable(crc32.Castagnoli),
	}
}

func (wal *wal) WriteLog(key string, value []byte, tombstone bool, seq uint64) error {
	filename := "data/wal/" + strconv.FormatUint(wal.LogNumber, 10) + ".log"
	walFile, err := os.OpenFile(
		filename,
		os.O_WRONLY|os.O_APPEND|os.O_CREATE,
		0644,
	)
	if err != nil {
		return err
	}
	defer walFile.Close()

	// Find current end position of file in-case of rollback
	rollbackOffset, err := walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)

	// Write: seq (8 bytes)
	binary.Write(buf, binary.LittleEndian, seq)

	// Write: tombstone (1 byte)
	if tombstone {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	// Write: key length (4 bytes) + key
	keyBytes := []byte(key)
	binary.Write(buf, binary.LittleEndian, uint32(len(keyBytes)))
	buf.Write(keyBytes)

	// Write: value length (4 bytes) + key
	binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	buf.Write(value)

	// Write: checksum (4 bytes)
	checksum := crc32.Checksum(buf.Bytes(), wal.crcTable)
	binary.Write(buf, binary.LittleEndian, checksum)

	// Rollback if error or fewer bytes written than expected
	bytesWritten, err := walFile.Write(buf.Bytes())
	if err != nil || bytesWritten < buf.Len() {
		walFile.Truncate(rollbackOffset)

		if err != nil {
			return err
		}

		return io.ErrShortWrite
	}

	// Flush to disk
	if err := walFile.Sync(); err != nil {
		walFile.Truncate(rollbackOffset)
		return err
	}

	wal.LastSeq = seq
	return nil
}

type sstables struct {
	levels uint
	sst    [][]uint64
}

func (sstables *sstables) Get(key string) ([]byte, error) {
	// TODO: Search SSTables

	return nil, ErrKeyNotFound
}
