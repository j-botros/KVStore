package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type wal struct {
	logNumber     uint64
	lastSeq       uint64
	capacityBytes uintptr // TODO: Track size of file
	crcTable      *crc32.Table
}

func newWal(logNumber uint64, lastSeq uint64, capacityBytes uintptr) *wal {
	return &wal{
		logNumber:     logNumber,
		lastSeq:       lastSeq,
		capacityBytes: capacityBytes,
		crcTable:      crc32.MakeTable(crc32.Castagnoli),
	}
}

func (wal *wal) writeLog(key string, value []byte, tombstone bool, seq uint64) error {
	filename := fmt.Sprintf("data/wal/%d.log", wal.logNumber)

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

	wal.lastSeq = seq
	return nil
}
