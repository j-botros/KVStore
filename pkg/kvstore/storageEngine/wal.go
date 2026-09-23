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
	logNumber uint64
	lastSeq   uint64
	crcTable  *crc32.Table
}

func newWal(logNumber uint64, crcTable *crc32.Table) *wal {
	return &wal{
		logNumber: logNumber,
		lastSeq:   0,
		crcTable:  crcTable,
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

// replayWal recovers MemTable and WAL Metadata from a WAL
func replayWal(logNumber uint64, maxSeqFromSsts uint64, crcTable *crc32.Table) (*memlog, error) {
	fpath := fmt.Sprintf("data/wal/%d.log", logNumber)

	// --- Open WAL file ---
	walFile, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	defer walFile.Close()

	fi, err := walFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("replayWal %d: stat: %w", logNumber, err)
	}

	// --- Replay entries into a fresh memlog ---
	ml := newMemlog(logNumber, crcTable)

	// readEntry expects an *io.SectionReader; wrap the whole file in one.
	r := io.NewSectionReader(walFile, 0, fi.Size())
	for {
		e, err := readEntry(r, crcTable)
		if err == ErrEntryNotFound {
			break // clean EOF
		} else if err != nil {
			// A partial write at the tail (crash mid-write) is possible.
			// Treat any decode/checksum error at this point as end-of-log.
			break
		}

		// Skip entries already persisted in SSTs.
		if e.seq <= maxSeqFromSsts {
			continue
		}

		if e.tombstone {
			ml.memtable.delete(e.key, e.seq)
		} else {
			ml.memtable.insert(e.key, e.value, e.seq)
		}

		if e.seq > ml.wal.lastSeq {
			ml.wal.lastSeq = e.seq
		}
	}

	return ml, nil
}
