package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

/* ====================================================================================
	SSTABLES CLASS
==================================================================================== */

type sstables struct {
	levels   uint
	sstList  [][]*sst
	crcTable *crc32.Table
}

func (sstables *sstables) get(key string) (value []byte, err error) {
	seq := uint64(0)
	tombstone := false
	for _, sst := range sstables.sstList[0] {
		if key >= sst.startKey && key <= sst.endKey {
			curValue, curTombstone, curSeq, err := sst.search(key)

			if !curTombstone && err == ErrKeyNotFound {
				continue
			} else if err != nil {
				return nil, err
			}

			if curSeq >= seq {
				seq = curSeq
				value = curValue
				tombstone = curTombstone
			}
		}
	}

	if tombstone {
		return nil, ErrKeyNotFound
	} else if value != nil {
		return value, nil
	}

	for level := uint(1); level < sstables.levels; level++ {
		for _, sst := range sstables.sstList[level] {
			if key >= sst.startKey && key <= sst.endKey {
				value, tombstone, _, err = sst.search(key)

				if tombstone {
					return nil, ErrKeyNotFound
				} else if err == ErrKeyNotFound {
					continue
				} else if err != nil {
					return nil, err
				}

				return value, nil
			}
		}
	}

	return nil, ErrKeyNotFound
}

func (sstables *sstables) flush(filenum uint64, memtable *memtable, crcTable *crc32.Table) error {
	newSst, err := newSst(filenum, memtable, crcTable)
	if err != nil {
		return err
	}

	sstables.sstList[0] = append(sstables.sstList[0], newSst)
	return nil
}

/* ====================================================================================
	SST CLASS & INDEX/BLOCK CLASSES
==================================================================================== */

type sst struct {
	// SST file data
	filenum  uint64
	level    uint
	lastSeq  uint64
	startKey string
	endKey   string

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
)

func newSst(filenum uint64, memtable *memtable, crcTable *crc32.Table) (*sst, error) {
	sst := &sst{
		filenum:  filenum,
		level:    0,
		crcTable: crcTable,
	}

	// Create bloom filter
	bf := newBloomFilter(memtable.numKeys)
	sst.bloomFilter = bf

	// TODO: Create index

	// Create file
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", sst.level, sst.filenum)

	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sstFile, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer sstFile.Close()

	curr := memtable.head.next[0]
	for curr != nil {
		// TODO: Write each entry to disk
	}

	return sst, nil
}

func (sst *sst) search(key string) (value []byte, isTombstone bool, seq uint64, err error) {
	// Check bloom filter
	if sst.bloomFilter.keyNotPresent(key) {
		return nil, false, 0, ErrKeyNotFound
	}

	// Find data block in SST from index
	blockOffset, blockLength, err := sst.index.getDatablock(key)
	if err != nil {
		return nil, false, 0, err
	}

	// Read from file
	filename := fmt.Sprintf("data/sstables/level-%d/%d.sst", sst.level, sst.filenum)

	sstFile, err := os.Open(filename)
	if err != nil {
		return nil, false, 0, err
	}
	defer sstFile.Close()

	r := io.NewSectionReader(sstFile, int64(blockOffset), int64(blockLength))
	for {
		buf := new(bytes.Buffer)

		// Read: seq (8 bytes)
		err = binary.Read(r, binary.LittleEndian, &seq)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, false, 0, err
		}

		err = binary.Write(buf, binary.LittleEndian, seq)
		if err != nil {
			return nil, false, 0, err
		}

		// Read: tombstone (1 byte)
		var tombstone byte

		err = binary.Read(r, binary.LittleEndian, &tombstone)
		if err != nil {
			return nil, false, 0, err
		}

		err = buf.WriteByte(tombstone)
		if err != nil {
			return nil, false, 0, err
		}

		// Read: key length (4 bytes)
		var keyLength uint32

		err = binary.Read(r, binary.LittleEndian, &keyLength)
		if err != nil {
			return nil, false, 0, err
		}

		err = binary.Write(buf, binary.LittleEndian, keyLength)
		if err != nil {
			return nil, false, 0, err
		}

		// Read: key
		keyBuf := make([]byte, keyLength)
		_, err := io.ReadFull(r, keyBuf)
		if err != nil {
			return nil, false, 0, err
		}
		buf.Write(keyBuf)
		ekey := string(keyBuf)

		if tombstone == 0 {
			// Read: value length (4 bytes)
			var valLength uint32

			err = binary.Read(r, binary.LittleEndian, &valLength)
			if err != nil {
				return nil, false, 0, err
			}

			err = binary.Write(buf, binary.LittleEndian, valLength)
			if err != nil {
				return nil, false, 0, err
			}

			// Read: value
			value = make([]byte, valLength)
			_, err := io.ReadFull(r, value)
			if err != nil {
				return nil, false, 0, err
			}
			buf.Write(value)
		}

		// Read: checksum (4 bytes)
		var checksum uint32

		err = binary.Read(r, binary.LittleEndian, &checksum)
		if err != nil {
			return nil, false, 0, err
		}

		if key == ekey {
			expected := crc32.Checksum(buf.Bytes(), sst.crcTable)
			if checksum != expected {
				return nil, false, 0, ErrBadData
			}

			return value, tombstone == 1, seq, nil
		}
	}

	return nil, false, 0, ErrKeyNotFound
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
