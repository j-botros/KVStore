package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type sstables struct {
	levels   uint
	sstList  [][]*sst
	crcTable *crc32.Table
}

func (sstables *sstables) get(key string) (value []byte, err error) {
	for level := uint(0); level < sstables.levels; level++ {
		for _, sst := range sstables.sstList[level] {
			if key >= sst.startKey && key <= sst.endKey {
				value, err := sst.search(key)

				if err != nil {
					return nil, err
				}

				return value, nil
			}
		}
	}

	return nil, ErrKeyNotFound
}

type sst struct {
	// SST file data
	filenum  uint64
	level    uint
	lastSeq  uint64
	startKey string
	endKey   string

	// Index
	index index

	// Bloom filter
	bloomFilter bloomFilter

	// Checksum table
	crcTable *crc32.Table
}

func (sst *sst) search(key string) (value []byte, err error) {
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
		buf := new(bytes.Buffer)

		// Read: seq (8 bytes)
		var seq uint64

		err = binary.Read(r, binary.LittleEndian, &seq)
		if err == io.EOF {
			break
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
		_, err := io.ReadFull(r, keyBuf)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBuf)
		ekey := string(keyBuf)

		if tombstone == 0 {
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
			value = make([]byte, valLength)
			_, err := io.ReadFull(r, value)
			if err != nil {
				return nil, err
			}
			buf.Write(value)
		}

		// Read: checksum (4 bytes)
		var checksum uint32

		err = binary.Read(r, binary.LittleEndian, &keyLength)
		if err != nil {
			return nil, err
		}

		if key == ekey {
			expected := crc32.Checksum(buf.Bytes(), sst.crcTable)
			if checksum != expected {
				return nil, ErrBadData
			}

			return value, nil
		}
	}

	return nil, ErrKeyNotFound
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

		if (*index)[m].lastKey >= key && key > (*index)[m].prevBlockKey {
			return (*index)[m].offset, (*index)[m].length, nil
		} else if (*index)[m].lastKey >= key {
			l = m + 1
		} else {
			r = m - 1
		}
	}

	return 0, 0, ErrKeyNotFound
}

const (
	FOOTER_MAGIC = uint64(0x4c55564c49414e41)
	FOOTER_SIZE  = 8 + 8 + 8 + 8 + 8 // Index offset + Index length + Bloom offset + Bloom length + Footer magic
)

func (sst *sst) readFooter() (index *index, bf *bloomFilter, err error) {
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

	*index = []*block{}
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

		block := newBlock(lastKey, offset, length, prevBlockKey)
		*index = append(*index, block)
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

	return index, bf, nil
}
