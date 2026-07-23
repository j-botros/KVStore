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

func (sstables *sstables) Get(key string) (value []byte, err error) {
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
	// TODO: Search SSTables

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
		var ekey string

		err = binary.Read(r, binary.LittleEndian, &ekey)
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.LittleEndian, ekey)
		if err != nil {
			return nil, err
		}

		if tombstone == 0 {
			// Read: value length (4 bytes)
			var valueLength uint32

			err = binary.Read(r, binary.LittleEndian, &valueLength)
			if err != nil {
				return nil, err
			}

			err = binary.Write(buf, binary.LittleEndian, valueLength)
			if err != nil {
				return nil, err
			}

			// Read: value
			err = binary.Read(r, binary.LittleEndian, &value)
			if err != nil {
				return nil, err
			}

			err = binary.Write(buf, binary.LittleEndian, value)
			if err != nil {
				return nil, err
			}
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
type index []block

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
	FOOTER_SIZE  = 8 + 8 + 8 + 8 + 8 // Block offset + Block length + Bloom offset + Bloom length + Footer magic
)

func readFooter(sstFile *os.File) (blockOffset uint64, blockLength uint64, bloomOffset uint64, bloomLength uint64, err error) {
	_, err = sstFile.Seek(-FOOTER_SIZE, io.SeekEnd)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	buf := make([]byte, FOOTER_SIZE)
	_, err = io.ReadFull(sstFile, buf)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	blockOffset = binary.LittleEndian.Uint64(buf[0:8])
	blockLength = binary.LittleEndian.Uint64(buf[8:16])
	bloomOffset = binary.LittleEndian.Uint64(buf[16:24])
	bloomLength = binary.LittleEndian.Uint64(buf[24:32])
	magic := binary.LittleEndian.Uint64(buf[32:40])

	if magic != FOOTER_MAGIC {
		return 0, 0, 0, 0, ErrBadFile
	}

	return blockOffset, blockLength, bloomOffset, bloomLength, nil
}
