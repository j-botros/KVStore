package storageengine

import (
	"hash/fnv"
)

type bloomFilter struct {
	bitstring []byte
	numHashes uint
	numKeys   uint
}

func NewBloomFilter(numKeys uint) *bloomFilter {
	return &bloomFilter{
		bitstring: make([]byte, (10*numKeys+7)/8),
		numKeys:   numKeys,
	}
}

func (bf *bloomFilter) setBloomBits(key string) {
	// Compute two independent hashes once
	h1 := fnv.New64()
	h1.Write([]byte(key))
	a := uint(h1.Sum64())

	h2 := fnv.New64()
	h2.Write([]byte(key))
	b := uint(h2.Sum64())

	// Derive k indices from the two hashes (no seed needed)
	for i := uint(0); i < bf.numHashes; i++ {
		index := (a + i*b) % bf.numKeys
		bf.bitstring[index/8] |= 1 << (index % 8)
	}
}

func (bf *bloomFilter) keyNotPresent(key string) bool {
	// Compute two independent hashes once
	h1 := fnv.New64()
	h1.Write([]byte(key))
	a := uint(h1.Sum64())

	h2 := fnv.New64()
	h2.Write([]byte(key))
	b := uint(h2.Sum64())

	// Derive k indices from the two hashes (no seed needed)
	for i := uint(0); i < bf.numHashes; i++ {
		index := (a + i*b) % bf.numKeys

		if bf.bitstring[index/8]&(1<<(index%8)) == 0 {
			return true
		}
	}
	return false
}
