package storageengine

import (
	"hash/fnv"
)

// bloomFilter represents a probabilistic data structure used to test whether an element is a member of a set.
// It is used in the storage engine within each SSTable to quickly check if a key might exist in that SSTable
// before performing expensive disk reads.
type bloomFilter struct {
	bitstring []byte
	numHashes uint64
	numKeys   uint64
}

// newBloomFilter creates and initializes a new bloom filter.
//
// Parameters:
//   - numKeys (uint64): The expected number of keys to be inserted into the bloom filter.
//
// Returns:
//   - *bloomFilter: A pointer to the newly created bloom filter.
//
// Errors:
//   - None
func newBloomFilter(numKeys uint64) *bloomFilter {
	return &bloomFilter{
		bitstring: make([]byte, (10*numKeys+7)/8),
		numKeys:   numKeys,
	}
}

// setBloomBits hashes the given key and sets the corresponding bits in the bloom filter.
//
// Parameters:
//   - key (string): The key string to insert into the bloom filter.
//
// Returns:
//   - None
//
// Errors:
//   - None
func (bf *bloomFilter) setBloomBits(key string) {
	// Compute two independent hashes once
	h1 := fnv.New64()
	h1.Write([]byte(key))
	a := uint64(h1.Sum64())

	h2 := fnv.New64()
	h2.Write([]byte(key))
	b := uint64(h2.Sum64())

	// Derive k indices from the two hashes (no seed needed)
	for i := uint64(0); i < bf.numHashes; i++ {
		index := (a + i*b) % bf.numKeys
		bf.bitstring[index/8] |= 1 << (index % 8)
	}
}

// keyNotPresent checks if the given key is definitely not in the bloom filter.
//
// Parameters:
//   - key (string): The key to check for absence in the bloom filter.
//
// Returns:
//   - bool: True if the key is definitely not present, false if the key might be present.
//
// Errors:
//   - None
func (bf *bloomFilter) keyNotPresent(key string) bool {
	// Compute two independent hashes once
	h1 := fnv.New64()
	h1.Write([]byte(key))
	a := uint64(h1.Sum64())

	h2 := fnv.New64()
	h2.Write([]byte(key))
	b := uint64(h2.Sum64())

	// Derive k indices from the two hashes (no seed needed)
	for i := uint64(0); i < bf.numHashes; i++ {
		index := (a + i*b) % bf.numKeys

		if bf.bitstring[index/8]&(1<<(index%8)) == 0 {
			return true
		}
	}
	return false
}
