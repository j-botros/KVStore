package storageengine

import "testing"

func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		numKeys        uint64
		expectedBitLen int
	}{
		{0, 0},     // (0+7)/8 = 0
		{1, 2},     // (10+7)/8 = 2
		{8, 10},    // (80+7)/8 = 10
		{10, 13},   // (100+7)/8 = 13
		{100, 125}, // (1000+7)/8 = 125
	}

	for _, tt := range tests {
		bf := newBloomFilter(tt.numKeys)
		if len(bf.bitstring) != tt.expectedBitLen {
			t.Errorf("newBloomFilter(%d): bitstring len = %d, want %d",
				tt.numKeys, len(bf.bitstring), tt.expectedBitLen)
		}
		if bf.numKeys != tt.numKeys {
			t.Errorf("newBloomFilter(%d): numKeys = %d, want %d",
				tt.numKeys, bf.numKeys, tt.numKeys)
		}
		if bf.numHashes != 0 {
			t.Errorf("newBloomFilter(%d): numHashes = %d, want 0",
				tt.numKeys, bf.numHashes)
		}
	}
}

func TestBloomFilter_DefaultNumHashes_AlwaysPassThrough(t *testing.T) {
	// With the default numHashes=0, the check loop never executes,
	// so keyNotPresent always returns false ("key may be present").
	bf := newBloomFilter(100)
	keys := []string{"hello", "world", "", "anything"}
	for _, k := range keys {
		if bf.keyNotPresent(k) {
			t.Errorf("keyNotPresent(%q) = true; want false when numHashes=0", k)
		}
	}
}

func TestSetBloomBits_NoFalseNegatives(t *testing.T) {
	bf := newBloomFilter(100)
	bf.numHashes = 3

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		bf.setBloomBits(key)
	}

	// Bloom filter guarantee: no false negatives.
	// Every inserted key must be reported as "possibly present" (false).
	for _, key := range keys {
		if bf.keyNotPresent(key) {
			t.Errorf("keyNotPresent(%q) = true after setBloomBits; bloom filters must not have false negatives", key)
		}
	}
}

func TestKeyNotPresent_EmptyFilterWithHashes(t *testing.T) {
	bf := newBloomFilter(100)
	bf.numHashes = 3

	// All bits are zero and numHashes > 0, so any lookup must return true.
	if !bf.keyNotPresent("nonexistent") {
		t.Error("keyNotPresent(\"nonexistent\") = false on empty filter with numHashes > 0; want true")
	}
}

func TestSetBloomBits_SetsNonZeroBits(t *testing.T) {
	bf := newBloomFilter(100)
	bf.numHashes = 5

	// Before insertion, all bits should be zero.
	for _, b := range bf.bitstring {
		if b != 0 {
			t.Fatal("expected all-zero bitstring before any insertion")
		}
	}

	bf.setBloomBits("test-key")

	// After insertion, at least one byte must be non-zero.
	anySet := false
	for _, b := range bf.bitstring {
		if b != 0 {
			anySet = true
			break
		}
	}
	if !anySet {
		t.Error("expected at least one non-zero byte after setBloomBits")
	}
}
