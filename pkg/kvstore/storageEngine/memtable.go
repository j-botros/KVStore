package storageengine

import (
	"math/rand"
)

const (
	MAX_LEVELS           int    = 12
	ENTRY_OVERHEAD_BYTES uint64 = 21 // seq(8) + tombstone(1) + keyLength(4) + valueLength(4) + checksum(4)
)

// node represents a single key-value entry in the memtable's skip list data structure.
// It is used internally by the memtable to store and link entries across multiple levels.
type node struct {
	key       string
	value     []byte
	level     int
	next      [MAX_LEVELS]*node
	seq       uint64
	tombstone bool
}

// newNode creates a new skip list node for the memtable.
//
// Parameters:
//   - key (string): The key for the node.
//   - value ([]byte): The value byte slice for the node.
//   - level (int): The maximum level the node exists in the skip list.
//   - seq (uint64): The sequence number of the operation.
//   - tombstone (bool): Indicates if the node represents a deletion (tombstone).
//
// Returns:
//   - *node: A pointer to the newly created node.
//
// Errors:
//   - None
func newNode(key string, value []byte, level int, seq uint64, tombstone bool) *node {
	return &node{
		key:       key,
		value:     value,
		level:     level,
		seq:       seq,
		tombstone: tombstone,
	}
}

// memtable represents an in-memory key-value store implemented as a skip list.
// It is used in the storage engine to hold the most recent writes (and deletes) before
// they are flushed to disk as SSTables.
type memtable struct {
	head      *node
	height    int
	sizeBytes uint64
	numKeys   uint64
}

// newMemtable creates and initializes an empty memtable (skip list).
//
// Parameters:
//   - None
//
// Returns:
//   - *memtable: A pointer to the newly created memtable.
//
// Errors:
//   - None
func newMemtable() *memtable {
	node := newNode("", nil, MAX_LEVELS, 0, false)
	return &memtable{
		head:      node,
		height:    1,
		sizeBytes: 0,
		numKeys:   0,
	}
}

// get retrieves the value associated with the specified key from the memtable.
//
// Parameters:
//   - key (string): The key to search for in the memtable.
//
// Returns:
//   - value ([]byte): The value associated with the key if found, otherwise nil.
//   - err (error): An error if the key is not found or is a tombstone.
//
// Errors:
//   - ErrKeyNotFound: Thrown when the key does not exist in the memtable or has been deleted (tombstone).
func (m *memtable) get(key string) (value []byte, err error) {
	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.next[l] != nil && curr.next[l].key < key {
			curr = curr.next[l]
		}
	}

	curr = curr.next[0] // step forward at level 0
	if curr != nil && curr.key == key {
		if curr.tombstone {
			return nil, ErrKeyNotFound // treat tombstone as deleted
		}
		return curr.value, nil
	}
	return nil, ErrKeyNotFound
}

// insert adds a new key-value pair to the memtable or updates an existing one.
//
// Parameters:
//   - key (string): The key to insert or update.
//   - value ([]byte): The value associated with the key.
//   - seq (uint64): The sequence number for this insertion operation.
//
// Returns:
//   - None
//
// Errors:
//   - None
func (m *memtable) insert(key string, value []byte, seq uint64) {
	// Track predecessors at each level
	prevs := [MAX_LEVELS]*node{}
	for i := range prevs {
		prevs[i] = m.head
	}

	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.next[l] != nil && curr.next[l].key < key {
			curr = curr.next[l]
		}
		prevs[l] = curr
	}

	// Check if the key already exists at level 0
	curr = curr.next[0]
	if curr != nil && curr.key == key {
		// Update in place
		oldSize := entrySizeBytes(curr.key, curr.value)

		curr.value = value
		curr.seq = seq
		curr.tombstone = false

		newSize := entrySizeBytes(curr.key, curr.value)
		m.sizeBytes = m.sizeBytes - oldSize + newSize

		return
	}

	// New key: create and stitch in at every level
	level := m.randomLevel()
	node := newNode(key, value, level, seq, false)
	m.height = max(m.height, level)

	for i := 0; i < level; i++ {
		node.next[i] = prevs[i].next[i]
		prevs[i].next[i] = node
	}

	m.sizeBytes += entrySizeBytes(key, value)
	m.numKeys++
}

// delete marks a key as deleted in the memtable by inserting/updating it as a tombstone.
//
// Parameters:
//   - key (string): The key to delete.
//   - seq (uint64): The sequence number for this deletion operation.
//
// Returns:
//   - None
//
// Errors:
//   - None
func (m *memtable) delete(key string, seq uint64) {
	// Track predecessors at each level
	prevs := [MAX_LEVELS]*node{}
	for i := range prevs {
		prevs[i] = m.head
	}

	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.next[l] != nil && curr.next[l].key < key {
			curr = curr.next[l]
		}
		prevs[l] = curr
	}

	// Check if the key already exists at level 0
	curr = curr.next[0]
	if curr != nil && curr.key == key {
		// Update in place
		oldSize := entrySizeBytes(curr.key, curr.value)

		curr.value = []byte{}
		curr.seq = seq
		curr.tombstone = true

		newSize := entrySizeBytes(curr.key, curr.value)
		m.sizeBytes = m.sizeBytes - oldSize + newSize

		return
	}

	// New key: create and stitch in at every level
	level := m.randomLevel()
	tombstone := newNode(key, []byte{}, level, seq, true)
	m.height = max(m.height, level)

	for i := 0; i < level; i++ {
		tombstone.next[i] = prevs[i].next[i]
		prevs[i].next[i] = tombstone
	}

	m.sizeBytes += entrySizeBytes(key, []byte{})
	m.numKeys++
}

// randomLevel generates a random level for a new skip list node.
//
// Parameters:
//   - None
//
// Returns:
//   - int: A randomly determined level between 1 and MAX_LEVELS.
//
// Errors:
//   - None
func (m *memtable) randomLevel() int {
	level := 1
	for rand.Float64() < 0.5 && level < MAX_LEVELS {
		level++
	}
	return level
}

// entrySizeBytes calculates the size in bytes of a memtable entry.
//
// Parameters:
//   - key (string): The key string.
//   - value ([]byte): The value byte slice.
//
// Returns:
//   - uint64: The total size in bytes of the entry including overhead.
//
// Errors:
//   - None
func entrySizeBytes(key string, value []byte) uint64 {
	return ENTRY_OVERHEAD_BYTES + uint64(len(key)+len(value))
}
