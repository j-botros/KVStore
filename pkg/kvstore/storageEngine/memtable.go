package storageengine

import (
	"math/rand"
)

const (
	MAX_LEVELS           int    = 12
	ENTRY_OVERHEAD_BYTES uint64 = 21 // seq(8) + tombstone(1) + keyLength(4) + valueLength(4) + checksum(4)
)

type node struct {
	key       string
	value     []byte
	level     int
	next      [MAX_LEVELS]*node
	seq       uint64
	tombstone bool
}

func newNode(key string, value []byte, level int, seq uint64, tombstone bool) *node {
	return &node{
		key:       key,
		value:     value,
		level:     level,
		seq:       seq,
		tombstone: tombstone,
	}
}

type memtable struct {
	head          *node
	height        int
	capacityBytes uint64
	sizeBytes     uint64
	numKeys       uint64
}

func newMemtable(capacityBytes uint64) *memtable {
	node := newNode("", nil, MAX_LEVELS, 0, false)
	return &memtable{
		head:          node,
		height:        1,
		capacityBytes: capacityBytes,
		sizeBytes:     0,
		numKeys:       0,
	}
}

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

func (m *memtable) randomLevel() int {
	level := 1
	for rand.Float64() < 0.5 && level < MAX_LEVELS {
		level++
	}
	return level
}

func entrySizeBytes(key string, value []byte) uint64 {
	return ENTRY_OVERHEAD_BYTES + uint64(len(key)+len(value))
}
