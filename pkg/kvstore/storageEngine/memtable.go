package storageengine

import (
	"math/rand"
	"unsafe"
)

const (
	MAX_LEVELS int = 12
)

type node struct {
	Key       string
	Value     []byte
	Level     int
	Next      [MAX_LEVELS]*node
	Seq       uint64
	Tombstone bool
}

func newNode(key string, value []byte, level int, seq uint64, tombstone bool) *node {
	return &node{
		Key:       key,
		Value:     value,
		Level:     level,
		Seq:       seq,
		Tombstone: tombstone,
	}
}

type Memtable struct {
	head          *node
	height        int
	capacityBytes uintptr
	sizeBytes     uintptr
}

func NewMemtable(capacityBytes uintptr) *Memtable {
	node := newNode("", nil, MAX_LEVELS, 0, false)
	return &Memtable{
		head:          node,
		height:        1,
		capacityBytes: capacityBytes,
		sizeBytes:     unsafe.Sizeof(*node),
	}
}

func (m *Memtable) Get(key string) ([]byte, error) {
	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.Next[l] != nil && curr.Next[l].Key < key {
			curr = curr.Next[l]
		}
	}

	curr = curr.Next[0] // step forward at level 0
	if curr != nil && curr.Key == key {
		if curr.Tombstone {
			return nil, ErrKeyNotFound // treat tombstone as deleted
		}
		return curr.Value, nil
	}
	return nil, ErrKeyNotFound
}

func (m *Memtable) Insert(key string, value []byte, seq uint64) {
	// Track predecessors at each level
	prevs := [MAX_LEVELS]*node{}
	for i := range prevs {
		prevs[i] = m.head
	}

	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.Next[l] != nil && curr.Next[l].Key < key {
			curr = curr.Next[l]
		}
		prevs[l] = curr
	}

	// Check if the key already exists at level 0
	curr = curr.Next[0]
	if curr != nil && curr.Key == key {
		// Update in place
		m.sizeBytes -= unsafe.Sizeof(*curr) + uintptr(len(curr.Key)) + uintptr(len(curr.Value))

		curr.Value = value
		curr.Seq = seq
		curr.Tombstone = false

		m.sizeBytes += unsafe.Sizeof(*curr) + uintptr(len(curr.Key)) + uintptr(len(curr.Value))

		return
	}

	// New key: create and stitch in at every level
	level := m.randomLevel()
	node := newNode(key, value, level, seq, false)
	m.height = max(m.height, level)

	for i := 0; i < level; i++ {
		node.Next[i] = prevs[i].Next[i]
		prevs[i].Next[i] = node
	}

	m.sizeBytes += unsafe.Sizeof(*node) + uintptr(len(key)) + uintptr(len(value))
}

func (m *Memtable) Delete(key string, seq uint64) {
	// Track predecessors at each level
	prevs := [MAX_LEVELS]*node{}
	for i := range prevs {
		prevs[i] = m.head
	}

	curr := m.head
	for l := m.height - 1; l >= 0; l-- {
		for curr.Next[l] != nil && curr.Next[l].Key < key {
			curr = curr.Next[l]
		}
		prevs[l] = curr
	}

	// Check if the key already exists at level 0
	curr = curr.Next[0]
	if curr != nil && curr.Key == key {
		// Update in place
		m.sizeBytes -= uintptr(len(curr.Value))

		curr.Value = []byte{}
		curr.Seq = seq
		curr.Tombstone = true

		return
	}

	// New key: create and stitch in at every level
	level := m.randomLevel()
	tombstone := newNode(key, []byte{}, level, seq, true)
	m.height = max(m.height, level)

	for i := 0; i < level; i++ {
		tombstone.Next[i] = prevs[i].Next[i]
		prevs[i].Next[i] = tombstone
	}

	m.sizeBytes += unsafe.Sizeof(*tombstone) + uintptr(len(key))
}

func (m *Memtable) randomLevel() int {
	level := 1
	for rand.Float64() < 0.5 && level < MAX_LEVELS {
		level++
	}
	return level
}
