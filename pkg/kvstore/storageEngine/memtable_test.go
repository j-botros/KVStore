package storageengine

import (
	"testing"
)

func TestNewNode(t *testing.T) {
	key := "mykey"
	val := []byte("myval")
	level := 5
	seq := uint64(42)
	tombstone := true
	n := newNode(key, val, level, seq, tombstone)

	if n.key != "mykey" {
		t.Errorf("key = %q, want %q", n.key, "mykey")
	}
	if string(n.value) != "myval" {
		t.Errorf("value = %q, want %q", n.value, "myval")
	}
	if n.level != 5 {
		t.Errorf("level = %d, want 5", n.level)
	}
	if n.seq != 42 {
		t.Errorf("seq = %d, want 42", n.seq)
	}
	if !n.tombstone {
		t.Error("tombstone = false, want true")
	}

	// All next pointers should be nil for a freshly created node.
	for i, ptr := range n.next {
		if ptr != nil {
			t.Errorf("next[%d] is non-nil", i)
		}
	}
}

func TestNewMemtable(t *testing.T) {
	m := newMemtable()

	if m.head == nil {
		t.Fatal("head is nil")
	}
	if m.height != 1 {
		t.Errorf("height = %d, want 1", m.height)
	}
	if m.sizeBytes != 0 {
		t.Errorf("sizeBytes = %d, want 0", m.sizeBytes)
	}
}

func TestMemtable_InsertAndGet(t *testing.T) {
	m := newMemtable()

	entries := []struct {
		key string
		val string
	}{
		{"banana", "yellow"},
		{"apple", "red"},
		{"cherry", "dark-red"},
	}

	for i, e := range entries {
		key := e.key
		val := []byte(e.val)
		seq := uint64(i + 1)
		m.insert(key, val, seq)
	}

	for _, e := range entries {
		key := e.key
		val, err := m.get(key)
		if err != nil {
			t.Errorf("get(%q): unexpected error: %v", e.key, err)
			continue
		}
		if string(val) != e.val {
			t.Errorf("get(%q) = %q, want %q", e.key, val, e.val)
		}
	}
}

func TestMemtable_Get_NotFound(t *testing.T) {
	m := newMemtable()
	key1 := "exists"
	val1 := []byte("v")
	seq1 := uint64(1)
	m.insert(key1, val1, seq1)

	key2 := "nonexistent"
	_, err := m.get(key2)
	if err != ErrKeyNotFound {
		t.Errorf("get(\"nonexistent\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_Get_EmptyTable(t *testing.T) {
	m := newMemtable()

	key := "anything"
	_, err := m.get(key)
	if err != ErrKeyNotFound {
		t.Errorf("get on empty memtable: err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_InsertOverwrite(t *testing.T) {
	m := newMemtable()
	key1 := "key"
	val1 := []byte("first")
	seq1 := uint64(1)
	m.insert(key1, val1, seq1)

	key2 := "key"
	val2 := []byte("second")
	seq2 := uint64(2)
	m.insert(key2, val2, seq2)

	keyGet := "key"
	val, err := m.get(keyGet)
	if err != nil {
		t.Fatalf("get(\"key\"): %v", err)
	}
	if string(val) != "second" {
		t.Errorf("get(\"key\") = %q, want %q", val, "second")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := newMemtable()
	key1 := "key"
	val1 := []byte("val")
	seq1 := uint64(1)
	m.insert(key1, val1, seq1)

	key2 := "key"
	seq2 := uint64(2)
	m.delete(key2, seq2)

	keyGet := "key"
	_, err := m.get(keyGet)
	if err != ErrKeyNotFound {
		t.Errorf("get after delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_DeleteNonExistent(t *testing.T) {
	m := newMemtable()
	// Deleting a key that was never inserted should create a tombstone.
	key1 := "ghost"
	seq1 := uint64(1)
	m.delete(key1, seq1)

	keyGet := "ghost"
	_, err := m.get(keyGet)
	if err != ErrKeyNotFound {
		t.Errorf("get(\"ghost\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_ReinsertAfterDelete(t *testing.T) {
	m := newMemtable()
	key1 := "key"
	val1 := []byte("v1")
	seq1 := uint64(1)
	m.insert(key1, val1, seq1)

	keyDel := "key"
	seqDel := uint64(2)
	m.delete(keyDel, seqDel)

	key2 := "key"
	val2 := []byte("v2")
	seq2 := uint64(3)
	m.insert(key2, val2, seq2)

	keyGet := "key"
	val, err := m.get(keyGet)
	if err != nil {
		t.Fatalf("get after re-insert: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("get(\"key\") = %q, want %q", val, "v2")
	}
}

func TestMemtable_OrderedTraversal(t *testing.T) {
	m := newMemtable()
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for i, k := range keys {
		key := k
		val := []byte(k)
		seq := uint64(i + 1)
		m.insert(key, val, seq)
	}

	// Level-0 forward pointers must be sorted lexicographically.
	expected := []string{"alpha", "bravo", "charlie", "delta"}
	curr := m.head.next[0]
	for i, exp := range expected {
		if curr == nil {
			t.Fatalf("expected node %d (%q), got nil", i, exp)
		}
		if curr.key != exp {
			t.Errorf("node %d: key = %q, want %q", i, curr.key, exp)
		}
		curr = curr.next[0]
	}
	if curr != nil {
		t.Errorf("unexpected extra node after traversal: %q", curr.key)
	}
}

func TestMemtable_SizeTracking(t *testing.T) {
	m := newMemtable()
	initial := m.sizeBytes

	// Insert: sizeBytes must grow.
	key1 := "key"
	val1 := []byte("value")
	seq1 := uint64(1)
	m.insert(key1, val1, seq1)
	afterInsert := m.sizeBytes
	if afterInsert <= initial {
		t.Errorf("sizeBytes should increase after insert: %d <= %d", afterInsert, initial)
	}

	// Overwrite with a shorter value: sizeBytes must shrink.
	key2 := "key"
	val2 := []byte("v")
	seq2 := uint64(2)
	m.insert(key2, val2, seq2)
	afterUpdate := m.sizeBytes
	if afterUpdate >= afterInsert {
		t.Errorf("sizeBytes should decrease after update with shorter value: %d >= %d", afterUpdate, afterInsert)
	}

	// Delete (tombstone clears value to []byte{}): sizeBytes must shrink further.
	key3 := "key"
	seq3 := uint64(3)
	m.delete(key3, seq3)
	afterDelete := m.sizeBytes
	if afterDelete >= afterUpdate {
		t.Errorf("sizeBytes should decrease after delete: %d >= %d", afterDelete, afterUpdate)
	}
}

func TestRandomLevel(t *testing.T) {
	m := newMemtable()
	for i := 0; i < 1000; i++ {
		level := m.randomLevel()
		if level < 1 || level > MAX_LEVELS {
			t.Errorf("randomLevel() = %d, want [1, %d]", level, MAX_LEVELS)
		}
	}
}
