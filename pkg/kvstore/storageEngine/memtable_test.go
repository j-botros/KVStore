package storageengine

import (
	"testing"
	"unsafe"
)

func TestNewNode(t *testing.T) {
	n := newNode("mykey", []byte("myval"), 5, 42, true)

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
	m := newMemtable(1024)

	if m.head == nil {
		t.Fatal("head is nil")
	}
	if m.height != 1 {
		t.Errorf("height = %d, want 1", m.height)
	}
	if m.capacityBytes != 1024 {
		t.Errorf("capacityBytes = %d, want 1024", m.capacityBytes)
	}

	expectedSize := unsafe.Sizeof(node{})
	if m.sizeBytes != expectedSize {
		t.Errorf("sizeBytes = %d, want %d", m.sizeBytes, expectedSize)
	}
}

func TestMemtable_InsertAndGet(t *testing.T) {
	m := newMemtable(4096)

	entries := []struct {
		key string
		val string
	}{
		{"banana", "yellow"},
		{"apple", "red"},
		{"cherry", "dark-red"},
	}

	for i, e := range entries {
		m.insert(e.key, []byte(e.val), uint64(i+1))
	}

	for _, e := range entries {
		val, err := m.get(e.key)
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
	m := newMemtable(4096)
	m.insert("exists", []byte("v"), 1)

	_, err := m.get("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("get(\"nonexistent\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_Get_EmptyTable(t *testing.T) {
	m := newMemtable(4096)

	_, err := m.get("anything")
	if err != ErrKeyNotFound {
		t.Errorf("get on empty memtable: err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_InsertOverwrite(t *testing.T) {
	m := newMemtable(4096)
	m.insert("key", []byte("first"), 1)
	m.insert("key", []byte("second"), 2)

	val, err := m.get("key")
	if err != nil {
		t.Fatalf("get(\"key\"): %v", err)
	}
	if string(val) != "second" {
		t.Errorf("get(\"key\") = %q, want %q", val, "second")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := newMemtable(4096)
	m.insert("key", []byte("val"), 1)

	m.delete("key", 2)

	_, err := m.get("key")
	if err != ErrKeyNotFound {
		t.Errorf("get after delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_DeleteNonExistent(t *testing.T) {
	m := newMemtable(4096)
	// Deleting a key that was never inserted should create a tombstone.
	m.delete("ghost", 1)

	_, err := m.get("ghost")
	if err != ErrKeyNotFound {
		t.Errorf("get(\"ghost\"): err = %v, want ErrKeyNotFound", err)
	}
}

func TestMemtable_ReinsertAfterDelete(t *testing.T) {
	m := newMemtable(4096)
	m.insert("key", []byte("v1"), 1)
	m.delete("key", 2)
	m.insert("key", []byte("v2"), 3)

	val, err := m.get("key")
	if err != nil {
		t.Fatalf("get after re-insert: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("get(\"key\") = %q, want %q", val, "v2")
	}
}

func TestMemtable_OrderedTraversal(t *testing.T) {
	m := newMemtable(4096)
	keys := []string{"delta", "alpha", "charlie", "bravo"}
	for i, k := range keys {
		m.insert(k, []byte(k), uint64(i+1))
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
	m := newMemtable(4096)
	initial := m.sizeBytes

	// Insert: sizeBytes must grow.
	m.insert("key", []byte("value"), 1)
	afterInsert := m.sizeBytes
	if afterInsert <= initial {
		t.Errorf("sizeBytes should increase after insert: %d <= %d", afterInsert, initial)
	}

	// Overwrite with a shorter value: sizeBytes must shrink.
	m.insert("key", []byte("v"), 2)
	afterUpdate := m.sizeBytes
	if afterUpdate >= afterInsert {
		t.Errorf("sizeBytes should decrease after update with shorter value: %d >= %d", afterUpdate, afterInsert)
	}

	// Delete (tombstone clears value to []byte{}): sizeBytes must shrink further.
	m.delete("key", 3)
	afterDelete := m.sizeBytes
	if afterDelete >= afterUpdate {
		t.Errorf("sizeBytes should decrease after delete: %d >= %d", afterDelete, afterUpdate)
	}
}

func TestRandomLevel(t *testing.T) {
	m := newMemtable(4096)
	for i := 0; i < 1000; i++ {
		level := m.randomLevel()
		if level < 1 || level > MAX_LEVELS {
			t.Errorf("randomLevel() = %d, want [1, %d]", level, MAX_LEVELS)
		}
	}
}
