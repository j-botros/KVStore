package storageengine

type StorageEngine struct {
	memtable Memtable
	nextSeq  uint64
}

func (e *StorageEngine) Get(key string) ([]byte, error) {
	// Check Memtable for key-value
	value, err := e.memtable.Get(key)
	if value != nil && err == nil {
		return value, err
	} else if value == nil {
		return nil, ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	// TODO: Check SSTables for key-value

}

func (e *StorageEngine) Put(key string, value []byte) {
	// TODO: Update WAL

	// Push to Memtable
	e.memtable.Insert(key, value, e.nextSeq)
	e.nextSeq++
}

func (e *StorageEngine) Delete(key string) {
	// TODO: Update WAL

	// Delete from Memtable
	e.memtable.Delete(key, e.nextSeq)
	e.nextSeq++
}
