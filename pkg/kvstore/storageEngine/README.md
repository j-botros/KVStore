# Storage Engine

The `storageEngine` package is the core of KVStore. It implements a highly concurrent **Log-Structured Merge (LSM) Tree** architecture optimized for write-heavy workloads, fast point-reads, and efficient background maintenance.

## High-Level Data Flow

### Writes (`Put` & `Delete`)
1. **Write-Ahead Log (WAL)**: When a write or delete request arrives, the operation is immediately serialized and appended to an on-disk WAL file to guarantee durability. 
2. **Memtable Insertion**: After the WAL write succeeds, the operation is inserted into the active **Memtable** (an in-memory Skip List).
3. **Triggering Flush**: If the Memtable's size exceeds the configured capacity (`memCapacity`), a background goroutine is triggered to flush the data to disk.

### Reads (`Get`)
To return the most recent version of a key, the storage engine queries components in order of "newest" to "oldest":
1. **Active Memtable**: Checked first.
2. **Immutable Memtables**: Checked from newest to oldest if a flush is currently in progress.
3. **SSTables (L0 -> Ln)**: Disk files are queried level-by-level. A **Bloom Filter** is checked first to quickly skip files that do not contain the key. If the bloom filter matches, a binary search is performed on the SSTable's index to locate the exact data block on disk.

### Flushes
1. **Rotation**: The active Memtable is rotated into a queue of *immutable memtables* and a fresh active Memtable and WAL are created.
2. **Disk I/O**: The immutable memtable is written to disk as a new Sorted String Table (SSTable) at **Level 0 (L0)**.
3. **Cleanup**: Once the SSTable is safely on disk, the immutable memtable is discarded and its associated WAL file is deleted.

### Compaction
- As SSTables accumulate in Level 0 and beyond, levels exceed their capacity limits.
- A background **Leveled Compaction** process merges overlapping SSTables from a lower level (e.g., Level 0) with a higher level (e.g., Level 1) into a new set of strictly-sorted SSTables.
- This process prunes deleted data (tombstones) at the final level and ensures that read amplification is kept to a minimum.

---

## File Structure & Components

### `storageEngine.go`
The orchestrator of the entire system. It ties the WAL, Memtable, and SSTables together safely under a unified `StorageEngine` struct.
- **Key Structs**: `StorageEngine`, `memlog` (a pairing of a memtable and a WAL).
- **Important Methods**:
  - `Put(key, value)` / `Delete(key)`: Handles concurrent-safe WAL appends and Memtable inserts.
  - `Get(key)`: Orchestrates the cascading read logic.
  - `Flush()`: Rotates the active memtable and safely converts it into an L0 SSTable.
  - `Compact(srcSst)`: Atomically merges an SSTable with its overlapping counterparts in the next level down.

### `memtable.go`
An in-memory, thread-safe (via external engine locks) Skip List implementation that maintains keys in sorted order.
- **Key Structs**: `memtable`, `node`.
- **Important Methods**: `insert()`, `get()`, `delete()`.

### `wal.go`
A simple append-only log that guarantees no data is lost if the system crashes before the Memtable is flushed to an SSTable.
- **Key Structs**: `wal`.
- **Important Methods**: `writeLog()` to safely sync operations to disk.

### `sstables.go`
The heavy-lifter for disk I/O, binary searching, and leveled compaction logic.
- **Key Structs**: 
  - `sstables`: Manages all levels and triggers compaction.
  - `level`: Represents a specific level (L0, L1, etc.) containing a list of `sst` files.
  - `sst`: Represents a single on-disk Sorted String Table, holding file descriptors, key ranges, and byte sizes.
  - `compactionSrc` & `compactionWriter`: Helpers that stream entries out of overlapping SSTs and write them into new, capacity-limited output SSTs.
- **Important Methods**:
  - `sstables.flush()`: Creates a new SSTable file from a memtable.
  - `sst.search()`: Checks the bloom filter and executes a binary search on the file's index blocks.
  - `sst.compact()`: The core two-pointer merge algorithm for merging multiple sorted SST streams.

### `bloomFilter.go`
A space-efficient probabilistic data structure used inside each SSTable to prevent unnecessary, expensive disk reads for keys that don't exist in that file.
- **Key Structs**: `bloomFilter`.

### `engineErrors.go`
Defines the package's sentinel errors, such as `ErrKeyNotFound`, which the API layer checks for to return `404 Not Found` HTTP codes.

### `*_test.go`
Extensive testing suite for the engine. `testhelpers_test.go` provides mock configurations, while the other files test concurrent flushes, deadlocks, compaction serialization, and basic CRUD correctness.
