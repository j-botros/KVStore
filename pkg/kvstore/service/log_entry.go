package service

// LogEntry represents a single write operation in the replication log.
// NodeClientAdapter translates it to the wire format when sending to followers.
type LogEntry struct {
	SeqNum    uint64 // monotonically increasing log index
	Operation string // "PUT" | "DELETE"
	Key       string
	Value     []byte // empty for DELETE
}
