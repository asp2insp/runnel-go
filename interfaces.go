package runnel

type StreamHeader struct {
	FileSize    uint64
	EntryCount  uint64
	Tail        uint64
	LastMessage uint64
}

type Storage interface {
	// Allocate memory and open the storage
	Init(id string) *storage
	// Resize the storage to the given size
	Resize(newSize int64) *storage
	// Get a window into the storage. This window is not owned by
	// the client and the memory backing it may disappear.
	// DO NOT HOLD ONTO THIS REFERENCE
	GetBytes(start, end int64) []byte
	// Get the current capacity
	Capacity() uint64
	// Get the current number of entries (messages)
	Header() *StreamHeader
	// Get the current utilization (size used/capacity)
	// Returns an integer percentage out of 100 for performance
	// reasons
	Utilization() int
}

type Closable interface {
	Close()
}

// TODO: Add network and serialization types
