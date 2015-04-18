package i

type StreamHeader struct {
	FileSize   uint64
	EntryCount uint64
	// One past the end
	Tail uint64
	// One past the end
	LastMessage uint64
}

type Storage interface {
	// Allocate memory and open the storage
	Init(id string) Storage
	// Resize the storage to the given size
	Resize(newSize uint64) Storage
	// Get a window into the storage. This window is not owned by
	// the client and the memory backing it may disappear.
	// DO NOT HOLD ONTO THIS REFERENCE.
	GetBytes(start, end uint64) []byte
	ReturnBytes(slice []byte)
	// Get the current capacity (in bytes, not items)
	Capacity() uint64
	// Get the current number of entries (messages)
	Header() *StreamHeader
	// Get the current utilization (size used/capacity)
	// Returns an integer percentage out of 100 for performance
	// reasons
	Utilization() int
	// Close the storage, release all references
	Close()
	// Flush the memory contents to underlying medium
	Flush()
	// Refresh the in-memory version of the underlying medium
	Refresh()
}

type Closable interface {
	Close()
}

// TODO: Add network and serialization types
