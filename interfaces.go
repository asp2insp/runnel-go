package runnel

type Storage interface {
	Init(id string) *storage
	Resize(newSize int64) *storage
	GetBytes(start, end int64) []byte
	Size() uint64
	EntryCount() uint64
}

type Closable interface {
	Close()
}

// TODO: Add network and serialization types
