package runnel

import "github.com/edsrzf/mmap-go"
import "unsafe"

type StreamHeader struct {
	FileSize   uint64
	EntryCount uint64
	EntrySize  uint64
}

func toHeader(data mmap.MMap) *StreamHeader {
	return (*StreamHeader)(unsafe.Pointer(&data[0]))
}
