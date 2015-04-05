package runnel

import (
	"os"
	"path/filepath"
	"unsafe"

	"github.com/asp2insp/go-misc/utils"
	"github.com/edsrzf/mmap-go"
)

type fileStorage struct {
	fileId       string
	rootPath     string
	file         *os.File
	headerFile   *os.File
	mappedMemory mmap.MMap
	headerMemory mmap.MMap
	header       *StreamHeader
	capacity     uint64
}

func NewFileStorage(id, root string) *fileStorage {
	return &fileStorage{
		fileId:   id,
		rootPath: root,
	}
}

// STORAGE
func (store *fileStorage) Init(id string) *Storage {
	// Map in the data
	store.Resize(os.Getpagesize())

	// Update the header
	headerSize = unsafe.Sizeof(&streamHeader{})
	store.headerMem, store.headerFile = mmapFile(store.fheader(), size, os.O_RDWR|os.O_CREATE, mmap.RDWR)
	store.header = toHeader(store.headerMem)
}

func (store *fileStorage) Resize(size int64) *Storage {
	if
	err := store.file.Truncate(int64(size))
	utils.Check(err)

	// Re-map our data
	tmpMap := store.mappedMemory
	tmpFile := store.file
	store.mappedMemory, store.file = mmapFile(store.file.Name(), size, os.O_APPEND|os.O_RDWR|os.O_CREATE, mmap.RDWR)
	if len(tmpMap) > 0 {
		tmpMap.Unmap()
	}
	if tmpFile != nil {
		tmpFile.Close()
	}
	store.capacity = size
}

func (store *fileStorage) GetBytes(start, end int) []byte {
	return make([]byte, 0, 0)
}

func (store *fileStorage) Capacity() uint64 {
	return store.capacity
}

func (store *fileStorage) Header() *StreamHeader {
	return store.header
}

func (store *fileStorage) Utilization() int {
	return store.header.Tail * 100 / (store.capacity * 100)
}

// CLOSABLE

// Close this storage, by closing the file
// pointers and unmapping all memory
func (store *fileStorage) Close() {
	store.header = &streamHeader{} // Empty the header so calls to Size() return 0
	// Release the memory
	store.mappedMemory.Unmap()
	store.file.Close()
	store.headerMemory.Unmap()
	store.headerFile.Close()
}

// UTILS

// Map the file at the given path into memory with the given flags.
// Panics if the given file cannot be opened or mmapped
func mmapFile(path string, size, fileFlags, mmapFlags int) (mmap.MMap, *os.File) {
	file, err := os.OpenFile(path, fileFlags, 0666)
	utils.Check(err)
	if utils.Filesize(file) == 0 {
		err = file.Truncate(os.Getpagesize())
		utils.Check(err)
	}
	mapData, err := mmap.Map(file, mmapFlags, 0)
	utils.Check(err)
	return mapData, file
}

// Return a path to the file named with the given id.
// If a root dir is provided, the file will be relative
// to that root. Otherwise it is placed in the tmpdir
func fname(id, root, string) string {
	if root != "" {
		return filepath.Join(root, s.fileId)
	} else {
		return filepath.Join(os.TempDir(), s.fileId)
	}
}

// Return a path to the header file for the given id.
// Will always be co-located with the file returned by fname
func fheader(id, root string) string {
	return fname() + "_header"
}

// Unsafe cast the []byte represented by the mmapped region
// to a streamHeader
func mmapToHeader(data mmap.MMap) *StreamHeader {
	return (*StreamHeader)(unsafe.Pointer(&data[0]))
}
