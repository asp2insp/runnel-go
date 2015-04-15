package s

import (
	"os"
	"path/filepath"
	"unsafe"

	"github.com/asp2insp/go-misc/utils"
	"github.com/asp2insp/runnel-go/i"
	"github.com/edsrzf/mmap-go"
)

type fileStorage struct {
	fileId       string
	rootPath     string
	file         *os.File
	headerFile   *os.File
	mappedMemory mmap.MMap
	headerMemory mmap.MMap
	header       *i.StreamHeader
}

func NewFileStorage(root string) *fileStorage {
	return &fileStorage{
		rootPath: root,
	}
}

// STORAGE
func (store *fileStorage) Init(id string) i.Storage {
	store.fileId = id

	// Init the header
	headerSize := int64(unsafe.Sizeof(&i.StreamHeader{}))
	store.headerFile = open(fheader(store.fileId, store.rootPath), os.O_RDWR|os.O_CREATE|os.O_APPEND)
	store.headerFile.Truncate(headerSize)
	store.headerMemory = mmapFile(store.headerFile, mmap.RDWR)
	store.header = mmapToHeader(store.headerMemory)

	// Init the data
	store.file = open(fname(store.fileId, store.rootPath), os.O_RDWR|os.O_CREATE|os.O_APPEND)
	store.mappedMemory = mmapFile(store.file, mmap.RDWR)
	store.header.FileSize = utils.Filesize(store.file)
	return store
}

func (store *fileStorage) Resize(size uint64) i.Storage {
	if store.file != nil {
		err := store.file.Truncate(int64(size))
		utils.Check(err)
	}
	// Re-map our data
	tmpMap := store.mappedMemory
	store.mappedMemory = mmapFile(store.file, mmap.RDWR)
	if len(tmpMap) > 0 {
		tmpMap.Unmap()
	}
	store.header.FileSize = size
	return store
}

func (store *fileStorage) GetBytes(start, end uint64) []byte {
	return store.mappedMemory[start:end]
}

func (store *fileStorage) Capacity() uint64 {
	return store.header.FileSize
}

func (store *fileStorage) Header() *i.StreamHeader {
	return store.header
}

func (store *fileStorage) Utilization() int {
	cap := store.Capacity()
	if cap > 0 {
		return int(store.header.Tail * 100 / cap)
	} else {
		return 0
	}
}

func (store *fileStorage) Flush() {
	store.mappedMemory.Flush()
	store.headerMemory.Flush()
}

func (store *fileStorage) Refresh() {
	tmpMap := store.mappedMemory
	store.mappedMemory = mmapFile(store.file, mmap.RDWR)
	if len(tmpMap) > 0 {
		tmpMap.Unmap()
	}
	store.header.FileSize = utils.Filesize(store.file)
}

// CLOSABLE

// Close this storage, by closing the file
// pointers and unmapping all memory
func (store *fileStorage) Close() {
	store.header = &i.StreamHeader{} // Empty the header so calls to Size() return 0
	// Release the memory
	store.mappedMemory.Unmap()
	// store.file.Close()
	store.headerMemory.Unmap()
	// store.headerFile.Close()
}

// UTILS

// Open the given file with the given flags
func open(path string, fileFlags int) *os.File {
	file, err := os.OpenFile(path, fileFlags, 0666)
	utils.Check(err)
	if utils.Filesize(file) == 0 {
		err = file.Truncate(int64(os.Getpagesize()))
		utils.Check(err)
	}
	return file
}

// Map the file at the given path into memory with the given flags.
// Panics if the given file cannot be opened or mmapped
func mmapFile(file *os.File, mmapFlags int) mmap.MMap {
	mapData, err := mmap.Map(file, mmapFlags, 0)
	utils.Check(err)
	return mapData
}

// Return a path to the file named with the given id.
// If a root dir is provided, the file will be relative
// to that root. Otherwise it is placed in the tmpdir
func fname(id, root string) string {
	if root != "" {
		return filepath.Join(root, id)
	} else {
		return filepath.Join(os.TempDir(), id)
	}
}

// Return a path to the header file for the given id.
// Will always be co-located with the file returned by fname
func fheader(id, root string) string {
	return fname(id, root) + "_header"
}

// Unsafe cast the []byte represented by the mmapped region
// to a streamHeader
func mmapToHeader(data mmap.MMap) *i.StreamHeader {
	return (*i.StreamHeader)(unsafe.Pointer(&data[0]))
}
