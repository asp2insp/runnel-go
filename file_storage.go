package runnel

import (
	"os"
	"path/filepath"

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
}

func NewFileStorage(id, root string) *fileStorage {
	ret := &fileStorage{
		fileId: id,
	}
}

// STORAGE
func (store *fileStorage) Init(id string) *Storage {
	ret.headerMem, ret.headerFile = mmapFile(ret.fheader(), os.O_RDWR|os.O_CREATE, mmap.RDWR)
	ret.header = toHeader(ret.headerMem)
}

func (store *fileStorage) Resize(size int64) *Storage {
	err := store.file.Truncate(int64(size))
	utils.Check(err)

	// Re-map our data
	tmpMap := store.mappedMemory
	tmpFile := store.file
	store.mappedMemory, store.file = mmapFile(store.file.Name(), os.O_APPEND|os.O_RDWR|os.O_CREATE, mmap.RDWR)
	tmpMap.Unmap()
	tmpFile.Close()

	store.header.FileSize = size
}

func (store *fileStorage) GetBytes(start, end int) []byte {
	return make([]byte, 0, 0)
}

func (store *fileStorage) Size() uint64 {
	return 0
}

func (store *fileStorage) EntryCount() uint64 {
	return 0
}

// CLOSABLE
func (store *fileStorage) Close() {

}

// UTILS

func mmapFile(path string, fileFlags int, mmapFlags int) (mmap.MMap, *os.File) {
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

/**
 * Return a path to the file named with the given id.
 * If a root dir is provided, the file will be relative
 * to that root. Otherwise it is placed in the tmpdir
 */
func fname(id, root, string) string {
	if root != "" {
		return filepath.Join(root, s.fileId)
	} else {
		return filepath.Join(os.TempDir(), s.fileId)
	}
}

/**
 * Return a path to the header file for the given id.
 * Will always be co-located with the file returned by fname
 */
func fheader(id, root string) string {
	return fname() + "_header"
}
