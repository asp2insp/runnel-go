package s

import (
	"os"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

var testData = []byte("0123456789ABCDEF")

func TestInit(t *testing.T) {
	cleanup()
	store := NewFileStorage("")
	store.Init("id")
	defer store.Close()

	testutils.CheckString("id", store.fileId, t)
	testutils.CheckString("", store.rootPath, t)

	if store.file == nil {
		t.Errorf("No main file opened for %s", fname("id", ""))
	}
	if store.headerFile == nil {
		t.Errorf("No header file opened for %s", fheader("id", ""))
	}
	if store.Header() == nil {
		t.Error("No stream header available")
	}

	testutils.CheckUint64(0, store.Header().Tail, t)
	testutils.CheckUint64(0, store.Header().LastMessage, t)
	testutils.CheckUint64(uint64(os.Getpagesize()), store.Header().FileSize, t)
	testutils.CheckUint64(0, store.Header().EntryCount, t)
}

func TestPersistence(t *testing.T) {
	cleanup()
	store := NewFileStorage("").Init("id")
	copy(store.GetBytes(0, uint64(len(testData))), testData)
	store.Close()

	store = NewFileStorage("").Init("id")
	defer store.Close()
	if store.GetBytes(0, store.Capacity())[15] != 'F' {
		t.Errorf("Expected %b got %b", 'F', store.GetBytes(0, store.Capacity())[15])
	}
}

func TestUtilization(t *testing.T) {
	cleanup()
	store := NewFileStorage("")
	store.Init("id")
	defer store.Close()

	store.Header().Tail = 2048
	testutils.CheckInt(50, store.Utilization(), t)
}

func cleanup() {
	os.Remove(fname("id", ""))
	os.Remove(fheader("id", ""))
}
