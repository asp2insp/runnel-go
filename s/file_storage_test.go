package s

import (
	"os"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

func TestInit(t *testing.T) {
	store := NewFileStorage("")
	store.Init("id")

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
}
