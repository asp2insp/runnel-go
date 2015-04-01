package runnel

import "testing"
import "github.com/asp2insp/go-misc/testutils"
import "os"

//go:generate genny -in=runnel.go -out=IntStream.go gen "Typed=int"

func TestInsertIncrementsSize(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.insert(&data1)
	stream.insert(&data2)

	testutils.CheckUint64(2, stream.Size, t)
}

func TestInsertUpdatesInputHeader(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.insert(&data1)
	stream.insert(&data2)

	testutils.CheckUint64(2, stream.in.streamHeader.EntryCount, t)
	testutils.CheckUint64(8, stream.in.streamHeader.EntrySize, t)
}

func TestInsertUpdatesOutputHeader(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.insert(&data1)
	stream.insert(&data2)

	testutils.CheckUint64(2, stream.out.streamHeader.EntryCount, t)
	testutils.CheckUint64(8, stream.out.streamHeader.EntrySize, t)
}

func TestCreateSetsHeaderSize(t *testing.T) {
	stream := NewIntStream("test")
	testutils.CheckUint64(uint64(os.Getpagesize()), stream.in.streamHeader.FileSize, t)
}

func TestRoundTripData(t *testing.T) {
	stream := NewIntStream("test")
	for i := 0; i < 100; i++ {
		stream.insert(&i)
	}

	var ref *IntRef
	for i := 0; i < 100; i++ {
		ref = &IntRef{stream.fileId, uint64(i * 8)}
		testutils.CheckInt(i, *stream.out.resolve(ref), t)
	}
}

func TestPageIncrement(t *testing.T) {
	stream := NewIntStream("large")
	// 4096 / 8 = 512
	for i := 0; i < 513; i++ {
		stream.insert(&i)
	}
	// Should have overflowed onto another page
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.in.streamHeader.FileSize, t)

	var ref *IntRef
	for i := 0; i < 513; i++ {
		ref = &IntRef{stream.fileId, uint64(i * 8)}
		testutils.CheckInt(i, *stream.out.resolve(ref), t)
	}
	// Should see output header updated
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.out.streamHeader.FileSize, t)
}
