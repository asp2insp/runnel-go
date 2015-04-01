package runnel

import "testing"
import "github.com/asp2insp/go-misc/testutils"
import "os"

//go:generate genny -in=runnel.go -out=IntStream.go gen "Typed=int"

func TestInsertIncrementsSize(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckUint64(2, stream.Size, t)
}

func TestInsertUpdatesInputHeader(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckUint64(2, stream.header.EntryCount, t)
	testutils.CheckUint64(8, stream.header.EntrySize, t)
}

func TestInsertUpdatesOutputHeader(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckUint64(2, stream.header.EntryCount, t)
	testutils.CheckUint64(8, stream.header.EntrySize, t)
}

func TestFindOne(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckInt(45, *stream.out.resolve(stream.FindOne(0)), t)
	testutils.CheckInt(66, *stream.out.resolve(stream.FindOne(1)), t)
}

func TestCreateSetsHeaderSize(t *testing.T) {
	stream := NewIntStream("test")
	testutils.CheckUint64(uint64(os.Getpagesize()), stream.header.FileSize, t)
}

func TestRoundTripData(t *testing.T) {
	stream := NewIntStream("test")
	for i := 0; i < 100; i++ {
		stream.Insert(&i)
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
	for i := 0; i <= 513; i++ {
		stream.Insert(&i)
	}
	// Should have overflowed onto another page
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.header.FileSize, t)

	var ref *IntRef
	for i := 0; i <= 513; i++ {
		ref = &IntRef{stream.fileId, uint64(i * 8)}
		testutils.CheckInt(i, *stream.out.resolve(ref), t)
	}
	// Should see output header updated
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.header.FileSize, t)
}

func TestSingleOutputToChannel(t *testing.T) {
	stream := NewIntStream("stream")
	c := make(chan *int)
	stream.Outlet(c)

	data := 5
	stream.Insert(&data)

	out := <-c
	testutils.CheckInt(5, *out, t)
	stream.Close()
}

func TestDoubleOutputToChannel(t *testing.T) {
	stream := NewIntStream("stream")
	c := make(chan *int)
	stream.Outlet(c)

	data := 5
	stream.Insert(&data)
	data = 10
	stream.Insert(&data)

	out := <-c
	testutils.CheckInt(5, *out, t)
	out = <-c
	testutils.CheckInt(10, *out, t)
}

func TestOutputToChannel(t *testing.T) {
	stream := NewIntStream("channel")
	done := make(chan struct{})

	go func() {
		c := make(chan *int)
		stream.Outlet(c)
		var d *int
		for {
			d = <-c
			if *d == 513 {
				done <- struct{}{}
			}
		}
	}()

	for i := 0; i <= 513; i++ {
		stream.Insert(&i)
	}

	// Wait for the subroutine to finish
	<-done
}
