package runnel

import (
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

//go:generate genny -in=runnel.go -out=IntStream.go gen "Typed=int"

func TestCreation(t *testing.T) {
	var stream *IntStream = NewIntStream("test", "", nil)
	defer stream.Close()
}

func TestMakeWriter(t *testing.T) {
	var stream *IntStream = NewIntStream("test", "", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()
}

func TestMakeReader(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()

	var reader *IntStreamReader = stream.Reader(0 /* from beginning */)
	defer reader.Close()
}

func TestWrite(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	data := 5
	writer.Write(&data)
}

func TestWriteIncrementsSize(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()

	data1 := 45
	writer := stream.Writer()
	defer writer.Close()

	writer.Write(&data1)

	testutils.CheckUint64(1, stream.Size(), t)
}

/*
func TestDataRoundTrip(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	data := 5
	writer.Write(&data)

	var reader *IntStreamReader = stream.Reader(0) // from beginning
	defer reader.Close()

	out := reader.Read()
	testutils.CheckInt(5, out, t)
}





func TestInsertUpdatesInputHeader(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckUint64(2, stream.header.EntryCount, t)
	testutils.CheckUint64(8, stream.header.EntrySize, t)
	stream.Close()
}

func TestInsertUpdatesOutputHeader(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckUint64(2, stream.header.EntryCount, t)
	testutils.CheckUint64(8, stream.header.EntrySize, t)
	stream.Close()
}

func TestFindOne(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	data1 := 45
	data2 := 66
	stream.Insert(&data1)
	stream.Insert(&data2)

	testutils.CheckInt(45, *stream.out.resolve(stream.FindOne(0)), t)
	testutils.CheckInt(66, *stream.out.resolve(stream.FindOne(1)), t)
	stream.Close()
}

func TestCreateSetsHeaderSize(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	testutils.CheckUint64(uint64(os.Getpagesize()), stream.header.FileSize, t)
	stream.Close()
}

func TestRoundTripData(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	for i := 0; i < 100; i++ {
		stream.Insert(&i)
	}

	var ref *IntRef
	for i := 0; i < 100; i++ {
		ref = &IntRef{stream, uint64(i * 8)}
		testutils.CheckInt(i, *stream.out.resolve(ref), t)
	}
	stream.Close()
}

func TestPageIncrement(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	// 4096 / 8 = 512
	for i := 0; i <= 513; i++ {
		stream.Insert(&i)
	}
	// Should have overflowed onto another page
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.header.FileSize, t)

	var ref *IntRef
	for i := 0; i <= 513; i++ {
		ref = &IntRef{stream, uint64(i * 8)}
		testutils.CheckInt(i, *stream.out.resolve(ref), t)
	}
	// Should see output header updated
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.header.FileSize, t)
	stream.Close()
}

func TestSingleOutputToChannel(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	c := make(chan int)
	stream.Outlet(c)

	data := 5
	stream.Insert(&data)

	out := <-c
	testutils.CheckInt(5, out, t)
	stream.Close()
}

func TestDoubleOutputToChannel(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	c := make(chan int)
	stream.Outlet(c)

	data := 5
	stream.Insert(&data)
	data = 10
	stream.Insert(&data)

	out := <-c
	testutils.CheckInt(5, out, t)
	out = <-c
	testutils.CheckInt(10, out, t)
	stream.Close()
}

func TestOutputToChannel(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	done := make(chan struct{})

	go func() {
		c := make(chan int)
		stream.Outlet(c)
		var d int
		for {
			d = <-c
			if d == 513 {
				done <- struct{}{}
				break
			}
		}
	}()

	for i := 0; i <= 513; i++ {
		stream.Insert(&i)
	}

	// Wait for the subroutine to finish
	<-done
	stream.Close()
}

func TestOutputToMultipleChannels(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	done := make(chan struct{})

	for i := 0; i < 5; i++ {
		go func() {
			c := make(chan int)
			stream.Outlet(c)
			var d int
			for {
				d = <-c
				if d == 513 {
					done <- struct{}{}
					break
				}
			}
		}()
	}

	for i := 0; i <= 513; i++ {
		stream.Insert(&i)
	}

	// Wait for all subroutines to finish
	for i := 0; i < 5; i++ {
		<-done
	}
	stream.Close()
}

func TestOutputToChannelStartsFromBeginning(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	data := 55
	stream.Insert(&data)
	data = 66
	stream.Insert(&data)

	c := make(chan int)
	stream.Outlet(c)

	d := <-c
	testutils.CheckInt(55, d, t)
	d = <-c
	testutils.CheckInt(66, d, t)

	stream.Close()
}
*/
