package runnel

import (
	"os"
	"sync"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

//go:generate genny -in=runnel.go -out=IntStream.go gen "Typed=int"

func TestCreation(t *testing.T) {
	cleanupFiles()
	var stream *IntStream = NewIntStream("test", "id", nil)
	defer stream.Close()
}

func TestMakeWriter(t *testing.T) {
	cleanupFiles()
	var stream *IntStream = NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()
}

func TestMakeReader(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var reader *IntStreamReader = stream.Reader(0 /* from beginning */)
	defer reader.Close()
}

func TestWrite(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	testutils.ExpectTrue(writer.isAlive, "Writer should be alive", t)
	testutils.ExpectTrue(stream.IsAlive, "Stream should be alive", t)

	data := 5
	writer.Write(&data)
}

func TestWriteIncrementsSize(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	data1 := 45
	writer := stream.Writer()
	defer writer.Close()

	testutils.ExpectTrue(writer.isAlive, "Writer should be alive", t)
	testutils.ExpectTrue(stream.IsAlive, "Stream should be alive", t)
	writer.Write(&data1)

	testutils.CheckUint64(1, stream.Size(), t)
}

func TestDataRoundTrip(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
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
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	var data1 int = 45
	var data2 int = 66
	writer.Write(&data1)
	writer.Write(&data2)

	testutils.CheckUint64(2, stream.Size(), t)
	testutils.CheckUint64(16, stream.header().Tail, t)
	testutils.CheckUint64(16, stream.header().LastMessage, t)
	stream.Close()
}

func TestRoundTripMulti(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	for i := 0; i < 100; i++ {
		writer.Write(&i)
	}
	testutils.CheckUint64(100, stream.Size(), t)

	var reader *IntStreamReader = stream.Reader(0) // from beginning
	defer reader.Close()

	for i := 0; i < 100; i++ {
		testutils.CheckInt(i, reader.Read(), t)
	}
}

func TestPageIncrement(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	// 4096 / 8 = 512
	for i := 0; i < 513; i++ {
		writer.Write(&i)
	}
	testutils.CheckUint64(513, stream.Size(), t)

	var reader *IntStreamReader = stream.Reader(0) // from beginning
	defer reader.Close()

	for i := 0; i < 513; i++ {
		testutils.CheckInt(i, reader.Read(), t)
	}
	// Should see output header updated
	testutils.CheckUint64(uint64(os.Getpagesize()*2), stream.header().FileSize, t)
}

func TestStartReadFromMidway(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	// 4096 / 8 = 512
	for i := 0; i < 513; i++ {
		writer.Write(&i)
	}
	testutils.CheckUint64(513, stream.Size(), t)

	var reader *IntStreamReader = stream.Reader(250 * 8) // from middle
	defer reader.Close()

	for i := 250; i < 513; i++ {
		testutils.CheckInt(i, reader.Read(), t)
	}
}

func TestSingleWriterSingleReader(t *testing.T) {
	cleanupFiles()
	stream := NewIntStream("test", "id", nil)
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		var writer *IntStreamWriter = stream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, stream.Size(), t)
		wg.Done()
	}()

	go func() {
		var reader *IntStreamReader = stream.Reader(0) // from beginning
		defer reader.Close()

		for i := 0; i < 513; i++ {
			datum := reader.Read()
			testutils.CheckInt(i, datum, t)
		}
		wg.Done()
	}()

	wg.Wait()
}

/*
func TestSingleWriterMultipleReaders(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(1 + 10)

	go func() {
		var writer *IntStreamWriter = stream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, stream.Size(), t)
		wg.Done()
	}()

	for r := 0; r < 10; r++ {
		go func() {
			var reader *IntStreamReader = stream.Reader(0) // from beginning
			defer reader.Close()

			for i := 0; i < 513; i++ {
				testutils.CheckInt(i, reader.Read(), t)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestSingleWriterMultipleHungryReaders(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(1 + 10)

	for r := 0; r < 10; r++ {
		go func() {
			var reader *IntStreamReader = stream.Reader(0) // from beginning
			defer reader.Close()

			for i := 0; i < 513; i++ {
				testutils.CheckInt(i, reader.Read(), t)
			}
			wg.Done()
		}()
	}

	go func() {
		var writer *IntStreamWriter = stream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, stream.Size(), t)
		wg.Done()
	}()

	wg.Wait()
}

func TestMultipleWritersSingleReader(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(1 + 10)

	go func() {
		var reader *IntStreamReader = stream.Reader(0) // from beginning
		defer reader.Close()
		var target = 513 * 10 * 3

		for i := 0; i < 513*10; i++ {
			target -= reader.Read()
		}
		testutils.CheckInt(0, target, t)
		wg.Done()
	}()

	for w := 0; w < 10; w++ {
		go func() {
			var writer *IntStreamWriter = stream.Writer()
			defer writer.Close()
			var amount = 3
			// 4096 / 8 = 512
			for i := 0; i < 513; i++ {
				writer.Write(&amount)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	testutils.CheckUint64(513*10, stream.Size(), t)
}

func TestMultipleWritersMultipleReaders(t *testing.T) {
	stream := NewIntStream("test", "", nil)
	defer stream.Close()
	var wg sync.WaitGroup
	wg.Add(10 + 10)

	for r := 0; r < 10; r++ {
		go func() {
			var reader *IntStreamReader = stream.Reader(0) // from beginning
			defer reader.Close()
			var target = 513 * 10 * 3

			for i := 0; i < 513*10; i++ {
				target -= reader.Read()
			}
			testutils.CheckInt(0, target, t)
			wg.Done()
		}()
	}

	for w := 0; w < 10; w++ {
		go func() {
			var writer *IntStreamWriter = stream.Writer()
			defer writer.Close()
			var amount = 3
			// 4096 / 8 = 512
			for i := 0; i < 513; i++ {
				writer.Write(&amount)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	testutils.CheckUint64(513*10, stream.Size(), t)
}
*/
