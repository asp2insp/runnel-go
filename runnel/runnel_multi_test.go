package runnel

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

func TestMultiStreamWriteIncrementsSize(t *testing.T) {
	cleanupFiles()

	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	data1 := 45
	writer := stream.Writer()
	defer writer.Close()

	testutils.ExpectTrue(writer.isAlive, "Writer should be alive", t)
	testutils.ExpectTrue(stream.IsAlive, "Stream should be alive", t)
	writer.Write(&data1)

	stream2 := NewIntStream("test", "id", nil)
	defer stream2.Close()

	testutils.CheckUint64(1, stream.Size(), t)
	testutils.CheckUint64(1, stream2.Size(), t)
}

func TestMultiStreamDataRoundTrip(t *testing.T) {
	cleanupFiles()

	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	data := 5
	writer.Write(&data)

	stream2 := NewIntStream("test2", "id", nil)

	var reader *IntStreamReader = stream2.Reader(0) // from beginning
	defer reader.Close()

	out := reader.Read()
	testutils.CheckInt(5, out, t)
}

func TestMultiStreamInsertUpdatesInputHeader(t *testing.T) {
	cleanupFiles()

	stream := NewIntStream("input", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	var data1 int = 45
	var data2 int = 66
	writer.Write(&data1)
	writer.Write(&data2)

	stream2 := NewIntStream("output", "id", nil)
	defer stream2.Close()

	testutils.CheckUint64(2, stream2.Size(), t)
	testutils.CheckUint64(16, stream2.header().Tail, t)
	testutils.CheckUint64(16, stream2.header().LastMessage, t)
}

func TestMultiStreamRoundTripMulti(t *testing.T) {
	cleanupFiles()

	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	for i := 0; i < 100; i++ {
		writer.Write(&i)
	}
	testutils.CheckUint64(100, stream.Size(), t)

	stream2 := NewIntStream("test2", "id", nil)
	defer stream2.Close()
	var reader *IntStreamReader = stream2.Reader(0) // from beginning
	defer reader.Close()

	for i := 0; i < 100; i++ {
		testutils.CheckInt(i, reader.Read(), t)
	}
}

func TestMultiStreamPageIncrement(t *testing.T) {
	cleanupFiles()

	stream := NewIntStream("test", "id", nil)
	defer stream.Close()

	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()

	// 8 * 512 = 4096
	for i := 0; i < 513; i++ {
		writer.Write(&i)
	}
	testutils.CheckUint64(513, stream.Size(), t)

	stream2 := NewIntStream("test2", "id", nil)
	defer stream2.Close()
	var reader *IntStreamReader = stream2.Reader(0) // from beginning
	defer reader.Close()

	for i := 0; i < 513; i++ {
		testutils.CheckInt(i, reader.Read(), t)
	}
}

func TestMultiStreamSingleWriterSingleReader(t *testing.T) {
	cleanupFiles()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		inStream := NewIntStream("test", "id", nil)
		defer inStream.Close()
		var writer *IntStreamWriter = inStream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, inStream.Size(), t)
		wg.Done()
	}()

	go func() {
		outStream := NewIntStream("test", "id", nil)
		defer outStream.Close()
		var reader *IntStreamReader = outStream.Reader(0) // from beginning
		defer reader.Close()

		for i := 0; i < 513; i++ {
			testutils.CheckInt(i, reader.Read(), t)
		}
		wg.Done()
	}()

	wg.Wait()
}

func TestMultiStreamMultiSingleWriterMultipleReaders(t *testing.T) {
	cleanupFiles()

	var wg sync.WaitGroup
	wg.Add(1 + 10)

	go func() {
		inStream := NewIntStream("test", "id", nil)
		defer inStream.Close()
		var writer *IntStreamWriter = inStream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, inStream.Size(), t)
		wg.Done()
	}()

	for r := 0; r < 10; r++ {
		go func() {
			outStream := NewIntStream("test", "id", nil)
			defer outStream.Close()
			var reader *IntStreamReader = outStream.Reader(0) // from beginning
			defer reader.Close()

			for i := 0; i < 513; i++ {
				testutils.CheckInt(i, reader.Read(), t)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestMultiStreamMultiSingleWriterMultipleHungryReaders(t *testing.T) {
	cleanupFiles()

	var wg sync.WaitGroup
	wg.Add(1 + 10)

	for r := 0; r < 10; r++ {
		go func() {
			outStream := NewIntStream("test", "id", nil)
			defer outStream.Close()
			var reader *IntStreamReader = outStream.Reader(0) // from beginning
			defer reader.Close()

			for i := 0; i < 513; i++ {
				testutils.CheckInt(i, reader.Read(), t)
			}
			wg.Done()
		}()
	}

	go func() {
		inStream := NewIntStream("test", "id", nil)
		defer inStream.Close()
		var writer *IntStreamWriter = inStream.Writer()
		defer writer.Close()

		// 4096 / 8 = 512
		for i := 0; i < 513; i++ {
			writer.Write(&i)
		}
		testutils.CheckUint64(513, inStream.Size(), t)
		wg.Done()
	}()

	wg.Wait()
}

func TestMultiStreamMultipleWritersSingleReader(t *testing.T) {
	cleanupFiles()
	var wg sync.WaitGroup
	wg.Add(1 + 10)

	go func() {
		stream := NewIntStream("Out", "id", nil)
		defer stream.Close()
		var reader *IntStreamReader = stream.Reader(0) // from beginning
		defer reader.Close()
		var target = 513 * 10 * 3

		for i := 0; i < 513*10; i++ {
			target -= reader.Read()
		}
		testutils.CheckUint64(513*10, stream.Size(), t)
		testutils.CheckInt(0, target, t)
		wg.Done()
	}()

	for w := 0; w < 10; w++ {
		go func() {
			stream := NewIntStream("In", "id", nil)
			defer stream.Close()
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
}

func TestMultiStreamMultipleWritersMultipleReaders(t *testing.T) {
	cleanupFiles()
	var wg sync.WaitGroup
	wg.Add(10 + 10)

	for r := 0; r < 10; r++ {
		go func() {
			stream := NewIntStream("Out", "id", nil)
			defer stream.Close()
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
			stream := NewIntStream("In", "id", nil)
			defer stream.Close()
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
}

func cleanupFiles() {
	os.Remove(filepath.Join(os.TempDir(), "id"))
	os.Remove(filepath.Join(os.TempDir(), "id_header"))
}
