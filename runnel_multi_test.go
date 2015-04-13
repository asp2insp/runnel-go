package runnel

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

func TestMultiStreamWriteIncrementsSize(t *testing.T) {
	cleanupFiles(t)

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
	cleanupFiles(t)

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
	cleanupFiles(t)

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
	testutils.CheckUint64(8, stream2.header().LastMessage, t)
}

/*
func TestMultiStreamSingleWriterSingleReader(t *testing.T) {
	cleanupFiles(t)

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


func TestMultiSingleWriterMultipleReaders(t *testing.T) {
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

func TestMultiSingleWriterMultipleHungryReaders(t *testing.T) {
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

func TestMultiMultipleWritersSingleReader(t *testing.T) {
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

func TestMultiMultipleWritersMultipleReaders(t *testing.T) {
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

func cleanupFiles(t *testing.T) {
	os.Remove(filepath.Join(os.TempDir(), "id"))
	os.Remove(filepath.Join(os.TempDir(), "id_header"))
}
