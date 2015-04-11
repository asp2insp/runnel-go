package runnel

import (
	"sync"
	"testing"

	"github.com/asp2insp/go-misc/testutils"
)

func TestMultiSingleWriterSingleReader(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		inStream := NewIntStream("test", "/tmp/", nil)
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
		outStream := NewIntStream("test", "/tmp/", nil)
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

/*
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
