package runnel

import (
	"sync"
	"testing"
)

func BenchmarkMultiMulti(b *testing.B) {
	cleanupFiles()
	var wg sync.WaitGroup
	wg.Add(10 + 10)
	size := b.N

	for r := 0; r < 10; r++ {
		go func() {
			stream := NewIntStream("Out", "id", nil)
			defer stream.Close()
			var reader *IntStreamReader = stream.Reader(0) // from beginning
			defer reader.Close()
			var target = size * 10 * 3

			for i := 0; i < size*10; i++ {
				target -= reader.Read()
			}
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
			for i := 0; i < size; i++ {
				writer.Write(&amount)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkSingleWriterSingleReader(b *testing.B) {
	cleanupFiles()
	stream := NewIntStream("In", "id", nil)
	defer stream.Close()
	var writer *IntStreamWriter = stream.Writer()
	defer writer.Close()
	for i := 0; i < b.N; i++ {
		writer.Write(&i)
	}
}
