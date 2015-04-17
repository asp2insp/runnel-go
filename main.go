package main

import (
	"sync"

	"github.com/asp2insp/runnel-go/runnel"

	"github.com/pkg/profile"
)

func main() {
	defer profile.Start().Stop()

	cleanupFiles()
	var wg sync.WaitGroup
	wg.Add(10 + 10)
	size := b.N

	for r := 0; r < 10; r++ {
		go func() {
			stream := runnel.NewIntStream("Out", "id", nil)
			defer stream.Close()
			var reader *runnel.IntStreamReader = stream.Reader(0) // from beginning
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
			stream := runnel.NewIntStream("In", "id", nil)
			defer stream.Close()
			var writer *runnel.IntStreamWriter = stream.Writer()
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
