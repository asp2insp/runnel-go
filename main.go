package main

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/asp2insp/runnel-go/runnel"
	"github.com/pkg/profile"
)

func main() {
	defer profile.Start().Stop()
	os.Remove(filepath.Join(os.TempDir(), "id"))
	os.Remove(filepath.Join(os.TempDir(), "id_header"))
	var wg sync.WaitGroup
	workers := 10
	wg.Add(workers * 2)
	size := 10 * 1000

	for r := 0; r < workers; r++ {
		go func() {
			stream := runnel.NewIntStream("Out", "id", nil)
			defer stream.Close()
			var reader *runnel.IntStreamReader = stream.Reader(0) // from beginning
			defer reader.Close()
			var target = size * workers * 3

			for i := 0; i < size*workers; i++ {
				target -= reader.Read()
			}
			wg.Done()
		}()
	}

	for w := 0; w < workers; w++ {
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
