package runnel

import "testing"
import "github.com/asp2insp/go-misc/testutils"

//go:generate genny -in=runnel.go -out=IntStream.go gen "Typed=int"

func TestInsertIncrementsSize(t *testing.T) {
	stream := NewIntStream("test")
	data1 := 45
	data2 := 66
	stream.insert(&data1)
	stream.insert(&data2)

	testutils.CheckUint64(2, stream.Size, t)
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
