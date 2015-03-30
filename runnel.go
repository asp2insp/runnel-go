package runnel

import (
	"os"
  "unsafe"
	"path/filepath"

	"github.com/cheekybits/genny/generic"
)
import "github.com/edsrzf/mmap-go"
import "code.google.com/p/go-uuid/uuid"

type Typed generic.Type

type TypedStream struct {
	in     *inputManagerTyped
	out    *outputManagerTyped
	fileId string
	Name   string
}

type TypedRef struct {
	fileId string
	offset int64
}

func NewTypedStream(name string) *TypedStream {
	ret := new(TypedStream)
	ret.Name = name
	ret.fileId = uuid.New()
	ret.in = newInputManagerTyped(ret)
	ret.out = newOutputManagerTyped(ret)
	return ret
}

// ==================== INPUT ===================

type inputManagerTyped struct {
	inChannels []<-chan TypedRef
	streamData mmap.MMap
	parent     *TypedStream
}

func newInputManagerTyped(parent *TypedStream) *inputManagerTyped {
	ret := new(inputManagerTyped)
	ret.parent = parent
	ret.inChannels = make([]<-chan TypedRef, 0, 1)
	file, err := os.OpenFile(parent.fname(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	check(err)
	mapData, err := mmap.Map(file, mmap.RDWR, 0)
	check(err)
	ret.streamData = mapData
	return ret
}

// =================== OUTPUT ===================

type outputManagerTyped struct {
	outChannels []chan<- TypedRef
	streamData  mmap.MMap
	parent      *TypedStream
}

func newOutputManagerTyped(parent *TypedStream) *outputManagerTyped {
	ret := new(outputManagerTyped)
	ret.parent = parent
	ret.outChannels = make([]chan<- TypedRef, 0, 1)
	file, err := os.Open(parent.fname())
	check(err)
	mapData, err := mmap.Map(file, mmap.RDONLY, 0)
	check(err)
	ret.streamData = mapData
	return ret
}

func (outputManager *outputManagerTyped) resolve(ref *TypedRef) *Typed {
	if ref.fileId == outputManager.parent.fileId {
		return (*Typed)unsafe.Pointer(&outputManager.streamData[ref.offset])
	} else {
		return nil
	}
}

// =================== FILTERS ==================

// =================== STREAMS ==================

// ==================== UTILS ===================

func (p *TypedStream) fname() string {
	return filepath.Join(os.TempDir(), p.fileId)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
