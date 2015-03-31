package runnel

import (
	"os"
	"path/filepath"
	"unsafe"

	"code.google.com/p/go-uuid/uuid"
	"github.com/asp2insp/go-misc/utils"
	"github.com/cheekybits/genny/generic"
	"github.com/edsrzf/mmap-go"
)

type Typed generic.Type

type TypedStream struct {
	in     *inputManagerTyped
	out    *outputManagerTyped
	fileId string
	Name   string
	Size   uint64
}

type TypedRef struct {
	fileId string
	offset uint64
}

func NewTypedStream(name string) *TypedStream {
	ret := &TypedStream{
		Name:   name,
		fileId: uuid.New(),
	}
	ret.in = newInputManagerTyped(ret)
	ret.out = newOutputManagerTyped(ret)
	return ret
}

func (stream *TypedStream) insert(data *Typed) {
	// TODO: Connect to the inChannels rather than calling insert
	// directly
	stream.in.insert(data)
}

// ==================== INPUT ===================

type inputManagerTyped struct {
	inChannels []<-chan TypedRef
	streamData mmap.MMap
	parent     *TypedStream
	offset     uint64
}

func newInputManagerTyped(parent *TypedStream) *inputManagerTyped {
	ret := new(inputManagerTyped)
	ret.parent = parent
	ret.offset = 0
	ret.inChannels = make([]<-chan TypedRef, 0, 1)
	file, err := os.OpenFile(parent.fname(), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	utils.Check(err)
	fileInfo, err := file.Stat()
	utils.Check(err)
	if fileInfo.Size() == 0 {
		err = file.Truncate(int64(os.Getpagesize()))
		utils.Check(err)
	}
	mapData, err := mmap.Map(file, mmap.RDWR, 0)
	utils.Check(err)
	ret.streamData = mapData
	return ret
}

func (inputManager *inputManagerTyped) insert(data *Typed) {
	address := &inputManager.streamData[inputManager.offset]
	pointer := (*Typed)(unsafe.Pointer(address))
	*pointer = *data
	inputManager.offset += uint64(unsafe.Sizeof(*data))
	inputManager.parent.Size += 1
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
	utils.Check(err)
	mapData, err := mmap.Map(file, mmap.RDONLY, 0)
	utils.Check(err)
	ret.streamData = mapData
	return ret
}

func (outputManager *outputManagerTyped) resolve(ref *TypedRef) *Typed {
	if ref.fileId == outputManager.parent.fileId {
		address := &outputManager.streamData[ref.offset]
		return (*Typed)(unsafe.Pointer(address))
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
