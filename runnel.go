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
	inChannels   []<-chan TypedRef
	streamData   mmap.MMap
	streamHeader *StreamHeader
	parent       *TypedStream
	offset       uint64
	file         *os.File
}

func newInputManagerTyped(parent *TypedStream) *inputManagerTyped {
	ret := new(inputManagerTyped)
	ret.parent = parent
	ret.offset = 0
	ret.inChannels = make([]<-chan TypedRef, 0, 1)

	// Map in the data
	ret.streamData, ret.file = ret.mmapFile(parent.fname())

	// Map in the header
	headerMap, _ := ret.mmapFile(parent.fheader())
	ret.streamHeader = toHeader(headerMap)
	ret.streamHeader.FileSize = utils.Filesize(ret.file)
	return ret
}

func (inputManager *inputManagerTyped) mmapFile(path string) (mmap.MMap, *os.File) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	utils.Check(err)
	if utils.Filesize(file) == 0 {
		err = file.Truncate(int64(os.Getpagesize()))
		utils.Check(err)
	}
	mapData, err := mmap.Map(file, mmap.RDWR, 0)
	utils.Check(err)
	return mapData, file
}

func (iM *inputManagerTyped) insert(data *Typed) {
	header := iM.streamHeader
	// Check to see if we need to resize
	if header.EntrySize*(header.EntryCount+1) >= header.FileSize {
		iM.setFileSizeTo(header.FileSize * 2)
	}

	address := &iM.streamData[iM.offset]
	pointer := (*Typed)(unsafe.Pointer(address))
	*pointer = *data
	iM.offset += uint64(unsafe.Sizeof(*data))
	iM.parent.Size += 1

	// Update the header
	if iM.streamHeader.EntrySize == 0 {
		iM.streamHeader.EntrySize = uint64(unsafe.Sizeof(*data))
	}
	iM.streamHeader.EntryCount += 1
}

func (inputManager *inputManagerTyped) setFileSizeTo(size uint64) {
	err := inputManager.file.Truncate(int64(size))
	utils.Check(err)

	// Re-map our data
	inputManager.streamData.Unmap()
	inputManager.streamData, _ = inputManager.mmapFile(inputManager.file.Name())

	// Update the header
	inputManager.streamHeader.FileSize = size
}

// =================== OUTPUT ===================

type outputManagerTyped struct {
	outChannels   []chan<- TypedRef
	streamData    mmap.MMap
	streamHeader  *StreamHeader
	parent        *TypedStream
	file          *os.File
	lastKnownSize uint64
}

func newOutputManagerTyped(parent *TypedStream) *outputManagerTyped {
	ret := new(outputManagerTyped)
	ret.parent = parent
	ret.outChannels = make([]chan<- TypedRef, 0, 1)

	// Map in the data
	ret.streamData, ret.file = ret.mmapFile(parent.fname())

	// Map in the header
	headerMap, _ := ret.mmapFile(parent.fheader())
	ret.streamHeader = toHeader(headerMap)
	ret.lastKnownSize = ret.streamHeader.FileSize
	return ret
}

func (outputManager *outputManagerTyped) mmapFile(path string) (mmap.MMap, *os.File) {
	file, err := os.Open(path)
	utils.Check(err)
	mapData, err := mmap.Map(file, mmap.RDONLY, 0)
	utils.Check(err)
	return mapData, file
}

func (oM *outputManagerTyped) resolve(ref *TypedRef) *Typed {
	if ref.fileId == oM.parent.fileId {
		if oM.lastKnownSize != oM.streamHeader.FileSize {
			oM.refreshMap()
		}
		address := &oM.streamData[ref.offset]
		return (*Typed)(unsafe.Pointer(address))
	} else {
		return nil
	}
}

func (oM *outputManagerTyped) refreshMap() {
	oM.streamData.Unmap()
	oM.streamData, _ = oM.mmapFile(oM.file.Name())
}

// =================== FILTERS ==================

// =================== STREAMS ==================

// ==================== UTILS ===================

func (p *TypedStream) fname() string {
	return filepath.Join(os.TempDir(), p.fileId)
}

func (p *TypedStream) fheader() string {
	return filepath.Join(os.TempDir(), p.fileId+"_header")
}
