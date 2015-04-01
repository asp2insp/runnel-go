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
	in         *inputManagerTyped
	out        *outputManagerTyped
	fileId     string
	Name       string
	Size       uint64
	header     *StreamHeader
	headerMem  mmap.MMap
	headerFile *os.File
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

	ret.headerMem, ret.headerFile = mmapFile(ret.fheader(), os.O_RDWR|os.O_CREATE, mmap.RDWR)
	ret.header = toHeader(ret.headerMem)

	ret.in = newInputManagerTyped(ret)
	ret.out = newOutputManagerTyped(ret)
	return ret
}

// ==================== INPUT ===================

type inputManagerTyped struct {
	inChannels []<-chan TypedRef
	streamData mmap.MMap
	parent     *TypedStream
	offset     uint64
	file       *os.File
}

func newInputManagerTyped(parent *TypedStream) *inputManagerTyped {
	ret := new(inputManagerTyped)
	ret.parent = parent
	ret.offset = 0
	ret.inChannels = make([]<-chan TypedRef, 0, 1)

	// Map in the data
	ret.streamData, ret.file = mmapFile(parent.fname(), os.O_APPEND|os.O_RDWR|os.O_CREATE, mmap.RDWR)

	// Update the header
	ret.parent.header.FileSize = utils.Filesize(ret.file)
	return ret
}

func (iM *inputManagerTyped) insert(data *Typed) *TypedRef {
	header := iM.parent.header
	// Check to see if we need to resize
	if header.EntrySize*(header.EntryCount+1) >= header.FileSize {
		iM.setFileSizeTo(header.FileSize * 2)
	}

	address := &iM.streamData[iM.offset]
	pointer := (*Typed)(unsafe.Pointer(address))
	*pointer = *data
	retOffset := iM.offset
	iM.offset += uint64(unsafe.Sizeof(*data))
	iM.parent.Size += 1

	// Update the header
	if header.EntrySize == 0 {
		header.EntrySize = uint64(unsafe.Sizeof(*data))
	}
	header.EntryCount += 1

	return &TypedRef{iM.parent.fileId, retOffset}
}

func (inputManager *inputManagerTyped) setFileSizeTo(size uint64) {
	err := inputManager.file.Truncate(int64(size))
	utils.Check(err)

	// Re-map our data
	inputManager.streamData.Unmap()
	inputManager.streamData, _ = mmapFile(inputManager.file.Name(), os.O_APPEND|os.O_RDWR|os.O_CREATE, mmap.RDWR)

	// Update the header
	inputManager.parent.header.FileSize = size
}

// =================== OUTPUT ===================

type outputManagerTyped struct {
	outChannels   []chan *TypedRef
	streamData    mmap.MMap
	parent        *TypedStream
	file          *os.File
	lastKnownSize uint64
}

func newOutputManagerTyped(parent *TypedStream) *outputManagerTyped {
	ret := new(outputManagerTyped)
	ret.parent = parent
	ret.outChannels = make([]chan *TypedRef, 0, 1)

	// Map in the data
	ret.streamData, ret.file = mmapFile(parent.fname(), os.O_RDONLY, mmap.RDONLY)

	// Map in the header
	ret.lastKnownSize = ret.parent.header.FileSize
	return ret
}

func (oM *outputManagerTyped) notify(ref *TypedRef) {
	for i := range oM.outChannels {
		select {
		case oM.outChannels[i] <- ref:
		default:
		}
	}
}

func (oM *outputManagerTyped) resolve(ref *TypedRef) *Typed {
	if ref.fileId == oM.parent.fileId {
		if oM.lastKnownSize != oM.parent.header.FileSize {
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
	oM.streamData, _ = mmapFile(oM.file.Name(), os.O_RDONLY, mmap.RDONLY)
}

// =================== FILTERS ==================

// =================== STREAMS ==================

func (s *TypedStream) Outlet(c chan *Typed) {
	middle := make(chan *TypedRef)
	s.out.outChannels = append(s.out.outChannels, middle)
	go func() {
		var count uint64 = 0
		for s.IsAlive() {
			if s.header.EntryCount > count {
				datum := s.FindOne(count)
				// TODO: Update to public API
				c <- s.out.resolve(datum)
				count++
			} else {
				<-middle
			}
		}
	}()
}

func (s *TypedStream) FindOne(i uint64) *TypedRef {
	return &TypedRef{s.fileId, s.header.EntrySize * i}
}

func (s *TypedStream) Insert(data *Typed) {
	// TODO: Connect to the inChannels rather than calling insert
	// directly
	ref := s.in.insert(data)
	s.out.notify(ref)
}

func (s *TypedStream) IsAlive() bool {
	return s.header != nil
}

func (s *TypedStream) Close() {
	// TODO: make in/out implement closable
	s.in.streamData.Unmap()
	s.in.file.Close()
	s.out.streamData.Unmap()
	s.out.file.Close()
	s.headerMem.Unmap()
	s.headerFile.Close()
	s.header = nil
}

// ==================== UTILS ===================

func (s *TypedStream) fname() string {
	return filepath.Join(os.TempDir(), s.fileId)
}

func (s *TypedStream) fheader() string {
	return filepath.Join(os.TempDir(), s.fileId+"_header")
}
