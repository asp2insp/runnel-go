package runnel

import (
	"os"
	"unsafe"

	"code.google.com/p/go-uuid/uuid"
	"github.com/asp2insp/go-misc/utils"
	"github.com/cheekybits/genny/generic"
	"github.com/edsrzf/mmap-go"
)

type Typed generic.Type

type TypedStream struct {
	in      *inputManagerTyped
	out     *outputManagerTyped
	Name    string
	Id      string
	storage *Storage
	header  *StreamHeader
}

type TypedRef struct {
	stream *TypedStream
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

	// Map in the data
	ret.streamData, ret.file = mmapFile(parent.fname(), os.O_APPEND|os.O_RDWR|os.O_CREATE, mmap.RDWR)

	// Update the header
	ret.parent.header.FileSize = utils.Filesize(ret.file)
	return ret
}

func (iM *inputManagerTyped) insert(data *Typed) *TypedRef {
	if !iM.parent.IsAlive() {
		return nil
	}
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

	return &TypedRef{iM.parent, retOffset}
}

// =================== OUTPUT ===================

type outputManagerTyped struct {
	outChannels   []chan *TypedRef
	streamData    mmap.MMap
	parent        *TypedStream
	file          *os.File
	lastKnownSize uint64
}

/**
 * Build a new output manager which manages a set of output channels
 * for the stream and is responsible for reading from the mmapped file.
 */
func newOutputManagerTyped(parent *TypedStream) *outputManagerTyped {
	ret := new(outputManagerTyped)
	ret.parent = parent
	ret.outChannels = make([]chan *TypedRef, 0, 1)

	// Map in the data

	// Map in the header
	ret.lastKnownSize = ret.parent.header.FileSize
	return ret
}

/**
 * Notify all listeners that there is new data available
 */
func (oM *outputManagerTyped) notify(ref *TypedRef) {
	for i := range oM.outChannels {
		select {
		case oM.outChannels[i] <- ref:
		default:
		}
	}
}

/**
 * Resolve a reference into the mmap'd file and return the value
 */
func (oM *outputManagerTyped) resolve(ref *TypedRef) *Typed {
	if !oM.parent.IsAlive() {
		return nil
	}
	if ref.stream == oM.parent {
		address := &oM.streamData[ref.offset]
		return (*Typed)(unsafe.Pointer(address))
	} else {
		return nil
	}
}

// =================== FILTERS ==================

// =================== STREAMS ==================

/**
 * Output all values in the stream onto the channel
 */
func (s *TypedStream) Outlet(c chan Typed) {
	middle := make(chan *TypedRef)
	s.out.outChannels = append(s.out.outChannels, middle)
	go func() {
		var count uint64 = 0
		for s.IsAlive() {
			if s.header.EntryCount > count {
				datum := s.FindOne(count)
				// TODO: Update to public API
				c <- *s.out.resolve(datum)
				count++
			} else {
				<-middle
			}
		}
	}()
}

func (s *TypedStream) FindOne(i uint64) *TypedRef {
	return &TypedRef{s, s.header.EntrySize * i}
}

func (s *TypedStream) Insert(data *Typed) {
	// TODO: Connect to the inChannels rather than calling insert
	// directly
	ref := s.in.insert(data)
	s.out.notify(ref)
}

func (s *TypedStream) IsAlive() bool {
	return s.header.FileSize != 0
}

/**
 * Close out the stream
 */
func (s *TypedStream) Close() {
	// TODO: make in/out implement closable
	s.header = &StreamHeader{}
	s.in.streamData.Unmap()
	s.in.file.Close()
	s.out.streamData.Unmap()
	s.out.file.Close()
	s.headerMem.Unmap()
	s.headerFile.Close()
}

// ==================== UTILS ===================
