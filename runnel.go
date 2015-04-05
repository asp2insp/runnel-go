package runnel

import (
	"unsafe"

	"code.google.com/p/go-uuid/uuid"
	"github.com/cheekybits/genny/generic"
	"github.com/edsrzf/mmap-go"
)

type Typed generic.Type

type TypedStream struct {
	Name    string
	Id      string
	storage *Storage
	IsAlive bool
}

type TypedRef struct {
	stream *TypedStream
	offset uint64
}

func NewTypedStream(name, id string, storage *Storage) *TypedStream {
	if id == "" {
		id = uuid.New()
	}
	if storage == nil {
		storage = NewFileStorage(id, "")
	}
	ret := &TypedStream{
		Name:    name,
		fileId:  id,
		storage: storage,
	}
	return ret
}

// ==================== INPUT ===================

type streamWriterTyped struct {
	inChannel  <-chan TypedRef
	streamData mmap.MMap
	parent     *TypedStream
	offset     uint64
}

func newstreamWriterTyped(parent *TypedStream) *streamWriterTyped {
	ret := new(streamWriterTyped)
	ret.parent = parent
	ret.offset = 0
	ret.inChannels = make([]<-chan TypedRef, 0, 1)
	return ret
}

func (iM *streamWriterTyped) insert(data *Typed) *TypedRef {
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

type streamReaderTyped struct {
	parent        *TypedStream
	lastKnownSize uint64
}

/**
 * Build a new output manager which manages a set of output channels
 * for the stream and is responsible for reading from the mmapped file.
 */
func newstreamReaderTyped(parent *TypedStream) *streamReaderTyped {
	ret := new(streamReaderTyped)
	ret.parent = parent
	ret.outChannels = make([]chan *TypedRef, 0, 1)

	// Map in the data

	// Map in the header
	ret.lastKnownSize = ret.parent.header.FileSize
	return ret
}

/**
 * Resolve a reference into the storage and return the address of the value
 * that corresponds to the reference
 */
func (oM *streamReaderTyped) resolve(ref *TypedRef) *Typed {
	if !oM.parent.IsAlive() && ref.stream == oM.parent {
		// TODO: optimize this lookup
		address := &oM.parent.storage.GetBytes(0, -1)[ref.offset]
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
}

/**
 * Close out the stream
 */
func (s *TypedStream) Close() {
	s.IsAlive = false
	s.storage.Close()
}

// ==================== UTILS ===================
