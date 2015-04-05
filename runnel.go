package runnel

import (
	"unsafe"

	"code.google.com/p/go-uuid/uuid"
	"github.com/cheekybits/genny/generic"
)

type Typed generic.Type

type TypedStream struct {
	Name        string
	Id          string
	storage     *Storage
	IsAlive     bool
	tail        uint64 //TODO: move to header
	lastMessage uint64 //TODO: move to header
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

// ==================== WRITER ===================

type StreamWriterTyped struct {
	inChannel <-chan *Typed
	parent    *TypedStream
	isAlive   bool
}

// Create a writer for the given stream
// The writer will continually listen for calls to
// Insert and write them to the underlying stream
// as long as the writer and the stream that it operates
// on are both alive
func (stream *TypedStream) Writer() *StreamWriterTyped {
	ret := &StreamWriterTyped{
		parent:    parent,
		offset:    0,
		inChannel: make(<-chan *Typed, 10),
	}
	go ret.writeLoop()
	return ret
}

// As long as the writer is alive, pick up
// data from the input channel and write it
// to the stream
func (writer *StreamWriterTyped) writeLoop() {
	for writer.isAlive {
		datum := <-writer.inChannel
		writer.write(data)
	}
}

// Write the given data into the stream
// TODO: Prefix with header
// The data is written in 3 steps:
// 1. Allocate space by bumping tail
// 2. Write data into allocated space
// 3. Declare data is available by bumping lastMessage
func (writer *StreamWriterTyped) write(data *Typed) {
	if !writer.parent.IsAlive() {
		// If the stream isn't alive, there's no point
		return
	}
	storage := writer.parent.storage
	// Check to see if we need to resize
	if storage.Utilization() > 75 {
		// TODO: Work out how to handle multiple writers here
		storage.Resize(2 * storage.Capacity())
	}

	// TODO make this atomic
	// Get old tail
	offset := writer.parent.tail
	// Bump tail
	writer.parent.tail += uint64(unsafe.Sizeof(data))

	// Write data
	address := &writer.parent.storage.GetBytes(0, -1)[offset]
	pointer := (*Typed)(unsafe.Pointer(address))
	*pointer = *data

	// Declare data available
	writer.parent.lastMessage = offset
	writer.parent.storage.Header().EntryCount += 1
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

/**
 * Close out the stream
 */
func (s *TypedStream) Close() {
	s.IsAlive = false
	s.storage.Close()
}

// ==================== UTILS ===================
