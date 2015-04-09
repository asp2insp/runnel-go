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
	storage     Storage
	IsAlive     bool
	tail        uint64 //TODO: move to header
	lastMessage uint64 //TODO: move to header
}

type TypedRef struct {
	stream *TypedStream
	offset uint64
}

func NewTypedStream(name, id string, storage Storage) *TypedStream {
	if id == "" {
		id = uuid.New()
	}
	if storage == nil {
		storage = NewFileStorage(id, "")
	}
	ret := &TypedStream{
		Name:    name,
		Id:      id,
		storage: storage,
	}
	return ret
}

// ==================== WRITER ===================

type TypedStreamWriter struct {
	// Input channel allowing multiple goroutines to
	// safely write to this writer simultaneously
	inChannel <-chan *Typed
	// The stream that this writer will write to
	parent *TypedStream
	// Whether this writer is alive
	isAlive bool
}

// Create a writer for the given stream
// The writer will continually listen for calls to
// Insert and write them to the underlying stream
// as long as the writer and the stream that it operates
// on are both alive
func (stream *TypedStream) Writer() *TypedStreamWriter {
	ret := &TypedStreamWriter{
		parent:    stream,
		inChannel: make(<-chan *Typed, 10),
	}
	go ret.writeLoop()
	ret.isAlive = true
	return ret
}

// As long as the writer is alive, pick up
// data from the input channel and write it
// to the stream
func (writer *TypedStreamWriter) writeLoop() {
	for writer.isAlive {
		datum := <-writer.inChannel
		writer.write(datum)
	}
}

// Write the given data into the stream
// TODO: Prefix with header
// The data is written in 3 steps:
// 1. Allocate space by bumping tail
// 2. Write data into allocated space
// 3. Declare data is available by bumping lastMessage
func (writer *TypedStreamWriter) write(data *Typed) {
	if !writer.parent.IsAlive || !writer.isAlive {
		// If the stream/writer isn't alive, there's no point
		return
	}
	storage := writer.parent.storage
	// Check to see if we need to resize
	if storage.Utilization() > 75 {
		// TODO: Work out how to handle multiple writers here, maybe through buffer swap
		storage.Resize(uint64(2 * storage.Capacity()))
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

// Close the writer
func (writer *TypedStreamWriter) Close() {
	writer.isAlive = false
}

// =================== OUTPUT ===================

type TypedStreamReader struct {
	// Out channel to allow blocking reads
	outChannel chan Typed
	// The stream that this reader will read from
	parent *TypedStream
	// The position in the stream that this reader
	// will begin from
	base uint64
	// The progress this reader has made since
	// it started reading
	offset uint64
	// Whether this reader is alive
	isAlive bool
}

// Build a new stream reader which maintains its place in the stream
// and provides functionality for leaving the stream
// TODO: Allow filtered readers, or maybe do an intermediate stream?
func (stream *TypedStream) Reader(parent *TypedStream, base uint64) *TypedStreamReader {
	ret := &TypedStreamReader{
		parent:     stream,
		outChannel: make(chan Typed),
		base:       base,
		offset:     0,
	}
	go ret.readLoop()
	ret.isAlive = true
	return ret
}

// Loop endlessly to read the data from the stream
func (reader *TypedStreamReader) readLoop() {
	for reader.isAlive {
		if reader.base+reader.offset <= reader.parent.lastMessage {
			// Advance the reader through the stream
			address := &reader.parent.storage.GetBytes(0, -1)[reader.offset]
			pointer := (*Typed)(unsafe.Pointer(address))
			reader.outChannel <- *pointer
			if reader.base+reader.offset < reader.parent.lastMessage {
				reader.offset += uint64(unsafe.Sizeof(pointer))
			}
		}
	}
}

// Read a single value from the stream (in a blocking fashion)
func (reader *TypedStreamReader) read() Typed {
	return <-reader.outChannel
}

// =================== FILTERS ==================

// =================== STREAMS ==================

/**
 * Close out the stream
 */
func (s *TypedStream) Close() {
	s.IsAlive = false
	s.storage.Close()
}

// ==================== UTILS ===================
