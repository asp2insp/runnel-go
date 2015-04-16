package runnel

import (
	"fmt"
	"unsafe"

	"code.google.com/p/go-uuid/uuid"
	"github.com/asp2insp/runnel-go/i"
	"github.com/asp2insp/runnel-go/s"
	"github.com/cheekybits/genny/generic"
)

type Typed generic.Type

type TypedStream struct {
	Name              string
	Id                string
	storage           i.Storage
	IsAlive           bool
	lastKnownFileSize uint64
	typeSize          uint64
}

type TypedRef struct {
	stream *TypedStream
	offset uint64
}

func NewTypedStream(name, id string, store i.Storage) *TypedStream {
	if id == "" {
		id = uuid.New()
	}
	if store == nil {
		store = s.NewFileStorage("").Init(id)
	}
	ret := &TypedStream{
		Name:              name,
		Id:                id,
		storage:           store,
		IsAlive:           true,
		lastKnownFileSize: store.Capacity(),
		typeSize:          uint64(unsafe.Sizeof(new(Typed))),
	}
	return ret
}

func (stream *TypedStream) header() *i.StreamHeader {
	return stream.storage.Header()
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
	for writer.isAlive && writer.parent.IsAlive {
		datum := <-writer.inChannel
		writer.Write(datum)
	}
}

// Write the given data into the stream
// TODO: Prefix with header
// The data is written in 3 steps:
// 1. Allocate space by bumping tail
// 2. Write data into allocated space
// 3. Declare data is available by bumping lastMessage
func (writer *TypedStreamWriter) Write(data *Typed) {
	if !writer.parent.IsAlive || !writer.isAlive {
		// If the stream/writer isn't alive, there's no point
		return
	}
	storage := writer.parent.storage
	// Check to see if we need to resize
	if storage.Utilization() > 75 {
		// TODO: Work out how to handle multiple writers here, maybe through buffer swap
		storage.Resize(uint64(2 * storage.Capacity()))
		writer.parent.lastKnownFileSize = writer.parent.header().FileSize
	}

	// TODO make this atomic
	// Get old tail
	offset := writer.parent.header().Tail
	// Bump tail
	writer.parent.header().Tail += writer.parent.typeSize

	// Check before we write
	if writer.parent.header().Tail > storage.Capacity() {
		panic(fmt.Sprintf("No Room! Header: %+v", storage.Header()))
	}

	// Write data
	slice := writer.parent.storage.GetBytes(offset, offset+writer.parent.typeSize)
	address := &slice[0]
	var pointer *Typed = (*Typed)(unsafe.Pointer(address))
	var datum Typed = *data
	*pointer = datum
	writer.parent.storage.ReturnBytes(slice)

	// Declare data available
	writer.parent.header().LastMessage = writer.max(writer.parent.header().LastMessage, offset+writer.parent.typeSize)
	writer.parent.header().EntryCount += 1
	storage.Flush()
}

// Close the writer
func (writer *TypedStreamWriter) Close() {
	writer.isAlive = false
}

func (writer *TypedStreamWriter) max(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
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
func (stream *TypedStream) Reader(base uint64) *TypedStreamReader {
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
	for reader.isAlive && reader.parent.IsAlive {
		if reader.parent.lastKnownFileSize != reader.parent.header().FileSize {
			reader.parent.storage.Refresh()
		}
		if reader.base+reader.offset < reader.parent.header().LastMessage {
			// Advance the reader through the stream
			bot := reader.base + reader.offset
			slice := reader.parent.storage.GetBytes(bot, bot+reader.parent.typeSize)
			address := &slice[0]
			pointer := (*Typed)(unsafe.Pointer(address))
			reader.outChannel <- *pointer
			reader.parent.storage.ReturnBytes(slice)
			reader.offset += reader.parent.typeSize
		}
	}
}

// Read a single value from the stream (in a blocking fashion)
func (reader *TypedStreamReader) Read() Typed {
	return <-reader.outChannel
}

func (reader *TypedStreamReader) Close() {
	reader.isAlive = false
}

// =================== FILTERS ==================

// =================== STREAMS ==================

func (s *TypedStream) Size() uint64 {
	return s.header().EntryCount
}

// Close out the stream
func (s *TypedStream) Close() {
	s.IsAlive = false
	s.storage.Close()
}

// ==================== UTILS ===================
