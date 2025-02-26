package ddrv

import (
	"io"
	"log"
	"fmt"
)

// Reader is a structure that manages the reading of a sequence of Chunks.
// It reads chunks in order, closing each one after it's read and moving on to the next.
type Reader struct {
	chunks []Attachment  // The sequence of chunks to be read.
	curIdx int           // Index of the chunk that is currently being read.
	closed bool          // Indicates whether the Reader has been closed.
	disc   *Manager      // Manager object provides access to the chunks.
	reader io.ReadCloser // The reader that is reading the current chunk.
	pos    int64         // Global file offset
}

// NewReader creates new Reader instance which implements io.ReadCloser.
func NewReader(chunks []Attachment, pos int64, arc *Manager) (io.ReadCloser, error) {
	r := &Reader{chunks: chunks, pos: pos, disc: arc}
	// Calculate Start and End for each chunk
	var offset int64
	for i := range r.chunks {
		r.chunks[i].Start = offset                             // Start offset for chunk i
		r.chunks[i].End = offset + int64(r.chunks[i].Size) - 1 // End offset (inclusive)
		offset = r.chunks[i].End + 1
	}
	// If pos > total size, return EOF
	if r.pos > offset {
		return nil, io.EOF
	}
	// Find starting chunk and drop all chunks completely before 'pos'
	var start int
	for i, chunk := range r.chunks {
		start += chunk.Size
		if start > int(r.pos) {
			// Drop extra chunks to save memory
			r.chunks = r.chunks[i:]
			break
		}
	}
	log.Printf("NewReader: Starting at global offset %d in chunk 0", r.pos)
	return r, nil
}

// Read reads data from the current chunk into p.
// If it reaches the end of a chunk, it moves to the next one.
func (r *Reader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrClosed
	}
	// Handle empty file case
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	if r.reader == nil {
		if err := r.next(); err != nil {
			return 0, err
		}
	}
	var totalRead int
	for {
		nr, err := r.reader.Read(p[totalRead:])
		totalRead += nr
		// **Wichtig:** Update des globalen Offsets
		r.pos += int64(nr)
		
		if err == io.EOF {
			log.Printf("Read: Finished chunk %d, totalRead so far: %d bytes", r.curIdx, totalRead)
			r.curIdx++
			if r.curIdx >= len(r.chunks) {
				return totalRead, io.EOF
			}
			if err = r.next(); err != nil {
				return totalRead, err
			}
			// Versuche, weiter zu lesen
			continue
		}

		if err != nil && err != io.EOF {
			return totalRead, err
		}

		if totalRead >= len(p) {
			return totalRead, nil
		}
	}
}

// Close closes the Reader.
func (r *Reader) Close() error {
	if r.closed {
		return ErrAlreadyClosed
	}
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			log.Printf("Reader.Close: Error closing current chunk reader: %v", err)
			return err
		}
		log.Printf("Reader.Close: Closed current chunk reader.")
	}
	r.closed = true
	log.Printf("Reader.Close: Reader closed successfully.")
	return nil
}

// next moves to the next chunk in the chunks slice, creating a new reader for it.
func (r *Reader) next() error {
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return err
		}
	}
	chunk := r.chunks[r.curIdx]
	// Berechne den relativen Startoffset in diesem Chunk:
	var start int
	if r.pos > chunk.Start {
		start = int(r.pos - chunk.Start)
	}
	log.Printf("next: Starting new reader for chunk %d at relative offset %d (global pos: %d, chunk start: %d)", r.curIdx, start, r.pos, chunk.Start)
	reader, err := r.disc.read(chunk.URL, start, chunk.Size-1)
	if err != nil {
		return fmt.Errorf("next: failed to create reader for chunk %d: %w", r.curIdx, err)
	}
	r.reader = reader
	return nil
}
