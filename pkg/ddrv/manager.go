package ddrv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"log"
	"time"
)

// ErrClosed is returned when a writer or reader is
// closed and caller is trying to read or write
var ErrClosed = errors.New("is closed")

// ErrAlreadyClosed is returned when the reader/writer is already closed
var ErrAlreadyClosed = errors.New("already closed")

// ErrInvalidWebhookURL is the error returned for an invalid webhook URL.
var ErrInvalidWebhookURL = errors.New("invalid webhook URL")

// Attachment represents a Discord attachment URL and Size
type Attachment struct {
	URL   string `json:"url"`  // URL where the data is stored
	Size  int    `json:"size"` // Size of the data
	Start int64  // Start position of the data in the overall data sequence
	End   int64  // End position of the data in the overall data sequence
}

// Manager provides an interface to read and write large files to Discord by splitting the files into
// smaller chunks, uploading or downloading these chunks through Discord webhooks, and reassembling
// them into the original files.
type Manager struct {
	chunkSize int             // Size of each chunk of data to be processed
	webhooks  []string        // List of webhook URLs to be used for data storing
	clients   []*Rest         // List of webhook clients corresponding to the webhook URLs
	lastCIdx  int             // Index of the last used webhook client
	traceCtx  context.Context // Context for HTTP client tracing
	mu        sync.Mutex
}

// NewManager returns a new instance of Manager with specified chunk size and webhook URLs.
// It initializes a list of webhook rest clients for each webhook URL.
func NewManager(chunkSize int, webhooks []string) (*Manager, error) {
	st := &Manager{
		chunkSize: chunkSize,
		webhooks:  webhooks,
		clients:   make([]*Rest, 0),
	}
	for _, url := range webhooks {
		client, err := NewRest(url)
		if err != nil {
			return nil, err
		}
		st.clients = append(st.clients, client)
	}

	// Initialize tracing context for HTTP requests
	clientTrace := &httptrace.ClientTrace{}
	st.traceCtx = httptrace.WithClientTrace(context.Background(), clientTrace)

	return st, nil
}

// NewWriter creates a new ddrv.Writer instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
func (mgr *Manager) NewWriter(onChunk func(chunk *Attachment)) io.WriteCloser {
	return NewWriter(onChunk, mgr.chunkSize, mgr)
}

// NewNWriter creates a new ddrv.NWriter instance that implements an io.WriterCloser.
// This allows for writing large files to Discord as small, manageable chunks.
// NWriter buffers bytes into memory and writes data to discord in parallel
func (mgr *Manager) NewNWriter(onChunk func(chunk *Attachment)) io.WriteCloser {
	return NewNWriter(onChunk, mgr.chunkSize, mgr)
}

// NewReader creates a new Reader instance that implements an io.ReaderCloser.
// This allows for reading large files from Discord that were split into small chunks.
func (mgr *Manager) NewReader(chunks []Attachment, pos int64) (io.ReadCloser, error) {
	return NewReader(chunks, pos, mgr)
}

// read fetches a range of data from the specified URL.
// The range is specified by the start and end positions.
func (mgr *Manager) read(url string, start, end int) (io.ReadCloser, error) {
    refreshedURL := fmt.Sprintf("https://api.animemoe.us/discord/refresh?url=%s", url)
    log.Printf("read: Refreshing URL %s to %s", url, refreshedURL)

    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 1; attempt <= 3; attempt++ {
        req, err := http.NewRequestWithContext(mgr.traceCtx, http.MethodGet, refreshedURL, nil)
        if err != nil {
            log.Printf("read: Error creating request for %s: %v", refreshedURL, err)
            return nil, err
        }
        req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

        log.Printf("read: Attempt %d for %s", attempt, refreshedURL)
        res, err := client.Do(req)
        if err == nil && res.StatusCode == http.StatusPartialContent {
            log.Printf("read: Successfully fetched %s", refreshedURL)
            return res.Body, nil
        }
        if err != nil {
            log.Printf("read: Error fetching %s on attempt %d: %v", refreshedURL, attempt, err)
        } else {
            log.Printf("read: Unexpected status %d for %s on attempt %d", res.StatusCode, refreshedURL, attempt)
            res.Body.Close()
        }
        if attempt < 3 {
            time.Sleep(time.Duration(attempt) * time.Second)
        }
    }
    log.Printf("read: Failed to fetch %s after 3 attempts", refreshedURL)
    return nil, fmt.Errorf("read: failed to fetch %s after 3 attempts", refreshedURL)
}

// write created new attachment on Discord with provided Reader,
// returning the Attachment.
func (mgr *Manager) write(r io.Reader) (*Attachment, error) {
	// Select the next webhook client
	client := mgr.next()

	// Create a new Manager message with the data as an attachment
	return client.CreateAttachment(r)
}

// next returns the next webhook client in the list, cycling through the list in a round-robin manner.
func (mgr *Manager) next() *Rest {
	mgr.mu.Lock()
	// Select the next client
	client := mgr.clients[mgr.lastCIdx]
	// Update the index of the last used client, wrapping around to the start of the list if necessary
	mgr.lastCIdx = (mgr.lastCIdx + 1) % len(mgr.clients)
	mgr.mu.Unlock()
	return client
}
