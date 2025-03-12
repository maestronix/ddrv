package ddrv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

// ErrClosed is returned when a writer or reader is closed and caller is trying to read or write
var ErrClosed = errors.New("is closed")

// ErrAlreadyClosed is returned when the reader/writer is already closed
var ErrAlreadyClosed = errors.New("already closed")

// ErrInvalidWebhookURL is the error returned for an invalid webhook URL.
var ErrInvalidWebhookURL = errors.New("invalid webhook URL")

// Attachment represents a Discord attachment URL and Size.
type Attachment struct {
	URL   string `json:"url"`  // URL where the data is stored
	Size  int    `json:"size"` // Size of the data
	Start int64  // Start position of the data in the overall data sequence
	End   int64  // End position of the data in the overall data sequence
}

// Manager provides an interface to read and write large files by splitting them into smaller chunks,
// refreshing the chunk URLs via an external API, and reassembling them.
type Manager struct {
	chunkSize int             // Size of each chunk of data to be processed
	webhooks  []string        // List of webhook URLs (unused im Refresh-Kontext)
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

	// Initialize tracing context for HTTP requests.
	clientTrace := &httptrace.ClientTrace{}
	st.traceCtx = httptrace.WithClientTrace(context.Background(), clientTrace)

	return st, nil
}

// NewWriter creates a new ddrv.Writer instance that implements an io.WriterCloser.
func (mgr *Manager) NewWriter(onChunk func(chunk *Attachment)) io.WriteCloser {
	return NewWriter(onChunk, mgr.chunkSize, mgr)
}

// NewNWriter creates a new ddrv.NWriter instance that implements an io.WriterCloser.
func (mgr *Manager) NewNWriter(onChunk func(chunk *Attachment)) io.WriteCloser {
	return NewNWriter(onChunk, mgr.chunkSize, mgr)
}

// NewReader creates a new Reader instance that implements an io.ReadCloser.
func (mgr *Manager) NewReader(chunks []Attachment, pos int64) (io.ReadCloser, error) {
	return NewReader(chunks, pos, mgr)
}

// read fetches a range of data from the specified URL.
// It assumes the URL is already refreshed.
func (mgr *Manager) read(url string, start, end int) (io.ReadCloser, error) {
	log.Printf("read: Using URL %s for range bytes=%d-%d", url, start, end)
	req, err := http.NewRequestWithContext(mgr.traceCtx, http.MethodGet, url, nil)
	if err != nil {
		log.Printf("read: Error creating request for %s: %v", url, err)
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		log.Printf("read: Error executing request for %s: %v", url, err)
		return nil, err
	}
	if res.StatusCode != http.StatusPartialContent {
		log.Printf("read: Unexpected status %d for %s", res.StatusCode, url)
		res.Body.Close()
		return nil, fmt.Errorf("read: expected status %d but received %d", http.StatusPartialContent, res.StatusCode)
	}
	log.Printf("read: Successfully fetched data from %s", url)
	return res.Body, nil
}

// write creates a new attachment using the provided Reader.
func (mgr *Manager) write(r io.Reader) (*Attachment, error) {
	client := mgr.next()
	return client.CreateAttachment(r)
}

// next returns the next webhook client in round-robin fashion.
func (mgr *Manager) next() *Rest {
	mgr.mu.Lock()
	client := mgr.clients[mgr.lastCIdx]
	mgr.lastCIdx = (mgr.lastCIdx + 1) % len(mgr.clients)
	mgr.mu.Unlock()
	return client
}

// --- Refresh functionality for parallel chunk URL updating ---

type refreshPayload struct {
	AttachmentURLs []string `json:"attachment_urls"`
}

type refreshResponse struct {
	RefreshedURLs []struct {
		Refreshed string `json:"refreshed"`
	} `json:"refreshed_urls"`
}

// refreshJob represents a job to refresh a single chunk URL.
type refreshJob struct {
	url          string // Original URL from the DB.
	start        int    // Start byte (e.g., 0).
	end          int    // End byte (e.g., node.Size - 1).
	refreshedURL string // Will store the new, refreshed URL.
}

// refreshURL sends a POST request to the refresh API and returns the new URL.
func (mgr *Manager) refreshURL(url string, start, end int) (string, error) {
	payload := refreshPayload{
		AttachmentURLs: []string{url},
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("refreshURL: failed to marshal payload: %v", err)
		return "", err
	}

	// API endpoint (keine Authentifizierung erforderlich)
	refreshAPI := "https://api.animemoe.us/discord/refresh"
	req, err := http.NewRequest(http.MethodPost, refreshAPI, bytes.NewReader(jsonPayload))
	if err != nil {
		log.Printf("refreshURL: error creating request: %v", err)
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 1; attempt <= 3; attempt++ {
		log.Printf("refreshURL: Attempt %d for URL %s", attempt, url)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Printf("refreshURL: error reading response body: %v", err)
				return "", err
			}
			var rResp refreshResponse
			if err := json.Unmarshal(bodyBytes, &rResp); err != nil {
				log.Printf("refreshURL: error unmarshalling response: %v", err)
				return "", err
			}
			if len(rResp.RefreshedURLs) > 0 {
				newURL := rResp.RefreshedURLs[0].Refreshed
				log.Printf("refreshURL: successfully refreshed URL. New URL: %s", newURL)
				return newURL, nil
			}
		} else if err != nil {
			log.Printf("refreshURL: error on attempt %d for URL %s: %v", attempt, url, err)
		} else {
			log.Printf("refreshURL: unexpected status %d on attempt %d for URL %s", resp.StatusCode, attempt, url)
			resp.Body.Close()
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	log.Printf("refreshURL: failed to refresh URL %s after 3 attempts", url)
	return "", fmt.Errorf("refreshURL: failed to refresh URL %s after 3 attempts", url)
}

// refreshChunks processes a slice of refreshJob in parallel using a worker pool.
func (mgr *Manager) refreshChunks(jobs []refreshJob) error {
	const maxWorkers = 10
	jobChan := make(chan *refreshJob, len(jobs))
	errChan := make(chan error, len(jobs))
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for job := range jobChan {
			newURL, err := mgr.refreshURL(job.url, job.start, job.end)
			if err != nil {
				errChan <- err
			} else {
				job.refreshedURL = newURL
			}
		}
	}

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	for i := range jobs {
		jobChan <- &jobs[i]
	}
	close(jobChan)
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}
