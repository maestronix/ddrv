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

// Manager provides an interface to read and write large files to Discord by splitting the files into
// smaller chunks, uploading or downloading these chunks through an external refresh API, and reassembling
// them into the original files.
type Manager struct {
	chunkSize int             // Size of each chunk of data to be processed
	webhooks  []string        // List of webhook URLs (unused im Refresh-Kontext)
	clients   []*Rest         // List of webhook clients corresponding to the webhook URLs
	lastCIdx  int             // Index of the last used webhook client
	traceCtx  context.Context // Context for HTTP client tracing
	mu        sync.Mutex
}

// NewManager returns a new instance of Manager with specified chunk size and webhook URLs.
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

// refreshPayload ist die Struktur für den API-Payload.
type refreshPayload struct {
	AttachmentURLs []string `json:"attachment_urls"`
}

// refreshResponse repräsentiert die API-Antwort.
type refreshResponse struct {
	RefreshedURLs []struct {
		Refreshed string `json:"refreshed"`
	} `json:"refreshed_urls"`
}

// refreshJob repräsentiert einen Job zum Refreshen eines Chunk-URLs.
type refreshJob struct {
	url          string // Ursprüngliche URL aus der DB.
	start        int    // Start-Byte (z.B. 0)
	end          int    // End-Byte (z.B. node.Size - 1)
	refreshedURL string // Hier wird der aktualisierte URL gespeichert.
}

// refreshURL führt den Refresh-Vorgang für eine einzelne URL durch.
// Es wird ein POST-Request an die API geschickt, die als Antwort den neuen URL liefert.
func (mgr *Manager) refreshURL(url string, start, end int) (string, error) {
	// Erstelle den Payload. In diesem Beispiel ignorieren wir start/end.
	payload := refreshPayload{
		AttachmentURLs: []string{url},
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("refreshURL: failed to marshal payload: %v", err)
		return "", err
	}

	// API-Endpunkt: Beachte, dass hier kein Bot-Token benötigt wird.
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

// refreshChunks nimmt ein Slice von refreshJob und aktualisiert alle URLs parallel mithilfe eines Worker-Pools.
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

	// Starte maxWorkers Worker.
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Sende Zeiger auf alle Jobs in den Channel.
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
