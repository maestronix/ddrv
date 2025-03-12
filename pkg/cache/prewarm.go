package cache

import (
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/forscht/ddrv/internal/dataprovider"
)

type Cache struct {
	Nodes map[string][]*dataprovider.File
	Files map[string]*dataprovider.File
	Mu    sync.RWMutex
}

var GlobalCache = &Cache{
	Nodes: make(map[string][]*dataprovider.File),
	Files: make(map[string]*dataprovider.File),
}

// Prewarm lädt die gesamte Verzeichnisstruktur aus der DB in den Cache.
func Prewarm(provider dataprovider.Provider) {
	startTime := time.Now()
	log.Printf("Starting pre-warming at %v", startTime)

	rootNodes, err := dataprovider.GetChild(dataprovider.RootDirId)
	if err != nil {
		log.Fatalf("Failed to prewarm root: %v", err)
	}
	log.Printf("Loaded %d root nodes", len(rootNodes))

	var wg sync.WaitGroup
	resultChan := make(chan struct {
		path  string
		nodes []*dataprovider.File
	}, 10000)
	semaphore := make(chan struct{}, 100)

	var workerCount int
	var worker func(path, parentID string, depth int)
	worker = func(path, parentID string, depth int) {
		workerCount++
		defer wg.Done()
		semaphore <- struct{}{}
		defer func() { <-semaphore }()

		var children []*dataprovider.File
		// Bis zu 5 Versuche, um die Kinder abzurufen
		for attempt := 1; attempt <= 5; attempt++ {
			var err error
			children, err = dataprovider.GetChild(parentID)
			if err == nil {
				break
			}
			log.Printf("Error prewarming %s (depth %d, attempt %d): %v", path, depth, attempt, err)
			if attempt < 5 {
				sleepTime := time.Duration(attempt*attempt) * 100 * time.Millisecond
				time.Sleep(sleepTime)
			}
		}
		if children == nil {
			log.Printf("Failed to prewarm %s (depth %d) after 5 attempts", path, depth)
			return
		}

		normalizedPath := filepath.Clean(path)
		dirs, files := 0, 0
		var totalSize int64
		for _, child := range children {
			if child.Dir {
				dirs++
			} else {
				files++
			}
			totalSize += child.Size
		}
		log.Printf("Prewarmed %s (depth %d) with %d children (%d dirs, %d files, total size %d bytes)",
			normalizedPath, depth, len(children), dirs, files, totalSize)
		resultChan <- struct {
			path  string
			nodes []*dataprovider.File
		}{normalizedPath, children}
		for _, child := range children {
			if child.Dir {
				wg.Add(1)
				go worker(normalizedPath+"/"+child.Name, child.ID, depth+1)
			}
		}
	}

	// Starte Worker für jeden Root-Knoten
	for _, node := range rootNodes {
		if node.Dir {
			wg.Add(1)
			go worker("/"+node.Name, node.ID, 1)
		}
	}

	go func() {
		wg.Wait()
		close(resultChan)
		log.Printf("All workers completed")
	}()

	// Aufbau eines neuen Caches in lokalen Maps
	newNodes := make(map[string][]*dataprovider.File)
	newFiles := make(map[string]*dataprovider.File)
	totalEntries := 0
	totalFileSize := int64(0)
	for res := range resultChan {
		newNodes[res.path] = res.nodes

		// Speichere das Verzeichnis-Objekt für diesen Pfad, falls nicht bereits vorhanden.
		if _, exists := newFiles[res.path]; !exists {
			newFiles[res.path] = &dataprovider.File{
				ID:    "", // Kann leer bleiben oder als spezieller Wert gesetzt werden
				Name:  filepath.Base(res.path),
				Dir:   true,
				Size:  0,
				MTime: time.Now(),
			}
		}

		totalEntries += len(res.nodes)
		for _, node := range res.nodes {
			totalFileSize += node.Size
			fullPath := filepath.Join(res.path, node.Name)
			newFiles[fullPath] = node
		}
	}

	// Speichere explizit auch den Root-Pfad "/" in den Caches:
	newNodes["/"] = rootNodes
	rootFile := &dataprovider.File{
		ID:    dataprovider.RootDirId,
		Name:  "/",
		Dir:   true,
		Size:  0,
		MTime: time.Now(),
	}
	newFiles["/"] = rootFile

	// Atomarer Swap des globalen Caches
	GlobalCache.Mu.Lock()
	log.Printf("acquiring Lock to swap cache")
	GlobalCache.Nodes = newNodes
	GlobalCache.Files = newFiles
	GlobalCache.Mu.Unlock()
	log.Printf("unlocked, cache swap complete")

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	log.Printf("Pre-warming completed at %v, took %v, cached %d directories with %d total entries, total file size %d bytes, used %d workers",
		endTime, duration, len(newNodes), totalEntries, totalFileSize, workerCount)
}

// StartPeriodicCacheUpdate startet eine Hintergrundroutine, die den Prewarm-Cache in regelmäßigen Abständen aktualisiert.
func StartPeriodicCacheUpdate(interval time.Duration, provider dataprovider.Provider) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			<-ticker.C
			log.Printf("PeriodicCacheUpdate: Starting cache update...")
			Prewarm(provider)
			log.Printf("PeriodicCacheUpdate: Cache update finished.")
		}
	}()
}
