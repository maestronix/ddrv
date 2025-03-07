package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/joho/godotenv"

	"github.com/forscht/ddrv/internal/config"
	"github.com/forscht/ddrv/internal/filesystem"
	"github.com/forscht/ddrv/internal/ftp"
	"github.com/forscht/ddrv/internal/http"
	"github.com/forscht/ddrv/internal/webdav"
	"github.com/forscht/ddrv/pkg/cache"
	"github.com/forscht/ddrv/pkg/ddrv"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	_ = godotenv.Load()

	kong.Parse(config.New(), kong.Vars{
		"version": fmt.Sprintf("ddrv %s", version),
	})

	if config.ChunkSize() > 25*1024*1024 || config.ChunkSize() < 0 {
		log.Fatalf("ddrv: invalid chunkSize %d", config.ChunkSize())
	}

	// Erzeuge den Manager
	mgr, err := ddrv.NewManager(config.ChunkSize(), strings.Split(config.Webhooks(), ","))
	if err != nil {
		log.Fatalf("ddrv: failed to open ddrv mgr: %v", err)
	}

	// Erzeuge das Backend-Filesystem, das intern dataprovider.New() aufruft
	backend := filesystem.New(mgr)
	// Übergebe ein valides Backend an das CacheFs
	fs := cache.NewCacheFs(mgr, backend)

	// Starte den Prewarm-Prozess einmalig beim Start
	go cache.Prewarm(nil)
	// Starte zusätzlich eine Hintergrundroutine, die alle 15 Minuten den Cache aktualisiert.
	cache.StartPeriodicCacheUpdate(15*time.Minute, nil)

	errCh := make(chan error)

	if config.FTPAddr() != "" {
		go func() {
			// Nutze den Manager für den FTP-Server
			ftpServer := ftp.New(mgr)
			log.Printf("ddrv: starting FTP server on: %s", config.FTPAddr())
			errCh <- ftpServer.ListenAndServe()
		}()
	}
	if config.HTTPAddr() != "" {
		go func() {
			httpServer := http.New(mgr)
			log.Printf("ddrv: starting HTTP server on: %s", config.HTTPAddr())
			errCh <- httpServer.Listen(config.HTTPAddr())
		}()
	}
	if config.WDAddr() != "" {
		go func() {
			// Übergib das CacheFs (mit Backend) an den WebDAV-Server
			webdavServer := webdav.New(fs)
			log.Printf("ddrv: starting WEBDAV server on: %s", config.WDAddr())
			errCh <- webdavServer.ListenAndServe()
		}()
	}

	log.Fatalf("ddrv: ddrv error %v", <-errCh)
}
