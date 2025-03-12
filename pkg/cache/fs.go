package cache

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"
	"github.com/forscht/ddrv/internal/dataprovider"
	"github.com/forscht/ddrv/pkg/ddrv"
)

type CacheFs struct {
	mgr     *ddrv.Manager
	backend afero.Fs
}

func NewCacheFs(mgr *ddrv.Manager, backend afero.Fs) afero.Fs {
	// Überprüfe, ob das Backend gültig ist.
	if backend == nil {
		log.Printf("NewCacheFs: Backend is nil – bitte ein gültiges Backend übergeben")
		// Alternativ: Einen Fehler zurückgeben oder ein Standard-Backend verwenden.
	}
	return &CacheFs{mgr: mgr, backend: backend}
}

func (fs *CacheFs) Name() string { return "CacheFs" }

// Stat liefert aus dem Cache, falls vorhanden, ansonsten vom Backend.
func (fs *CacheFs) Stat(name string) (os.FileInfo, error) {
	normalizedName := filepath.Clean(name)
	GlobalCache.Mu.RLock()
	defer GlobalCache.Mu.RUnlock()
	if file, ok := GlobalCache.Files[normalizedName]; ok {
		log.Printf("Stat: Serving %s from cache", normalizedName)
		return &cacheFileInfo{file}, nil
	}
	log.Printf("Stat: Cache miss for %s, querying backend", normalizedName)
	return fs.backend.Stat(normalizedName)
}

// ReadDir liefert aus dem Cache (nur für Verzeichnisse) oder ruft das Backend auf.
func (fs *CacheFs) ReadDir(name string) ([]os.FileInfo, error) {
	normalizedName := filepath.Clean(name)
	GlobalCache.Mu.RLock()
	if nodes, ok := GlobalCache.Nodes[normalizedName]; ok {
		GlobalCache.Mu.RUnlock()
		log.Printf("ReadDir: Serving %s from cache (%d entries)", normalizedName, len(nodes))
		fis := make([]os.FileInfo, len(nodes))
		for i, node := range nodes {
			fis[i] = &cacheFileInfo{node}
		}
		return fis, nil
	}
	GlobalCache.Mu.RUnlock()
	log.Printf("ReadDir: Cache miss for %s, querying backend", normalizedName)
	file, err := fs.backend.Open(normalizedName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.Readdir(-1)
}

// Open: Für Dateien delegieren wir direkt an das Backend, da wir hier nicht den Dateiinhalt cachen wollen.
func (fs *CacheFs) Open(name string) (afero.File, error) {
	normalizedName := filepath.Clean(name)
	GlobalCache.Mu.RLock()
	if file, ok := GlobalCache.Files[normalizedName]; ok {
		GlobalCache.Mu.RUnlock()
		if !file.Dir {
			log.Printf("Open: %s is a file, delegating to backend", normalizedName)
			return fs.backend.Open(normalizedName)
		}
	}
	GlobalCache.Mu.RUnlock()
	log.Printf("Open: Cache miss for %s, querying backend", normalizedName)
	return fs.backend.Open(normalizedName)
}

// Die restlichen Operationen delegieren direkt an das Backend.
func (fs *CacheFs) Create(name string) (afero.File, error) {
	log.Printf("Create: Delegating %s to backend", name)
	return fs.backend.Create(name)
}
func (fs *CacheFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	log.Printf("OpenFile: Delegating %s to backend", name)
	return fs.backend.OpenFile(name, flag, perm)
}
func (fs *CacheFs) Mkdir(name string, perm os.FileMode) error {
	log.Printf("Mkdir: Delegating %s to backend", name)
	return fs.backend.Mkdir(name, perm)
}
func (fs *CacheFs) MkdirAll(path string, perm os.FileMode) error {
	log.Printf("MkdirAll: Delegating %s to backend", path)
	return fs.backend.MkdirAll(path, perm)
}
func (fs *CacheFs) Remove(name string) error {
	log.Printf("Remove: Delegating %s to backend", name)
	return fs.backend.Remove(name)
}
func (fs *CacheFs) RemoveAll(path string) error {
	log.Printf("RemoveAll: Delegating %s to backend", path)
	return fs.backend.RemoveAll(path)
}
func (fs *CacheFs) Rename(oldname, newname string) error {
	log.Printf("Rename: Delegating %s to %s to backend", oldname, newname)
	return fs.backend.Rename(oldname, newname)
}
func (fs *CacheFs) Chmod(name string, mode os.FileMode) error {
	log.Printf("Chmod: Delegating %s to backend", name)
	return fs.backend.Chmod(name, mode)
}
func (fs *CacheFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	log.Printf("Chtimes: Delegating %s to backend", name)
	return fs.backend.Chtimes(name, atime, mtime)
}
func (fs *CacheFs) Chown(name string, uid, gid int) error {
	log.Printf("Chown: Delegating %s to backend", name)
	return fs.backend.Chown(name, uid, gid)
}

// cacheFileInfo implementiert os.FileInfo
type cacheFileInfo struct {
	*dataprovider.File
}

func (f *cacheFileInfo) Name() string       { return f.File.Name }
func (f *cacheFileInfo) Size() int64        { return f.File.Size }
func (f *cacheFileInfo) Mode() os.FileMode {
	if f.File.Dir {
		return os.ModeDir | 0755
	}
	return 0444
}
func (f *cacheFileInfo) ModTime() time.Time { return f.File.MTime }
func (f *cacheFileInfo) IsDir() bool        { return f.File.Dir }
func (f *cacheFileInfo) Sys() interface{}   { return nil }
