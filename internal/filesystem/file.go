package filesystem

import (
    "io"
    "os"
    "path/filepath"
    "time"
    "log"

    "github.com/forscht/ddrv/internal/config"
    "github.com/forscht/ddrv/internal/dataprovider"
    "github.com/forscht/ddrv/pkg/cache"
    "github.com/forscht/ddrv/pkg/ddrv"
)

type File struct {
    id    string
    name  string
    dir   bool
    size  int64
    mtime time.Time

    flag         int
    off          int64
    data         []*dataprovider.Node
    readDirCount int

    mgr         *ddrv.Manager
    chunks      []*ddrv.Attachment
    streamWrite io.WriteCloser
    streamRead  io.ReadCloser
}

func (f *File) Size() int64                { return f.size }
func (f *File) ModTime() time.Time         { return f.mtime }
func (f *File) IsDir() bool                { return f.dir }
func (f *File) Sys() interface{}           { return nil }
func (f *File) Stat() (os.FileInfo, error) { return f, nil }
func (f *File) Sync() error                { return nil }

func (f *File) Truncate(_ int64) error                 { return ErrNotSupported }
func (f *File) WriteAt(_ []byte, _ int64) (int, error) { return 0, ErrNotSupported }

func (f *File) Name() string {
    _, name := filepath.Split(f.name)
    if name == "" {
        return "/"
    }
    return name
}

func (f *File) Mode() os.FileMode {
    if f.IsDir() {
        return os.ModeDir | 0755
    }
    return 0444
}

func (f *File) Readdirnames(n int) ([]string, error) {
    if !f.IsDir() {
        return nil, ErrIsNotDir
    }
    fi, err := f.Readdir(n)
    names := make([]string, len(fi))
    for i, f := range fi {
        _, names[i] = filepath.Split(f.Name())
    }
    return names, err
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
    if !f.IsDir() {
        return nil, ErrIsNotDir
    }
    normalizedName := filepath.Clean(f.name)
    cache.GlobalCache.Mu.RLock()
    if cachedNodes, ok := cache.GlobalCache.Nodes[normalizedName]; ok {
        cache.GlobalCache.Mu.RUnlock()
        log.Printf("Serving %s from cache (%d entries)", normalizedName, len(cachedNodes))
        fis := make([]os.FileInfo, len(cachedNodes))
        for i, node := range cachedNodes {
            fis[i] = convertToAferoFile(node)
        }
        if count > 0 {
            start := f.readDirCount
            if start >= len(fis) {
                return nil, io.EOF
            }
            end := start + count
            if end > len(fis) {
                end = len(fis)
            }
            f.readDirCount = end
            return fis[start:end], nil
}
        return fis, nil
    }
    cache.GlobalCache.Mu.RUnlock()
    log.Printf("Cache miss for %s, querying database", normalizedName)

    files, err := dataprovider.Ls(f.name, count, f.readDirCount)
    if err != nil {
        return nil, err
    }
    entries := make([]os.FileInfo, len(files))
    for i, file := range files {
        entries[i] = convertToAferoFile(file)
    }
    if count > 0 && len(entries) == 0 {
        err = io.EOF
    }
    f.readDirCount += len(entries)

    return entries, err
}

func (f *File) Read(p []byte) (n int, err error) {
    if f.IsDir() {
        return 0, ErrIsDir
    }
    if f.streamRead == nil {
        if err := f.openReadStream(0); err != nil {
            return 0, err
        }
    }
    n, err = f.streamRead.Read(p)
    if err != nil && err != io.EOF {
        return n, err
    }
    f.off += int64(n)
    return n, err
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
    if f.IsDir() {
        return 0, ErrIsDir
    }
    if _, err := f.Seek(off, io.SeekCurrent); err != nil {
        return 0, err
    }
    return f.Read(p)
}

func (f *File) WriteString(s string) (ret int, err error) {
    if f.IsDir() {
        return 0, ErrIsDir
    }
    return f.Write([]byte(s))
}

func (f *File) Write(p []byte) (int, error) {
    if f.IsDir() {
        return 0, ErrIsDir
    }
    if !CheckFlag(os.O_WRONLY, f.flag) {
        return 0, ErrNotSupported
    }
    if f.streamWrite == nil {
        if CheckFlag(os.O_APPEND, f.flag) {
            if err := dataprovider.DeleteFileNodes(f.id); err != nil {
                return 0, err
            }
        }
        if config.AsyncWrite() {
            f.streamWrite = f.mgr.NewNWriter(func(chunk *ddrv.Attachment) {
                f.chunks = append(f.chunks, chunk)
            })
        } else {
            f.streamWrite = f.mgr.NewWriter(func(chunk *ddrv.Attachment) {
                f.chunks = append(f.chunks, chunk)
            })
        }
    }
    n, err := f.streamWrite.Write(p)
    return n, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
    if f.IsDir() {
        return 0, ErrIsDir
    }
    if !CheckFlag(os.O_RDONLY, f.flag) {
        return 0, ErrNotSupported
    }
    pos := int64(0)
    switch whence {
    case io.SeekStart:
        pos = offset
    case io.SeekCurrent:
        pos = f.off + offset
    case io.SeekEnd:
        pos = f.Size() - offset
    }
    if pos < 0 {
        return 0, ErrInvalidSeek
    }
    if f.streamRead != nil {
        if err := f.streamRead.Close(); err != nil {
            return 0, err
        }
    }
    f.streamRead = nil
    if err := f.openReadStream(pos); err != nil {
        return 0, err
    }
    return pos, nil
}

func (f *File) Close() error {
    if f.streamWrite != nil {
        if err := f.streamWrite.Close(); err != nil {
            return err
        }
        if len(f.chunks) == 1 && f.chunks[0].Size == 0 {
            return nil
        }
        nodes := make([]*dataprovider.Node, len(f.chunks))
        for i, chunk := range f.chunks {
            nodes[i] = convertToNode(chunk)
        }
        err := dataprovider.CreateFileNodes(f.id, nodes)
        if err != nil {
            return err
        }
        f.streamWrite = nil
    }
    if f.streamRead != nil {
        if err := f.streamRead.Close(); err != nil {
            return err
        }
        f.streamRead = nil
    }
    return nil
}

func (f *File) openReadStream(startAt int64) error {
    chunks := make([]ddrv.Attachment, len(f.data))
    for i, node := range f.data {
        chunks[i] = ddrv.Attachment{URL: node.URL, Size: node.Size}
    }
    stream, err := f.mgr.NewReader(chunks, startAt)
    if err != nil {
        return err
    }
    f.streamRead = stream
    return nil
}

func convertToNode(chunk *ddrv.Attachment) *dataprovider.Node {
    return &dataprovider.Node{URL: chunk.URL, Size: chunk.Size}
}

// ConvertToFile konvertiert ein dataprovider.File in ein *File (WebDAV-kompatibel).
func ConvertToFile(df *dataprovider.File) *File {
    return &File{id: df.ID, name: df.Name, dir: df.Dir, size: df.Size, mtime: df.MTime}
}

// SetFlag setzt den Zugriff-Flag in der File-Struktur.
func (f *File) SetFlag(flag int) {
    f.flag = flag
}
