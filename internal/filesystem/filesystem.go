package filesystem

import (
    "errors"
    "os"
    "path/filepath"
    "time"
    "log"

    "github.com/spf13/afero"
    "github.com/forscht/ddrv/internal/dataprovider"
    "github.com/forscht/ddrv/pkg/cache"
    "github.com/forscht/ddrv/pkg/ddrv"
)

var (
    ErrIsDir        = &os.PathError{Err: errors.New("is a directory")}
    ErrIsNotDir     = &os.PathError{Err: errors.New("is not a directory")}
    ErrNotSupported = &os.PathError{Err: errors.New("fs doesn't support this operation")}
    ErrInvalidSeek  = &os.PathError{Err: errors.New("invalid seek offset")}
    ErrReadOnly     = os.ErrPermission
)

type Fs struct {
    mgr *ddrv.Manager
}

func New(mgr *ddrv.Manager) afero.Fs {
    dataprovider.New() // Initialisiert den Provider
    return NewLogFs(&Fs{mgr: mgr})
}

func (fs *Fs) Name() string                        { return "LogFs" }
func (fs *Fs) Chown(_ string, _, _ int) error      { return ErrNotSupported }
func (fs *Fs) Chmod(_ string, _ os.FileMode) error { return ErrNotSupported }
func (fs *Fs) Chtimes(name string, _ time.Time, mtime time.Time) error {
    return dataprovider.ChMTime(name, mtime)
}

func (fs *Fs) Create(name string) (afero.File, error) {
    if err := dataprovider.Touch(name); err != nil {
        return nil, err
    }
    return fs.OpenFile(name, os.O_WRONLY, 0666)
}

func (fs *Fs) Mkdir(name string, _ os.FileMode) error {
    parent, _ := filepath.Split(name)
    file, err := dataprovider.Stat(parent)
    if err != nil {
        return err
    }
    if !file.Dir {
        return ErrIsNotDir
    }
    return dataprovider.Mkdir(name)
}

func (fs *Fs) MkdirAll(path string, _ os.FileMode) error {
    return dataprovider.Mkdir(path)
}

func (fs *Fs) Open(name string) (afero.File, error) {
    f, err := dataprovider.Stat(name)
    if err != nil {
        return nil, err
    }
    file := convertToAferoFile(f)
    file.flag = os.O_RDONLY
    file.mgr = fs.mgr
    if !file.dir {
        file.data, err = dataprovider.GetFileNodes(file.id)
        if err != nil {
            return nil, err
        }
    }
    return file, nil
}

func (fs *Fs) OpenFile(name string, flag int, _ os.FileMode) (afero.File, error) {
    if !CheckFlag(flag, os.O_WRONLY|os.O_RDONLY|os.O_CREATE|os.O_TRUNC|os.O_RDWR) {
        return nil, ErrReadOnly
    }

    f, err := dataprovider.Stat(name)
    if err != nil {
        if CheckFlag(os.O_CREATE, flag) {
            return fs.Create(name)
        }
        return nil, err
    }

    file := convertToAferoFile(f)
    file.flag = flag
    file.mgr = fs.mgr

    if CheckFlag(os.O_TRUNC, flag) {
        if err = dataprovider.DeleteFileNodes(file.id); err != nil {
            return nil, err
        }
    }

    if !file.dir {
        file.data, err = dataprovider.GetFileNodes(file.id)
        if err != nil {
            return nil, err
        }
    }
    return file, nil
}

func (fs *Fs) Remove(name string) error {
    parent, _ := filepath.Split(name)
    _, err := dataprovider.Stat(parent)
    if err != nil {
        return err
    }
    return dataprovider.Rm(name)
}

func (fs *Fs) RemoveAll(path string) error {
    return dataprovider.Rm(path)
}

func (fs *Fs) Rename(oldname, newname string) error {
    return dataprovider.Mv(oldname, newname)
}

func (fs *Fs) Stat(name string) (os.FileInfo, error) {
    f, err := dataprovider.Stat(name)
    if err != nil {
        return nil, os.ErrNotExist
    }
    return convertToAferoFile(f).Stat()
}

func (fs *Fs) ReadDir(name string) ([]os.FileInfo, error) {
    normalizedName := filepath.Clean(name)
    cache.GlobalCache.Mu.RLock()
    if cachedNodes, ok := cache.GlobalCache.Nodes[normalizedName]; ok {
        cache.GlobalCache.Mu.RUnlock()
        log.Printf("Serving %s from cache (%d entries)", normalizedName, len(cachedNodes))
        fis := make([]os.FileInfo, len(cachedNodes))
        for i, node := range cachedNodes {
            fis[i] = convertToAferoFile(node)
        }
        return fis, nil
    }
    cache.GlobalCache.Mu.RUnlock()
    log.Printf("Cache miss for %s, querying database", normalizedName)
    f, err := dataprovider.Stat(normalizedName)
    if err != nil {
        return nil, err
    }
    if !f.Dir {
        return nil, ErrIsNotDir
    }
    children, err := dataprovider.GetChild(f.ID)
    if err != nil {
        return nil, err
    }
    fis := make([]os.FileInfo, len(children))
    for i, child := range children {
        fis[i] = convertToAferoFile(child)
    }
    cache.GlobalCache.Mu.Lock()
    cache.GlobalCache.Nodes[name] = children
    cache.GlobalCache.Mu.Unlock()
    return fis, nil
}

func CheckFlag(flag int, allowedFlags int) bool {
    return flag == (flag & allowedFlags)
}

func convertToAferoFile(df *dataprovider.File) *File {
    return &File{id: df.ID, name: df.Name, dir: df.Dir, size: df.Size, mtime: df.MTime}
}