package webdav

import (
	"context"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"golang.org/x/net/webdav"
)

type webdavFs struct {
	dfs afero.Fs
}

func NewFs(dfs afero.Fs) webdav.FileSystem {
	return &webdavFs{dfs: dfs}
}

func (wf *webdavFs) Mkdir(_ context.Context, name string, perm os.FileMode) error {
	return wf.dfs.Mkdir(filepath.Clean(name), perm)
}

func (wf *webdavFs) OpenFile(_ context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	return wf.dfs.OpenFile(filepath.Clean(name), flag, perm)
}

func (wf *webdavFs) RemoveAll(_ context.Context, name string) error {
	return wf.dfs.RemoveAll(filepath.Clean(name))
}

func (wf *webdavFs) Rename(_ context.Context, oldName, newName string) error {
	return wf.dfs.Rename(filepath.Clean(oldName), filepath.Clean(newName))
}

func (wf *webdavFs) Stat(_ context.Context, name string) (os.FileInfo, error) {
	return wf.dfs.Stat(filepath.Clean(name))
}