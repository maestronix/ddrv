package ftp

import (
    "crypto/tls"
    "errors"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"

    "github.com/fclairamb/ftpserverlib"
    "github.com/spf13/afero"

    "github.com/forscht/ddrv/internal/config"
    "github.com/forscht/ddrv/internal/filesystem"
    "github.com/forscht/ddrv/pkg/cache"
    "github.com/forscht/ddrv/pkg/ddrv"
)

const IPResolveURL = "https://ipinfo.io/ip"

var (
    ErrNoTLS                 = errors.New("TLS is not configured")
    ErrBadUserNameOrPassword = errors.New("bad username or password")
)

func New(mgr *ddrv.Manager) *ftpserver.FtpServer {
    addr := config.FTPAddr()
    ptr := config.FTPPortRange()
    username := config.Username()
    password := config.Password()

    var portRange *ftpserver.PortRange
    if ptr != "" {
        portRange = &ftpserver.PortRange{}
        if _, err := fmt.Sscanf(ptr, "%d-%d", &portRange.Start, &portRange.End); err != nil {
            log.Fatalf("bad ftp port range %v", err)
        }
    }

    backendFs := filesystem.New(mgr)
    fs := cache.NewCacheFs(mgr, backendFs)

    driver := &Driver{
        Fs:       fs,
        username: username,
        password: password,
        Settings: &ftpserver.Settings{
            ListenAddr:          addr,
            DefaultTransferType: ftpserver.TransferTypeBinary,
            IdleTimeout:         86400,
        },
    }

    if portRange != nil {
        driver.Settings.PassiveTransferPortRange = portRange
        driver.Settings.PublicIPResolver = func(context ftpserver.ClientContext) (string, error) {
            resp, err := http.Get(IPResolveURL)
            if err != nil {
                return "", err
            }
            ip, err := io.ReadAll(resp.Body)
            if err != nil {
                return "", err
            }
            return string(ip), nil
        }
    }

    server := ftpserver.NewFtpServer(driver)
    return server
}

type Driver struct {
    Fs       afero.Fs
    Debug    bool
    Settings *ftpserver.Settings
    username string
    password string
}

func (d *Driver) ClientConnected(cc ftpserver.ClientContext) (string, error) {
    log.Printf("new conn - addr:%s id: %d at %v", cc.RemoteAddr(), cc.ID(), time.Now())
    return "Ditto FTP Server", nil
}

func (d *Driver) ClientDisconnected(cc ftpserver.ClientContext) {
    log.Printf("lost conn - addr:%s id: %d", cc.RemoteAddr(), cc.ID())
}

func (d *Driver) AuthUser(_ ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
    if d.username != "" && d.username != user || d.password != "" && d.password != pass {
        return nil, ErrBadUserNameOrPassword
    }
    return d.Fs, nil
}

func (d *Driver) GetSettings() (*ftpserver.Settings, error) { return d.Settings, nil }

func (d *Driver) GetTLSConfig() (*tls.Config, error) { return nil, ErrNoTLS }