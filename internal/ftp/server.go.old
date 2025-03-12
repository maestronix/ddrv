package ftp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/fclairamb/ftpserverlib"
	"github.com/spf13/afero"

	"github.com/forscht/ddrv/internal/config"
)

const IPResolveURL = "https://ipinfo.io/ip"

// Define custom error messages
var (
	ErrNoTLS                 = errors.New("TLS is not configured")    // Error for missing TLS configuration
	ErrBadUserNameOrPassword = errors.New("bad username or password") // Error for failed authentication
)

// New creates a new FTP server instance with the provided file system and address.
func New(
	fs afero.Fs,
) *ftpserver.FtpServer { // Return a pointer to an FTP server instance

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

	driver := &Driver{
		Fs:       fs,       // The file system to serve over FTP
		username: username, // Username for authentication
		password: password, // Password for authentication
		Settings: &ftpserver.Settings{
			ListenAddr:          addr,                         // The network address to listen on
			DefaultTransferType: ftpserver.TransferTypeBinary, // Default to binary transfer mode
			// Stooopid FTP thinks connection is idle, even when file transfer is going on.
			// Default is 900 seconds so, after which the server will drop the connection
			// Increased it to 24 hours to allow big file transfers
			IdleTimeout: 86400, // 24 hour
		},
	}

	// Enable PASV mode of portRange is supplied
	if portRange != nil {
		// Range of ports for passive FTP connections
		driver.Settings.PassiveTransferPortRange = portRange
		// Function to resolve the static IP of the server
		driver.Settings.PublicIPResolver = func(context ftpserver.ClientContext) (string, error) {
			resp, err := http.Get(IPResolveURL) // Fetch static IP
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

	// Instantiate the FTP server with the driver and return a pointer to it
	server := ftpserver.NewFtpServer(driver)

	return server
}

// Driver is the FTP server driver implementation.
type Driver struct {
	Fs       afero.Fs            // The file system to serve over FTP
	Debug    bool                // Debug mode flag
	Settings *ftpserver.Settings // The FTP server settings
	username string              // Username for authentication
	password string              // Password for authentication
}

// ClientConnected is called when a client is connected to the FTP server.
func (d *Driver) ClientConnected(cc ftpserver.ClientContext) (string, error) {
	log.Printf("new conn - addr:%s id: %d", cc.RemoteAddr(), cc.ID()) // Log the new connection details
	return "Ditto FTP Server", nil                                    // Return a welcome message
}

// ClientDisconnected is called when a client is disconnected from the FTP server.
func (d *Driver) ClientDisconnected(cc ftpserver.ClientContext) {
	log.Printf("lost conn - addr:%s id: %d", cc.RemoteAddr(), cc.ID()) // Log the lost connection details
}

// AuthUser authenticates a user during the FTP server login process.
func (d *Driver) AuthUser(_ ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
	// If authentication is required, check the provided username and password against the expected values
	if d.username != "" && d.username != user || d.password != "" && d.password != pass {
		return nil, ErrBadUserNameOrPassword // If either check fails, return an authentication error
	}
	return d.Fs, nil // If the checks pass or authentication is not required, proceed with the provided file system
}

// GetSettings returns the FTP server settings.
func (d *Driver) GetSettings() (*ftpserver.Settings, error) { return d.Settings, nil }

// GetTLSConfig returns the TLS configuration for the FTP server.
func (d *Driver) GetTLSConfig() (*tls.Config, error) { return nil, ErrNoTLS } // The server does not support TLS, so return a "no TLS" error
