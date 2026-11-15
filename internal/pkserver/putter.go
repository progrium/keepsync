package pkserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
)

// Putter is the node side of the server: an api.BlobPutter that uploads
// files to the server's perkeepd over the transport.
type Putter struct {
	c *client.Client
}

var _ api.BlobPutter = (*Putter)(nil)

// NewPutter returns a Putter that reaches the server at addr through t.
func NewPutter(t api.Transport, server api.Addr) (*Putter, error) {
	c, err := client.New(client.OptionServer(baseURL), client.OptionNoExternalConfig())
	if err != nil {
		return nil, fmt.Errorf("pkserver: client: %w", err)
	}
	c.SetHTTPClient(&http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return t.Dial(ctx, server, api.PortPerkeep)
		},
	}})
	c.Logger = log.New(logWriter{log.Printf}, "pkserver: putter: ", 0)
	return &Putter{c: c}, nil
}

// PutFile uploads the file at path, with its mtime, and returns its file
// schema blobref. Chunks the server already has are not re-sent.
func (p *Putter) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("pkserver: %s is not a regular file", path)
	}
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, p.c, filepath.Base(path), fi.ModTime(), f)
	if err != nil {
		return "", fmt.Errorf("pkserver: put %s: %w", path, err)
	}
	return api.BlobRef(ref.String()), nil
}
