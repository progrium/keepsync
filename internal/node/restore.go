package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/keeper"
)

// RestoreParams is the body of MethodRestore.
type RestoreParams struct {
	Folder string `json:"folder"`
	// Path is a file or directory inside the folder ("" or "." for all).
	Path string `json:"path,omitempty"`
	// To is where restored files go, keeping their relative paths.
	// Default the folder itself.
	To string `json:"to,omitempty"`
	// Force overwrites local files that differ from the kept version.
	Force bool `json:"force,omitempty"`
}

// RestoreResult is the result of MethodRestore.
type RestoreResult struct {
	Dir       string   `json:"dir"`
	Restored  []string `json:"restored"`
	Unchanged []string `json:"unchanged,omitempty"` // already present with the kept size and mtime
	Skipped   []string `json:"skipped,omitempty"`   // present but different (use force)
	Errors    []string `json:"errors,omitempty"`
	// Reexpire is set when files were restored into a folder whose expire
	// window they are already past: the next keeper pass removes them
	// from sync again.
	Reexpire bool `json:"reexpire,omitempty"`
}

// fetcher returns a Perkeep client that reaches the server's perkeepd over
// the transport, for reading kept files back (nodes may fetch blobs).
func (n *Node) fetcher() (*client.Client, error) {
	c, err := client.New(client.OptionServer("http://perkeep.keepsync"), client.OptionNoExternalConfig())
	if err != nil {
		return nil, err
	}
	server := api.Addr(n.cfg.Server)
	c.SetHTTPClient(&http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return n.tr.Dial(ctx, server, api.PortPerkeep)
		},
	}})
	c.Logger = log.New(io.Discard, "", 0)
	return c, nil
}

// restore pulls kept files of a folder back out of Perkeep, including
// files that have expired from sync. It reads the folder's current tree.
func (n *Node) restore(ctx context.Context, p RestoreParams) (RestoreResult, error) {
	var res RestoreResult
	lf, err := n.localFolder(p.Folder)
	if err != nil {
		return res, err
	}
	m, err := keeper.ReadMapping(lf.Path)
	if err != nil {
		return res, fmt.Errorf("folder %s is not kept here (%w)", p.Folder, err)
	}
	prefix := strings.Trim(path.Clean("/"+filepath.ToSlash(p.Path)), "/")
	dest := lf.Path
	if p.To != "" {
		if dest, err = absDir(p.To); err != nil {
			return res, err
		}
	}
	res.Dir = dest

	tree, err := n.reg.Paths(ctx, m.Permanode)
	if err != nil {
		return res, err
	}
	var names []string
	for name := range tree {
		if prefix == "" || name == prefix || strings.HasPrefix(name, prefix+"/") {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return res, fmt.Errorf("nothing kept under %q in %s", p.Path, p.Folder)
	}
	sort.Strings(names)

	var expire time.Duration
	n.mu.Lock()
	for _, f := range n.member.Folders {
		if f.Name == p.Folder && f.Keep != nil {
			expire = f.Keep.Expire
		}
	}
	n.mu.Unlock()

	c, err := n.fetcher()
	if err != nil {
		return res, err
	}
	for _, name := range names {
		e := tree[name]
		dst := filepath.Join(dest, filepath.FromSlash(name))
		if !strings.HasPrefix(dst, dest+string(filepath.Separator)) {
			res.Errors = append(res.Errors, name+": path escapes the folder")
			continue
		}
		status, mt, err := restoreFile(ctx, c, e.File, dst, p.Force)
		switch {
		case err != nil:
			res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", name, err))
		case status == "unchanged":
			res.Unchanged = append(res.Unchanged, name)
		case status == "skipped":
			res.Skipped = append(res.Skipped, name)
		default:
			res.Restored = append(res.Restored, name)
			if dest == lf.Path && expire > 0 && time.Since(mt) > expire {
				res.Reexpire = true
			}
		}
	}
	if len(res.Restored) > 0 && dest == lf.Path {
		n.sy.Scan(ctx, lf.ID, prefix)
	}
	return res, nil
}

// restoreFile writes the kept file ref to dst with its mtime. A file
// already at dst with the same size and mtime is left alone; one that
// differs is left alone unless force.
func restoreFile(ctx context.Context, f blob.Fetcher, ref api.BlobRef, dst string, force bool) (string, time.Time, error) {
	br, ok := blob.Parse(string(ref))
	if !ok {
		return "", time.Time{}, fmt.Errorf("bad file ref %q", ref)
	}
	fr, err := schema.NewFileReader(ctx, f, br)
	if err != nil {
		return "", time.Time{}, err
	}
	defer fr.Close()
	mt := fr.ModTime()
	if fi, err := os.Lstat(dst); err == nil {
		if fi.Mode().IsRegular() && fi.Size() == fr.Size() && fi.ModTime().Equal(mt) {
			return "unchanged", mt, nil
		}
		if !force {
			return "skipped", mt, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", mt, err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", mt, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".keepsync-restore-*")
	if err != nil {
		return "", mt, err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, fr); err != nil {
		tmp.Close()
		return "", mt, err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return "", mt, err
	}
	if err := tmp.Close(); err != nil {
		return "", mt, err
	}
	if !mt.IsZero() {
		if err := os.Chtimes(tmp.Name(), mt, mt); err != nil {
			return "", mt, err
		}
	}
	return "restored", mt, os.Rename(tmp.Name(), dst)
}
