package keeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/progrium/keepsync/internal/api"
)

// MappingFile is the file in a kept folder's root that names its permanode.
// Syncthing carries it to every node so all nodes agree on the permanode.
const MappingFile = ".keepsync"

// Mapping is the content of a .keepsync file.
type Mapping struct {
	Version   int         `json:"version"`
	Folder    string      `json:"folder"`
	Permanode api.BlobRef `json:"permanode"`
}

// ErrNoMapping is returned by ReadMapping when the folder has no .keepsync.
var ErrNoMapping = errors.New("keeper: no " + MappingFile + " file")

// ReadMapping reads dir/.keepsync.
func ReadMapping(dir string) (Mapping, error) {
	b, err := os.ReadFile(filepath.Join(dir, MappingFile))
	if errors.Is(err, os.ErrNotExist) {
		return Mapping{}, ErrNoMapping
	}
	if err != nil {
		return Mapping{}, err
	}
	var m Mapping
	if err := json.Unmarshal(b, &m); err != nil {
		return Mapping{}, fmt.Errorf("keeper: bad %s: %w", MappingFile, err)
	}
	if m.Permanode == "" {
		return Mapping{}, fmt.Errorf("keeper: %s has no permanode", MappingFile)
	}
	return m, nil
}

// WriteMapping writes dir/.keepsync atomically. It leaves an identical file
// untouched so Syncthing sees no change.
func WriteMapping(dir string, m Mapping) error {
	if m.Version == 0 {
		m.Version = 1
	}
	if cur, err := ReadMapping(dir); err == nil && cur == m {
		return nil
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(dir, ".keepsync-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, MappingFile))
}

// Keep marks folder (synced at dir) kept: it asks the registry for the
// folder's permanode, creating it on first use, and writes .keepsync. This is
// the core of `keepsync keep <folder>`. If dir already has a .keepsync naming
// a different permanode, Keep refuses rather than split the archive.
func Keep(ctx context.Context, reg api.RegistryClient, folder, dir string, p api.KeepPolicy) (api.BlobRef, error) {
	p = WithDefaults(p)
	if cur, err := ReadMapping(dir); err == nil {
		if p.Permanode != "" && p.Permanode != cur.Permanode {
			return "", fmt.Errorf("keeper: %s names %s, policy names %s", MappingFile, cur.Permanode, p.Permanode)
		}
		p.Permanode = cur.Permanode
	} else if !errors.Is(err, ErrNoMapping) {
		return "", err
	}
	check := p
	if check.Permanode == "" {
		check.Permanode = "pending"
	}
	if err := ValidatePolicy(check); err != nil {
		return "", err
	}
	pn, err := reg.Keep(ctx, folder, p)
	if err != nil {
		return "", err
	}
	if p.Permanode != "" && pn != p.Permanode {
		return "", fmt.Errorf("keeper: registry returned permanode %s, folder has %s", pn, p.Permanode)
	}
	if err := WriteMapping(dir, Mapping{Folder: folder, Permanode: pn}); err != nil {
		return "", err
	}
	return pn, nil
}
