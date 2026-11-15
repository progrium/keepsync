package server

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/progrium/keepsync/internal/registry"
)

// FileStore is a registry.Store that keeps the state in one JSON file,
// replaced atomically on every save.
//
// The design keeps registry state in Perkeep so it comes back from the
// bucket on recovery. Until that store exists, the file lives in the server
// dir, and every save also writes the same bytes to Mirror on the ZeroFS
// mount (kit.BucketRegistryPath), so the next flush carries the current
// members and folders to the bucket for `keepsync recover`.
type FileStore struct {
	Path string
	// Mirror, if set, gets a copy of every save. A failed mirror write is
	// reported to Logf but does not fail the save: the registry keeps
	// working while ZeroFS has trouble, and the next save retries.
	Mirror string
	Logf   func(format string, args ...any)
	mu     sync.Mutex
}

// SyncMirror copies the saved state, if any, to Mirror. The server calls
// it once ZeroFS is up, so the bucket has the registry even before the
// first change.
func (s *FileStore) SyncMirror() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Mirror == "" {
		return nil
	}
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return writeFileAtomic(s.Mirror, b)
}

var _ registry.Store = (*FileStore)(nil)

func (s *FileStore) Load(ctx context.Context) (registry.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st registry.State
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	err = json.Unmarshal(b, &st)
	return st, err
}

func (s *FileStore) Save(ctx context.Context, st registry.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(s.Path, b); err != nil {
		return err
	}
	if s.Mirror != "" {
		if err := writeFileAtomic(s.Mirror, b); err != nil && s.Logf != nil {
			s.Logf("registry: mirror to %s: %v", s.Mirror, err)
		}
	}
	return nil
}

func writeFileAtomic(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
