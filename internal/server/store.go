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
// bucket on recovery. Until that store exists, this file lives in the
// server dir next to the Perkeep identity.
type FileStore struct {
	Path string
	mu   sync.Mutex
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
	return writeFileAtomic(s.Path, b)
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
