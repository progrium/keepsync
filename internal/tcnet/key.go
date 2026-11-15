package tcnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/tailscale/tailcat"
)

var errNoKey = errors.New("tcnet: no saved key")

// NewKey returns a new identity with a pre-shared key and no region yet.
func NewKey() *tailcat.PrivateKey {
	k := tailcat.NewPrivateKey()
	k.Public.RegionID = -1 // pick by latency at first start
	return k
}

// LoadKey reads an identity saved by SaveKey, or by `tailcat genkey`.
func LoadKey(path string) (*tailcat.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", errNoKey, path)
	}
	if err != nil {
		return nil, err
	}
	var k tailcat.PrivateKey
	if err := json.Unmarshal(b, &k); err != nil {
		return nil, fmt.Errorf("tcnet: parse %s: %w", path, err)
	}
	if k.Private.IsZero() {
		return nil, fmt.Errorf("tcnet: %s has no private key", path)
	}
	if k.Public.PresharedKey.IsZero() {
		return nil, fmt.Errorf("tcnet: %s has no pre-shared key", path)
	}
	return &k, nil
}

// SaveKey writes an identity with owner-only permissions, atomically.
func SaveKey(path string, k *tailcat.PrivateKey) error {
	b, err := json.MarshalIndent(k, "", "\t")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".key-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
