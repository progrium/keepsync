package zerofs

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// PinnedVersion is the ZeroFS release keepsync is built and tested against.
// The bucket format is tied to the ZeroFS version, so keepsync never
// upgrades it on its own; changing this pin is a deliberate source change.
const PinnedVersion = "v2.3.7"

// Release describes one ZeroFS release: where to fetch it and the SHA-256 of
// the release tarball and of each platform binary inside it.
type Release struct {
	Version    string
	URL        string // the multi-platform tarball
	TarballSHA string
	// Binaries maps GOOS/GOARCH to the binary's name inside the tarball and
	// its SHA-256.
	Binaries map[string]Binary
}

// Binary is one platform binary inside a release tarball.
type Binary struct {
	Name   string
	SHA256 string
}

// Pinned is the release keepsync downloads and checks binaries against.
var Pinned = Release{
	Version:    PinnedVersion,
	URL:        "https://github.com/Barre/ZeroFS/releases/download/" + PinnedVersion + "/zerofs-pgo-multiplatform.tar.gz",
	TarballSHA: "8efa26f7abee82e5b720e57ef9c307d0f745a22cd0d81a6d3bbc095db56707c8",
	Binaries: map[string]Binary{
		"linux/amd64":   {"zerofs-linux-amd64-pgo", "4dc30fa42b8577398f0524e395a15632e7457e4b6d5318fabd7ddc0a2bcc414f"},
		"linux/arm64":   {"zerofs-linux-arm64-pgo", "18716e03b0967b627214962c032e6c6ae5365d84d73ca1ff3e26d2b746e4d6af"},
		"darwin/amd64":  {"zerofs-darwin-x86_64-pgo", "83f1ee00989a08593e54401d7e9566c56dbe8f722ab068e0108541021ab76730"},
		"darwin/arm64":  {"zerofs-darwin-aarch64-pgo", "83fde721567f982abd475aea25956b6281e145c7d93972fc3e8664ff82ba332f"},
		"freebsd/amd64": {"zerofs-freebsd-amd64-pgo", "afb5a3f26d6596a22c16a4392c6adba158075a3ec4c0ea8f487c24ef8b802a48"},
	},
}

// ErrNotFound means no zerofs binary was found by FindBinary.
var ErrNotFound = errors.New("zerofs: binary not found")

// ErrUnsupportedPlatform means the release has no binary for this platform.
var ErrUnsupportedPlatform = errors.New("zerofs: no release binary for this platform")

// Platform is the GOOS/GOARCH key used in Release.Binaries.
func Platform() string { return runtime.GOOS + "/" + runtime.GOARCH }

// LocalPath is where keepsync keeps its own copy of zerofs in dataDir.
func LocalPath(dataDir string) string { return filepath.Join(dataDir, "bin", "zerofs") }

// FindBinary returns the zerofs binary to run, in lookup order: explicit (the
// --zerofs flag or config value) if set, then zerofs on PATH, then
// keepsync's own copy in dataDir. An explicit path that does not exist is an
// error rather than a fallthrough, so a typo is not silently ignored.
func FindBinary(explicit, dataDir string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("zerofs: --zerofs %s: %w", explicit, err)
		}
		return explicit, nil
	}
	if p, err := exec.LookPath("zerofs"); err == nil {
		return p, nil
	}
	if dataDir != "" {
		p := LocalPath(dataDir)
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", ErrNotFound
}

// CheckPin reports whether the binary at path is the pinned build for this
// platform. A mismatch is not an error: callers warn and carry on.
func (r Release) CheckPin(path string) (bool, error) {
	b, ok := r.Binaries[Platform()]
	if !ok {
		return false, nil
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return false, err
	}
	return sum == b.SHA256, nil
}

// Install downloads the release tarball, verifies the tarball's and the
// platform binary's SHA-256, and installs the binary at LocalPath(dataDir).
// Nothing is written to that path unless both checks pass. client may be nil.
func (r Release) Install(ctx context.Context, client *http.Client, dataDir string) (string, error) {
	want, ok := r.Binaries[Platform()]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrUnsupportedPlatform, Platform())
	}
	if client == nil {
		client = http.DefaultClient
	}
	dst := LocalPath(dataDir)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("zerofs: download %s: %w", r.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("zerofs: download %s: %s", r.URL, resp.Status)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".zerofs-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	defer tmp.Close()

	tarHash := sha256.New()
	gz, err := gzip.NewReader(io.TeeReader(resp.Body, tarHash))
	if err != nil {
		return "", fmt.Errorf("zerofs: release tarball: %w", err)
	}
	binHash := sha256.New()
	found := false
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("zerofs: release tarball: %w", err)
		}
		if h.Name != want.Name || h.Typeflag != tar.TypeReg {
			continue
		}
		if _, err := io.Copy(io.MultiWriter(tmp, binHash), tr); err != nil {
			return "", fmt.Errorf("zerofs: extract %s: %w", want.Name, err)
		}
		found = true
	}
	// Drain the rest so the tarball hash covers every byte.
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return "", fmt.Errorf("zerofs: download %s: %w", r.URL, err)
	}
	if got := hex.EncodeToString(tarHash.Sum(nil)); got != r.TarballSHA {
		return "", fmt.Errorf("zerofs: release tarball SHA-256 mismatch: got %s, want %s", got, r.TarballSHA)
	}
	if !found {
		return "", fmt.Errorf("zerofs: %s not in release tarball", want.Name)
	}
	if got := hex.EncodeToString(binHash.Sum(nil)); got != want.SHA256 {
		return "", fmt.Errorf("zerofs: %s SHA-256 mismatch: got %s, want %s", want.Name, got, want.SHA256)
	}
	if err := tmp.Chmod(0o755); err != nil {
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", err
	}
	return dst, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
