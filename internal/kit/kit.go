package kit

import (
	"archive/zip"
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"filippo.io/age"
	"perkeep.org/pkg/jsonsign"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/pkserver"
	"github.com/progrium/keepsync/internal/zerofs"
)

// FileName is the kit's file name, as server init and server kit write it.
const FileName = "keepsync-recovery.zip.age"

// FormatVersion is the kit format. Open refuses kits from a newer format.
const FormatVersion = 1

// Entries inside the zip.
const (
	entryManifest   = "kit.json"
	entrySums       = "SHA256SUMS"
	entryRecoverMD  = "RECOVER.md"
	entryKeepsync   = "bin/keepsync"
	entryZeroFS     = "bin/zerofs"
	entrySecring    = "perkeep/identity-secring.gpg"
	entryPerkeepd   = "perkeep/perkeepd.json"
	entryTailcatKey = "tailcat/server.key"
	entryConfig     = "server/config"
	entryRegistry   = "server/registry"
	entryZFSLicense = "licenses/ZEROFS-LICENSE"
	entryZFSSource  = "licenses/ZEROFS-SOURCE"
)

//go:embed licenses/ZEROFS-LICENSE
var zerofsLicense []byte

// scryptWorkFactor is the age scrypt work factor for new kits (2^18, about
// a second). Tests lower it.
var scryptWorkFactor = 18

// Source is everything Build puts in a kit. The CLI fills it from the
// server's config.
type Source struct {
	// ServerAddr is the server's tailcat address, for RECOVER.md and the
	// recovered server's sanity check.
	ServerAddr api.Addr
	// Storage is the bucket. Ideally its credentials are a read-only token;
	// Recover takes write credentials separately.
	Storage zerofs.Storage
	// ZeroFSPassword is the bucket's encryption password.
	ZeroFSPassword string
	// PerkeepDir is the server's pkserver.Config.Dir. Only the identity's
	// secret ring is taken from it.
	PerkeepDir string
	// TailcatKey is the path of the server's saved tailcat key.
	TailcatKey string
	// ConfigFile, if set, is the server's config file, restored verbatim.
	ConfigFile string
	// RegistryFile, if set, is the registry's state file. It is a
	// snapshot as of the kit's creation; members and folders added later
	// are re-registered by their nodes.
	RegistryFile string
	// ZeroFSBinary is the zerofs executable for this platform (see
	// zerofs.FindBinary).
	ZeroFSBinary string
	// KeepsyncBinary is the keepsync executable. Default os.Executable().
	KeepsyncBinary string
	// Now is the creation time. Default time.Now().
	Now time.Time
}

// Manifest is kit.json: the kit's metadata and the secrets small enough to
// hold inline.
type Manifest struct {
	Format         int            `json:"format"`
	Created        time.Time      `json:"created"`
	Platform       string         `json:"platform"` // GOOS/GOARCH of the binaries
	ServerAddr     api.Addr       `json:"serverAddr"`
	PerkeepKeyID   string         `json:"perkeepKeyID"`
	Storage        zerofs.Storage `json:"storage"`
	ZeroFSPassword string         `json:"zerofsPassword"`
	ZeroFSVersion  string         `json:"zerofsVersion"` // the pin keepsync was built with
	ZeroFSPinned   bool           `json:"zerofsPinned"`  // the bundled binary matches the pin
	HasConfig      bool           `json:"hasConfig"`
	HasRegistry    bool           `json:"hasRegistry"`
}

// Build writes an encrypted kit to path and returns the passphrase, which is
// shown to the user once and stored nowhere. The kit is written to a temp
// file next to path and renamed into place, owner-only.
func Build(src Source, path string) (passphrase string, err error) {
	passphrase, err = NewPassphrase()
	if err != nil {
		return "", err
	}
	if err := BuildWithPassphrase(src, path, passphrase); err != nil {
		return "", err
	}
	return passphrase, nil
}

// BuildWithPassphrase is Build with a passphrase the caller chose.
func BuildWithPassphrase(src Source, path, passphrase string) error {
	if passphrase == "" {
		return errors.New("kit: empty passphrase")
	}
	m, files, err := collect(src)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".kit-*")
	if err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if err := write(f, passphrase, m, files); err != nil {
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

// kitFile is one zip entry: either inline data or a file on disk.
type kitFile struct {
	name string
	data []byte
	path string
	mode os.FileMode
}

func collect(src Source) (Manifest, []kitFile, error) {
	var m Manifest
	if src.Storage.URL == "" {
		return m, nil, errors.New("kit: Source.Storage.URL is required")
	}
	if err := zerofs.ValidatePassword(src.ZeroFSPassword); err != nil {
		return m, nil, fmt.Errorf("kit: %w", err)
	}
	if src.ZeroFSBinary == "" {
		return m, nil, errors.New("kit: Source.ZeroFSBinary is required")
	}
	if src.KeepsyncBinary == "" {
		exe, err := os.Executable()
		if err != nil {
			return m, nil, fmt.Errorf("kit: keepsync binary: %w", err)
		}
		src.KeepsyncBinary = exe
	}
	if src.Now.IsZero() {
		src.Now = time.Now()
	}
	ring := pkserver.SecretRingPath(src.PerkeepDir)
	keyID, err := jsonsign.KeyIdFromRing(ring)
	if err != nil {
		return m, nil, fmt.Errorf("kit: perkeep identity: %w", err)
	}
	if src.TailcatKey == "" {
		return m, nil, errors.New("kit: Source.TailcatKey is required")
	}
	pinned, err := zerofs.Pinned.CheckPin(src.ZeroFSBinary)
	if err != nil {
		return m, nil, fmt.Errorf("kit: zerofs binary: %w", err)
	}
	m = Manifest{
		Format:         FormatVersion,
		Created:        src.Now.UTC(),
		Platform:       zerofs.Platform(),
		ServerAddr:     src.ServerAddr,
		PerkeepKeyID:   keyID,
		Storage:        src.Storage,
		ZeroFSPassword: src.ZeroFSPassword,
		ZeroFSVersion:  zerofs.PinnedVersion,
		ZeroFSPinned:   pinned,
		HasConfig:      src.ConfigFile != "",
		HasRegistry:    src.RegistryFile != "",
	}
	pkconf, _ := json.MarshalIndent(pkserver.HighLevelConfig(
		"$PERKEEP_DIR", "$ZEROFS_MOUNT", keyID, "(generated at start)"), "", "  ")
	files := []kitFile{
		{name: entryRecoverMD, data: recoverMD(m), mode: 0o644},
		{name: entryKeepsync, path: src.KeepsyncBinary, mode: 0o755},
		{name: entryZeroFS, path: src.ZeroFSBinary, mode: 0o755},
		{name: entrySecring, path: ring, mode: 0o600},
		{name: entryPerkeepd, data: append(pkconf, '\n'), mode: 0o644},
		{name: entryTailcatKey, path: src.TailcatKey, mode: 0o600},
		{name: entryZFSLicense, data: zerofsLicense, mode: 0o644},
		{name: entryZFSSource, data: []byte(zerofsSourceNote()), mode: 0o644},
	}
	if src.ConfigFile != "" {
		files = append(files, kitFile{name: entryConfig, path: src.ConfigFile, mode: 0o600})
	}
	if src.RegistryFile != "" {
		if _, err := os.Stat(src.RegistryFile); err == nil {
			files = append(files, kitFile{name: entryRegistry, path: src.RegistryFile, mode: 0o600})
		} else {
			m.HasRegistry = false
		}
	}
	return m, files, nil
}

func zerofsSourceNote() string {
	return fmt.Sprintf("ZeroFS %s is licensed under the GNU AGPL-3.0 (see ZEROFS-LICENSE).\n"+
		"keepsync runs it unmodified as a separate program.\n"+
		"Source for this exact version: https://github.com/Barre/ZeroFS/tree/%s\n",
		zerofs.PinnedVersion, zerofs.PinnedVersion)
}

// write streams the zip through age into w. Every entry but SHA256SUMS is
// hashed as it is written; SHA256SUMS goes last.
func write(w io.Writer, passphrase string, m Manifest, files []kitFile) error {
	rcpt, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	rcpt.SetWorkFactor(scryptWorkFactor)
	bw := bufio.NewWriterSize(w, 1<<20)
	aw, err := age.Encrypt(bw, rcpt)
	if err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	zw := zip.NewWriter(aw)
	sums := map[string]string{}
	man, _ := json.MarshalIndent(m, "", "  ")
	all := append([]kitFile{{name: entryManifest, data: append(man, '\n'), mode: 0o600}}, files...)
	for _, f := range all {
		sum, err := addEntry(zw, f, m.Created)
		if err != nil {
			return err
		}
		sums[f.name] = sum
	}
	sw, err := zw.CreateHeader(&zip.FileHeader{Name: entrySums, Method: zip.Deflate, Modified: m.Created})
	if err != nil {
		return err
	}
	if _, err := sw.Write(renderSums(sums)); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	if err := aw.Close(); err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	return bw.Flush()
}

func addEntry(zw *zip.Writer, f kitFile, mod time.Time) (string, error) {
	h := &zip.FileHeader{Name: f.name, Method: zip.Deflate, Modified: mod}
	h.SetMode(f.mode)
	ew, err := zw.CreateHeader(h)
	if err != nil {
		return "", err
	}
	hs := sha256.New()
	out := io.MultiWriter(ew, hs)
	if f.path == "" {
		out.Write(f.data)
	} else {
		in, err := os.Open(f.path)
		if err != nil {
			return "", fmt.Errorf("kit: %s: %w", f.name, err)
		}
		_, err = io.Copy(out, in)
		in.Close()
		if err != nil {
			return "", fmt.Errorf("kit: %s: %w", f.name, err)
		}
	}
	return hex.EncodeToString(hs.Sum(nil)), nil
}

// renderSums writes sha256sum(1) format, so `sha256sum -c SHA256SUMS` works
// on an unzipped kit.
func renderSums(sums map[string]string) []byte {
	names := make([]string, 0, len(sums))
	for n := range sums {
		names = append(names, n)
	}
	sort.Strings(names)
	var b bytes.Buffer
	for _, n := range names {
		fmt.Fprintf(&b, "%s  %s\n", sums[n], n)
	}
	return b.Bytes()
}

func parseSums(b []byte) (map[string]string, error) {
	sums := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		sum, name, ok := strings.Cut(line, "  ")
		if !ok || len(sum) != 64 {
			return nil, fmt.Errorf("kit: bad %s line %q", entrySums, line)
		}
		sums[name] = sum
	}
	return sums, nil
}

// NewPassphrase returns a random passphrase of 30 base32 characters (150
// bits) in groups of five, easy to write down and read back.
func NewPassphrase() (string, error) {
	b := make([]byte, 19)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	s := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))[:30]
	var groups []string
	for i := 0; i < len(s); i += 5 {
		groups = append(groups, s[i:i+5])
	}
	return strings.Join(groups, "-"), nil
}

// Kit is an opened, decrypted and verified kit.
type Kit struct {
	Manifest Manifest
	tmp      string
	zr       *zip.ReadCloser
}

// ErrPassphrase means the passphrase does not decrypt the kit.
var ErrPassphrase = errors.New("kit: wrong passphrase")

// Open decrypts the kit at path into a private temp file and verifies every
// entry against SHA256SUMS. Close removes the temp file.
func Open(path, passphrase string) (*Kit, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("kit: %w", err)
	}
	defer in.Close()
	id, err := age.NewScryptIdentity(passphrase)
	if err != nil {
		return nil, fmt.Errorf("kit: %w", err)
	}
	ar, err := age.Decrypt(bufio.NewReaderSize(in, 1<<20), id)
	if err != nil {
		var nm *age.NoIdentityMatchError
		if errors.As(err, &nm) {
			return nil, ErrPassphrase
		}
		return nil, fmt.Errorf("kit: decrypt: %w", err)
	}
	tmp, err := os.MkdirTemp("", "keepsync-kit-")
	if err != nil {
		return nil, err
	}
	k := &Kit{tmp: tmp}
	fail := func(err error) (*Kit, error) { k.Close(); return nil, err }
	zpath := filepath.Join(tmp, "kit.zip")
	zf, err := os.OpenFile(zpath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return fail(err)
	}
	if _, err := io.Copy(zf, ar); err != nil {
		zf.Close()
		return fail(fmt.Errorf("kit: decrypt: %w", err))
	}
	if err := zf.Close(); err != nil {
		return fail(err)
	}
	k.zr, err = zip.OpenReader(zpath)
	if err != nil {
		return fail(fmt.Errorf("kit: %w", err))
	}
	if err := k.verify(); err != nil {
		return fail(err)
	}
	b, err := k.read(entryManifest)
	if err != nil {
		return fail(err)
	}
	if err := json.Unmarshal(b, &k.Manifest); err != nil {
		return fail(fmt.Errorf("kit: %s: %w", entryManifest, err))
	}
	if k.Manifest.Format > FormatVersion {
		return fail(fmt.Errorf("kit: format %d is newer than this keepsync (%d)", k.Manifest.Format, FormatVersion))
	}
	return k, nil
}

// verify checks that SHA256SUMS lists exactly the other entries, and that
// each matches.
func (k *Kit) verify() error {
	b, err := k.read(entrySums)
	if err != nil {
		return err
	}
	sums, err := parseSums(b)
	if err != nil {
		return err
	}
	seen := 0
	for _, f := range k.zr.File {
		if f.Name == entrySums {
			continue
		}
		want, ok := sums[f.Name]
		if !ok {
			return fmt.Errorf("kit: %s is not in %s", f.Name, entrySums)
		}
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("kit: %s: %w", f.Name, err)
		}
		h := sha256.New()
		_, err = io.Copy(h, rc)
		rc.Close()
		if err != nil {
			return fmt.Errorf("kit: %s: %w", f.Name, err)
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != want {
			return fmt.Errorf("kit: %s: checksum mismatch", f.Name)
		}
		seen++
	}
	if seen != len(sums) {
		return fmt.Errorf("kit: %s lists %d entries, kit has %d", entrySums, len(sums), seen)
	}
	return nil
}

func (k *Kit) read(name string) ([]byte, error) {
	rc, err := k.zr.Open(name)
	if err != nil {
		return nil, fmt.Errorf("kit: %s: %w", name, err)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// Has reports whether the kit holds entry name.
func (k *Kit) Has(name string) bool {
	_, err := fs.Stat(k.zr, name)
	return err == nil
}

// Extract writes entry name to dst with mode perm, creating parent
// directories owner-only. It refuses to overwrite.
func (k *Kit) Extract(name, dst string, perm os.FileMode) error {
	rc, err := k.zr.Open(name)
	if err != nil {
		return fmt.Errorf("kit: %s: %w", name, err)
	}
	defer rc.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("kit: %w", err)
	}
	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Entries lists the kit's entries.
func (k *Kit) Entries() []string {
	var out []string
	for _, f := range k.zr.File {
		out = append(out, f.Name)
	}
	return out
}

// Close removes the decrypted copy.
func (k *Kit) Close() error {
	var err error
	if k.zr != nil {
		err = k.zr.Close()
	}
	return errors.Join(err, os.RemoveAll(k.tmp))
}
