package kit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"perkeep.org/pkg/auth"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/pkserver"
	"github.com/progrium/keepsync/internal/zerofs"
)

// replicaPrefix is the handler of the diskpacked storage on ZeroFS, as
// pkserver configures perkeepd.
const replicaPrefix = "/sto-zerofs/"

// BucketRegistryPath is where, relative to the ZeroFS mount, the server
// mirrors its registry state on every change, so recovery gets the
// current members and folders from the bucket rather than the kit's copy
// from when it was made. It sits beside the diskpacked packs, which
// diskpacked ignores.
const BucketRegistryPath = ".keepsync/registry.json"

// NewZeroFS makes the ZeroFS supervisor a restore runs. The default is
// zerofs.New; tests substitute a fake.
type NewZeroFS func(zerofs.Config) (api.ZeroFS, error)

func defaultNewZeroFS(c zerofs.Config) (api.ZeroFS, error) { return zerofs.New(c) }

// Credentials override the kit's bucket credentials, which are ideally a
// read-only token. Empty fields keep the kit's.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// RestoreStats describes one restore from the bucket.
type RestoreStats struct {
	// Blobs and Bytes were copied from the bucket's diskpacked replica into
	// the fresh local storage, and indexed.
	Blobs int64
	Bytes int64
	// BadBlobs failed their hash check when read from the bucket, and
	// were not restored.
	BadBlobs []api.BlobRef
	// ZeroFSStart is how long ZeroFS took to come up on the bucket.
	ZeroFSStart time.Duration
	// Copy is how long copying and indexing took.
	Copy time.Duration
	// Total is the whole restore, ZeroFS start to index caught up.
	Total time.Duration
	// Registry is where Recover took the registry state from: "bucket",
	// "kit" or "" for nowhere.
	Registry string
}

// restoreEnv is a running restored server: ZeroFS on the bucket and
// perkeepd on a fresh local dir.
type restoreEnv struct {
	zfs api.ZeroFS
	srv *pkserver.Server
	// registry is the registry state the server mirrored into the
	// bucket, or nil if there is none.
	registry []byte
	owner    *client.Client
	stats    RestoreStats
}

type restoreParams struct {
	storage    zerofs.Storage
	perkeepDir string
	zfsDataDir string
	mountDir   string
	mount      zerofs.MountMode
	mode       string // config.ModeStore or config.ModeCache
	newZeroFS  NewZeroFS
	logf       func(string, ...any)
	workers    int
}

// restore starts ZeroFS on the bucket, starts perkeepd with an empty local
// storage and index, and copies every blob of the diskpacked replica into
// it, which indexes them. The replica already holds every blob, so
// replicating them back writes nothing to the bucket.
//
// In cache mode the diskpacked store on ZeroFS is perkeepd's primary
// storage, so nothing is copied: every blob is read from the bucket,
// checked, and fed to the index.
func restore(ctx context.Context, k *Kit, p restoreParams) (_ *restoreEnv, err error) {
	start := time.Now()
	if p.newZeroFS == nil {
		p.newZeroFS = defaultNewZeroFS
	}
	if p.workers <= 0 {
		p.workers = 8
	}
	if p.logf == nil {
		p.logf = log.Printf
	}
	if err := ensureEmptyDir(p.perkeepDir); err != nil {
		return nil, err
	}
	bin := zerofs.LocalPath(p.zfsDataDir)
	if _, err := os.Stat(bin); errors.Is(err, os.ErrNotExist) {
		if err := k.Extract(entryZeroFS, bin, 0o755); err != nil {
			return nil, err
		}
	}
	if err := k.Extract(entrySecring, pkserver.SecretRingPath(p.perkeepDir), 0o600); err != nil {
		return nil, err
	}
	id, err := pkserver.Init(p.perkeepDir)
	if err != nil {
		return nil, err
	}
	if id.KeyID != k.Manifest.PerkeepKeyID {
		return nil, fmt.Errorf("kit: identity %s does not match the kit's %s", id.KeyID, k.Manifest.PerkeepKeyID)
	}

	z, err := p.newZeroFS(zerofs.Config{
		Binary:   bin,
		DataDir:  p.zfsDataDir,
		Storage:  p.storage,
		Password: k.Manifest.ZeroFSPassword,
		Mount:    p.mount,
		MountDir: p.mountDir,
	})
	if err != nil {
		return nil, err
	}
	if err := z.Start(ctx); err != nil {
		return nil, fmt.Errorf("kit: starting zerofs on %s: %w", p.storage.URL, err)
	}
	e := &restoreEnv{zfs: z}
	defer func() {
		if err != nil {
			e.close(context.WithoutCancel(ctx))
		}
	}()
	e.stats.ZeroFSStart = time.Since(start)
	if b, err := os.ReadFile(filepath.Join(z.Dir(), BucketRegistryPath)); err == nil {
		e.registry = b
	}

	cache := p.mode == config.ModeCache
	mode := pkserver.ModeStore
	if cache {
		mode = pkserver.ModeCache
	}
	e.srv, err = pkserver.Start(ctx, pkserver.Config{Mode: mode, Dir: p.perkeepDir, ZeroFS: z, Logf: p.logf})
	if err != nil {
		return e, err
	}
	e.owner, err = ownerClient(e.srv, "")
	if err != nil {
		return e, err
	}
	from, to, queues := replicaPrefix, e.owner, []string{"/sync/", "/sync-to-zerofs/"}
	if cache {
		from, queues = "/bs/", []string{"/sync/"}
		if to, err = ownerClient(e.srv, "/index/"); err != nil {
			return e, err
		}
	}
	replica, err := ownerClient(e.srv, from)
	if err != nil {
		return e, err
	}
	copyStart := time.Now()
	if err := copyBlobs(ctx, replica, to, p.workers, &e.stats); err != nil {
		return e, err
	}
	for _, q := range queues {
		if err := waitSynced(ctx, e.srv, q); err != nil {
			return e, err
		}
	}
	e.stats.Copy = time.Since(copyStart)
	e.stats.Total = time.Since(start)
	return e, nil
}

// close stops perkeepd and ZeroFS. perkeepd keeps its queue locks until
// the process exits, so a restored server is run by a new process.
func (e *restoreEnv) close(ctx context.Context) error {
	var errs []error
	if e.srv != nil {
		errs = append(errs, e.srv.Close())
	}
	if e.zfs != nil {
		errs = append(errs, e.zfs.Stop(ctx))
	}
	return errors.Join(errs...)
}

// copyBlobs copies every blob of from into to. Data blobs go first and
// schema blobs (files, permanodes, claims) after, so that when perkeepd
// sees a file it already has its chunks: blobpacked can pack the file and
// the index can resolve it without waiting.
func copyBlobs(ctx context.Context, from, to *client.Client, workers int, st *RestoreStats) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		firstErr error
		schemas  []pendingBlob
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	upload := func(br blob.Ref, b []byte) {
		_, err := to.Upload(ctx, &client.UploadHandle{BlobRef: br, Size: uint32(len(b)), Contents: bytes.NewReader(b)})
		if err != nil {
			fail(fmt.Errorf("kit: restoring %s: %w", br, err))
			return
		}
		atomic.AddInt64(&st.Blobs, 1)
		atomic.AddInt64(&st.Bytes, int64(len(b)))
	}

	refs := make(chan blob.SizedRef, 256)
	var enumErr error
	go func() { enumErr = from.EnumerateBlobs(ctx, refs, "", -1) }()
	parallel(workers, func() {
		for sb := range refs {
			b, err := fetchVerified(ctx, from, sb.Ref)
			if errors.Is(err, errBadHash) {
				mu.Lock()
				st.BadBlobs = append(st.BadBlobs, api.BlobRef(sb.Ref.String()))
				mu.Unlock()
				continue
			}
			if err != nil {
				fail(fmt.Errorf("kit: reading %s from the bucket: %w", sb.Ref, err))
				continue
			}
			if looksLikeSchema(b) {
				mu.Lock()
				schemas = append(schemas, pendingBlob{sb.Ref, b})
				mu.Unlock()
				continue
			}
			upload(sb.Ref, b)
		}
	})
	if firstErr == nil && enumErr != nil {
		firstErr = fmt.Errorf("kit: listing the bucket's blobs: %w", enumErr)
	}
	if firstErr != nil {
		return firstErr
	}
	next := make(chan pendingBlob)
	go func() {
		defer close(next)
		for _, b := range schemas {
			select {
			case next <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	parallel(workers, func() {
		for b := range next {
			upload(b.ref, b.data)
		}
	})
	if firstErr != nil {
		return firstErr
	}
	sort.Slice(st.BadBlobs, func(i, j int) bool { return st.BadBlobs[i] < st.BadBlobs[j] })
	return nil
}

type pendingBlob struct {
	ref  blob.Ref
	data []byte
}

func parallel(n int, f func()) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() { defer wg.Done(); f() }()
	}
	wg.Wait()
}

// looksLikeSchema reports whether b may be a schema blob. A false positive
// only costs ordering.
func looksLikeSchema(b []byte) bool {
	return len(b) > 0 && len(b) <= schema.MaxSchemaBlobSize && b[0] == '{' && bytes.Contains(b, []byte(`"camliType"`))
}

// fetchVerified reads a blob and checks it against its ref.
func fetchVerified(ctx context.Context, f blob.Fetcher, br blob.Ref) ([]byte, error) {
	rc, _, err := f.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	h := br.Hash()
	b, err := io.ReadAll(io.TeeReader(rc, h))
	if err != nil {
		return nil, err
	}
	if !br.HashMatches(h) {
		return nil, fmt.Errorf("%w: %s", errBadHash, br)
	}
	return b, nil
}

var errBadHash = errors.New("blob does not match its hash")

// ownerClient returns a client of the in-process perkeepd with owner
// access. prefix is a storage handler, or "" for the server's blob root
// with search.
func ownerClient(s *pkserver.Server, prefix string) (*client.Client, error) {
	var c *client.Client
	var err error
	if prefix == "" {
		c, err = client.New(client.OptionServer(s.URL()), client.OptionNoExternalConfig())
	} else {
		c, err = client.New(client.OptionServer(s.URL()+prefix), client.OptionNoExternalConfig())
		if err == nil {
			c, err = c.NewPathClient(prefix)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("kit: client: %w", err)
	}
	c.SetHTTPClient(&http.Client{Transport: s.Transport(auth.OpAll)})
	c.Logger = log.New(io.Discard, "", 0)
	return c, nil
}

// waitSynced waits for perkeepd's sync handler at prefix to have nothing
// left to copy.
func waitSynced(ctx context.Context, s *pkserver.Server, prefix string) error {
	hc := &http.Client{Transport: s.Transport(auth.OpAll)}
	for {
		n, err := syncBacklog(ctx, hc, s.URL(), prefix)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kit: %s still has %d blobs to copy: %w", prefix, n, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func syncBacklog(ctx context.Context, hc *http.Client, base, prefix string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", base+"/status/status.json", nil)
	if err != nil {
		return 0, err
	}
	res, err := hc.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kit: perkeepd status: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("kit: perkeepd status: %s", res.Status)
	}
	var st struct {
		Sync map[string]struct {
			BlobsToCopy int `json:"blobsToCopy"`
		} `json:"sync"`
	}
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		return 0, fmt.Errorf("kit: perkeepd status: %w", err)
	}
	q, ok := st.Sync[prefix]
	if !ok {
		return 0, fmt.Errorf("kit: perkeepd status has no %s", prefix)
	}
	return q.BlobsToCopy, nil
}

// ensureEmptyDir refuses to restore over an existing server.
func ensureEmptyDir(dir string) error {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dir, 0o700)
	}
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return fmt.Errorf("kit: %s is not empty; recover only into a fresh directory", dir)
	}
	return nil
}

func storageWith(s zerofs.Storage, c Credentials) zerofs.Storage {
	if c.AccessKeyID != "" {
		s.AccessKeyID = c.AccessKeyID
	}
	if c.SecretAccessKey != "" {
		s.SecretAccessKey = c.SecretAccessKey
	}
	return s
}

func absOr(p, def string) string {
	if p == "" {
		return def
	}
	return filepath.Clean(p)
}
