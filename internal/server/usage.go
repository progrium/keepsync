package server

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/progrium/keepsync/internal/config"
)

// DiskUsage is how much space the server uses, locally and in the bucket.
// Local sizes are measured by walking the directories, at most every
// localUsageEvery; the bucket by listing it, at most every bucketUsageEvery.
type DiskUsage struct {
	Mode string `json:"mode"`
	// BlobsBytes is the local Perkeep blob store (store mode only).
	BlobsBytes int64 `json:"blobsBytes"`
	// IndexBytes is the Perkeep index, which is local in both modes.
	IndexBytes int64 `json:"indexBytes"`
	// CacheBytes is ZeroFS's cache on disk, and CacheLimitBytes the most
	// it grows to: the configured size, raised to ZeroFS's floors.
	CacheBytes      int64 `json:"cacheBytes"`
	CacheLimitBytes int64 `json:"cacheLimitBytes"`
	// TotalBytes is everything in the server directory (not the mount).
	TotalBytes int64 `json:"totalBytes"`
	// VolumeFree and VolumeSize are for the volume the server dir is on.
	VolumeFree uint64    `json:"volumeFree"`
	VolumeSize uint64    `json:"volumeSize"`
	MeasuredAt time.Time `json:"measuredAt,omitzero"`
	Err        string    `json:"err,omitempty"`

	Bucket BucketUsage `json:"bucket"`
}

// BucketUsage is the size of everything under the bucket prefix.
type BucketUsage struct {
	Bytes      int64     `json:"bytes"`
	Objects    int64     `json:"objects"`
	MeasuredAt time.Time `json:"measuredAt,omitzero"`
	Err        string    `json:"err,omitempty"`
}

const (
	localUsageEvery  = time.Minute
	bucketUsageEvery = 10 * time.Minute
)

// usageTracker keeps the latest DiskUsage, refreshed in the background.
type usageTracker struct {
	cfg      *config.Server
	mountDir string // never walked: it is the bucket
	hc       *http.Client

	mu sync.Mutex
	u  DiskUsage
}

func newUsageTracker(cfg *config.Server, mountDir string) *usageTracker {
	t := &usageTracker{cfg: cfg, mountDir: mountDir, hc: &http.Client{Timeout: 2 * time.Minute}}
	t.u.Mode = cfg.StorageMode()
	t.u.CacheLimitBytes = zerofsCacheLimit(cfg.CacheSizeGB())
	return t
}

// run refreshes local usage every localUsageEvery and bucket usage every
// bucketUsageEvery until ctx is done.
func (t *usageTracker) run(ctx context.Context) {
	t.refreshLocal()
	t.refreshBucket(ctx)
	local := time.NewTicker(localUsageEvery)
	bucket := time.NewTicker(bucketUsageEvery)
	defer local.Stop()
	defer bucket.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-local.C:
			t.refreshLocal()
		case <-bucket.C:
			t.refreshBucket(ctx)
		}
	}
}

func (t *usageTracker) get() DiskUsage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.u
}

func (t *usageTracker) refreshLocal() {
	pk := t.cfg.PerkeepDir()
	var errs []error
	size := func(p string) int64 {
		n, err := dirSize(p, t.mountDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
		return n
	}
	var u DiskUsage
	if t.cfg.StorageMode() == config.ModeStore {
		u.BlobsBytes = size(filepath.Join(pk, "blobs"))
	}
	u.IndexBytes = size(filepath.Join(pk, "index.leveldb"))
	u.CacheBytes = size(filepath.Join(t.cfg.ZeroFSDir(), "cache"))
	u.TotalBytes = size(t.cfg.Dir)
	var err error
	if u.VolumeFree, u.VolumeSize, err = volumeSpace(t.cfg.Dir); err != nil {
		errs = append(errs, err)
	}
	u.MeasuredAt = time.Now()
	if err := errors.Join(errs...); err != nil {
		u.Err = err.Error()
	}
	t.mu.Lock()
	u.Mode, u.CacheLimitBytes, u.Bucket = t.u.Mode, t.u.CacheLimitBytes, t.u.Bucket
	t.u = u
	t.mu.Unlock()
}

func (t *usageTracker) refreshBucket(ctx context.Context) {
	b := BucketUsage{MeasuredAt: time.Now()}
	var err error
	b.Bytes, b.Objects, err = bucketSize(ctx, t.hc, t.cfg.Storage)
	if err != nil {
		b.Err = err.Error()
	}
	t.mu.Lock()
	t.u.Bucket = b
	t.mu.Unlock()
}

// zerofsCacheLimit is the disk cache ZeroFS 2.3.7 actually uses for a
// configured size: a tenth for metadata, the rest for data, each at least
// 1 GiB and metadata at most 16 GiB.
func zerofsCacheLimit(gb float64) int64 {
	const gib = 1 << 30
	total := int64(gb * 1e9)
	meta := min(max(total/10, gib), 16*gib)
	return meta + max(total-meta, gib)
}

// dirSize sums the sizes of the regular files under root, skipping skip
// (the ZeroFS mount) and anything on another device.
func dirSize(root, skip string) (int64, error) {
	rootDev, ok := devOf(root)
	var n int64
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == root {
				return err
			}
			return nil // vanished while walking, or unreadable
		}
		if d.IsDir() {
			if p == skip {
				return filepath.SkipDir
			}
			if ok {
				if dev, ok2 := devOf(p); ok2 && dev != rootDev {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if d.Type().IsRegular() {
			if fi, err := d.Info(); err == nil {
				n += diskBytes(fi)
			}
		}
		return nil
	})
	return n, err
}

// bucketSize sums object sizes under the storage URL's prefix. For s3:// it
// lists the bucket (one request per 1000 objects); for file:// it walks the
// directory.
func bucketSize(ctx context.Context, hc *http.Client, st config.Storage) (bytes, objects int64, err error) {
	u, err := url.Parse(st.URL)
	if err != nil {
		return 0, 0, err
	}
	switch u.Scheme {
	case "file":
		err := filepath.WalkDir(u.Path, func(p string, d fs.DirEntry, err error) error {
			if err == nil && d.Type().IsRegular() {
				if fi, err := d.Info(); err == nil {
					bytes += fi.Size()
					objects++
				}
			}
			return err
		})
		return bytes, objects, err
	case "s3":
	default:
		return 0, 0, fmt.Errorf("bucket usage: %s:// is not supported", u.Scheme)
	}

	bucket, prefix := u.Host, strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	region := st.Region
	if region == "" || region == "auto" && st.Endpoint == "" {
		region = "us-east-1"
	}
	endpoint := strings.TrimRight(st.Endpoint, "/")
	if endpoint == "" {
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	creds := aws.Credentials{AccessKeyID: st.AccessKeyID, SecretAccessKey: st.SecretAccessKey}
	const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/"+bucket+"?"+q.Encode(), nil)
		if err != nil {
			return 0, 0, err
		}
		req.Header.Set("X-Amz-Content-Sha256", emptySHA)
		if creds.AccessKeyID != "" {
			if err := v4.NewSigner().SignHTTP(ctx, creds, req, emptySHA, "s3", region, time.Now()); err != nil {
				return 0, 0, err
			}
		}
		resp, err := hc.Do(req)
		if err != nil {
			return 0, 0, fmt.Errorf("bucket usage: %w", err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if err != nil {
			return 0, 0, err
		}
		if resp.StatusCode != http.StatusOK {
			return 0, 0, fmt.Errorf("bucket usage: list %s: %s", bucket, resp.Status)
		}
		var res struct {
			Contents []struct {
				Size int64 `xml:"Size"`
			} `xml:"Contents"`
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		if err := xml.Unmarshal(body, &res); err != nil {
			return 0, 0, fmt.Errorf("bucket usage: %w", err)
		}
		for _, c := range res.Contents {
			bytes += c.Size
			objects++
		}
		if !res.IsTruncated || res.NextContinuationToken == "" {
			return bytes, objects, nil
		}
		token = res.NextContinuationToken
	}
}
