package kit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/search"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/zerofs"
)

// Layout is where a recovered server's files go. The CLI fills it from the
// server's config paths.
type Layout struct {
	// PerkeepDir is pkserver.Config.Dir. It must be missing or empty.
	PerkeepDir string
	// TailcatKey is the server's saved tailcat key path. It must not exist.
	TailcatKey string
	// ZeroFSDataDir is zerofs.Config.DataDir. The kit's zerofs binary is
	// installed at zerofs.LocalPath(ZeroFSDataDir) if none is there.
	ZeroFSDataDir string
	// ZeroFSMountDir is zerofs.Config.MountDir. Default ZeroFSDataDir/mnt.
	ZeroFSMountDir string
	// ConfigFile, if set and the kit holds one, receives the server's
	// config file. It must not exist.
	ConfigFile string
	// RegistryFile, if set and the kit holds one, receives the registry
	// state. It must not exist.
	RegistryFile string
}

// RecoverOptions configures Recover.
type RecoverOptions struct {
	Layout Layout
	// Credentials are write credentials for the bucket. A recovered
	// server writes to it, so a kit holding a read-only token needs them.
	Credentials Credentials
	Mount       zerofs.MountMode
	NewZeroFS   NewZeroFS
	Logf        func(string, ...any)
}

// Recover rebuilds the server from the bucket: it mounts ZeroFS on the
// kit's bucket, starts perkeepd on a fresh local storage and index, copies
// and reindexes every blob from the bucket's replica, then stops and puts
// the server's tailcat key and config in place. The server then comes back
// up at the same address with `keepsync server run`, in a new process,
// and nodes reconnect on their own.
//
// Never run it while the old server may still be running against the
// bucket: ZeroFS is single-writer.
func Recover(ctx context.Context, k *Kit, o RecoverOptions) (*RestoreStats, error) {
	l := o.Layout
	if l.PerkeepDir == "" || l.TailcatKey == "" || l.ZeroFSDataDir == "" {
		return nil, errors.New("kit: Layout.PerkeepDir, TailcatKey and ZeroFSDataDir are required")
	}
	for _, p := range []string{l.TailcatKey, l.ConfigFile, l.RegistryFile} {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			return nil, fmt.Errorf("kit: %s already exists; recover only onto a fresh host or directory", p)
		}
	}
	e, err := restore(ctx, k, restoreParams{
		storage:    storageWith(k.Manifest.Storage, o.Credentials),
		perkeepDir: l.PerkeepDir,
		zfsDataDir: l.ZeroFSDataDir,
		mountDir:   l.ZeroFSMountDir,
		mount:      o.Mount,
		newZeroFS:  o.NewZeroFS,
		logf:       o.Logf,
	})
	if err != nil {
		return nil, err
	}
	stats := e.stats
	registry := e.registry
	if n := len(stats.BadBlobs); n > 0 {
		e.close(ctx)
		return &stats, fmt.Errorf("kit: %d blobs in the bucket are damaged (first %s); nothing was installed", n, stats.BadBlobs[0])
	}
	// Nothing new should have reached the replica, but make sure of it
	// before stopping, as a clean server stop would.
	err = errors.Join(e.srv.Drain(ctx), e.srv.Flush(ctx))
	if cerr := e.close(ctx); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if err := k.Extract(entryTailcatKey, l.TailcatKey, 0o600); err != nil {
		return nil, err
	}
	if l.ConfigFile != "" && k.Has(entryConfig) {
		if err := k.Extract(entryConfig, l.ConfigFile, 0o600); err != nil {
			return nil, err
		}
	}
	if l.RegistryFile != "" {
		switch {
		case registry != nil:
			if err := writeNew(l.RegistryFile, registry); err != nil {
				return nil, err
			}
			stats.Registry = "bucket"
		case k.Has(entryRegistry):
			if err := k.Extract(entryRegistry, l.RegistryFile, 0o600); err != nil {
				return nil, err
			}
			stats.Registry = "kit"
		}
	}
	return &stats, nil
}

// DrillOptions configures Drill.
type DrillOptions struct {
	// Bucket is the URL of a copy of the bucket, such as
	// "s3://my-bucket-drill/keepsync". It must differ from the kit's
	// bucket: the drill never mounts the live bucket.
	Bucket      string
	Credentials Credentials
	// Baseline, if set, is the live server's Snapshot, taken after a
	// flush and before the bucket was copied. Without it the drill checks
	// only that the restored server is self-consistent.
	Baseline *Baseline
	// WorkDir holds the drill's ZeroFS cache and perkeepd. Default a new
	// temp dir, removed afterwards.
	WorkDir string
	// Samples is how many kept paths the index check searches for.
	// Default 5.
	Samples   int
	Mount     zerofs.MountMode
	NewZeroFS NewZeroFS
	Logf      func(string, ...any)
}

// Check is one drill check's outcome.
type Check struct {
	Name   string
	OK     bool
	Detail string
}

// DrillReport is the outcome of a drill.
type DrillReport struct {
	Bucket string
	Stats  RestoreStats
	Checks []Check
	// Restored counts what the restored server holds.
	Blobs, Permanodes, Files int
}

// OK reports whether every check passed.
func (r *DrillReport) OK() bool {
	for _, c := range r.Checks {
		if !c.OK {
			return false
		}
	}
	return len(r.Checks) > 0
}

func (r *DrillReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Recovery drill against %s\n", r.Bucket)
	for _, c := range r.Checks {
		mark := "ok  "
		if !c.OK {
			mark = "FAIL"
		}
		fmt.Fprintf(&b, "  %s %-12s %s\n", mark, c.Name, c.Detail)
	}
	fmt.Fprintf(&b, "  restored %d blobs (%d bytes), %d permanodes, %d kept files\n",
		r.Blobs, r.Stats.Bytes, r.Permanodes, r.Files)
	fmt.Fprintf(&b, "  restore time %s (zerofs up %s, copy and reindex %s)\n",
		r.Stats.Total.Round(time.Millisecond), r.Stats.ZeroFSStart.Round(time.Millisecond), r.Stats.Copy.Round(time.Millisecond))
	if r.OK() {
		b.WriteString("PASS\n")
	} else {
		b.WriteString("FAIL\n")
	}
	return b.String()
}

// Drill runs the recovery path against a copy of the bucket, without
// taking over the server's identity (no tailcat key is installed and
// nothing listens), and checks the result:
//
//  1. Blob check: every restored blob reads back and matches its hash, and
//     every baseline blob was restored.
//  2. File check: every kept file reads back from the restored server, with
//     the baseline's SHA-256.
//  3. Index check: the index finds every permanode, and finds sample kept
//     paths by search.
//  4. Timing: restore time, as a check that always passes.
//
// The report is returned with a nil error even when checks fail; err is
// for a drill that could not run.
func Drill(ctx context.Context, k *Kit, o DrillOptions) (*DrillReport, error) {
	if o.Bucket == "" {
		return nil, errors.New("kit: the drill needs --bucket, a copy of the bucket")
	}
	if sameBucket(o.Bucket, k.Manifest.Storage.URL) {
		return nil, fmt.Errorf("kit: %s is the live bucket; the drill must run against a copy (ZeroFS is single-writer)", o.Bucket)
	}
	if o.Samples <= 0 {
		o.Samples = 5
	}
	work := o.WorkDir
	if work == "" {
		d, err := os.MkdirTemp("", "keepsync-drill-")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(d)
		work = d
	}
	st := storageWith(k.Manifest.Storage, o.Credentials)
	st.URL = o.Bucket
	e, err := restore(ctx, k, restoreParams{
		storage:    st,
		perkeepDir: filepath.Join(work, "perkeep"),
		zfsDataDir: filepath.Join(work, "zerofs"),
		mount:      o.Mount,
		newZeroFS:  o.NewZeroFS,
		logf:       o.Logf,
	})
	if err != nil {
		return nil, err
	}
	defer e.close(context.WithoutCancel(ctx))

	rep := &DrillReport{Bucket: o.Bucket, Stats: e.stats}
	s, err := scanServer(ctx, e.srv, e.owner, 8)
	if err != nil {
		return nil, err
	}
	rep.Blobs, rep.Permanodes, rep.Files = len(s.Blobs), len(s.Permanodes), len(s.Files)
	s.BadBlobs = append(s.BadBlobs, e.stats.BadBlobs...)
	rep.Checks = append(rep.Checks, blobCheck(s, o.Baseline), fileCheck(s, o.Baseline))
	rep.Checks = append(rep.Checks, indexCheck(ctx, e, s, o.Baseline, o.Samples))
	rep.Checks = append(rep.Checks, registryCheck(k, e.registry))
	rep.Checks = append(rep.Checks, Check{Name: "timing", OK: true, Detail: fmt.Sprintf(
		"restore %s, %d blobs at %.1f MB/s; bucket operation counts not measured (ZeroFS does not report them)",
		e.stats.Total.Round(time.Millisecond), e.stats.Blobs, mbps(e.stats.Bytes, e.stats.Copy))})
	return rep, nil
}

func blobCheck(s *scan, base *Baseline) Check {
	c := Check{Name: "blobs", OK: true}
	var problems []string
	if len(s.BadBlobs) > 0 {
		problems = append(problems, fmt.Sprintf("%d fail their hash check (first %s)", len(s.BadBlobs), s.BadBlobs[0]))
	}
	extra := 0
	if base != nil {
		got := make(map[api.BlobRef]uint32, len(s.Blobs))
		for _, b := range s.Blobs {
			got[b.Ref] = b.Size
		}
		var missing, wrongSize []api.BlobRef
		for _, b := range base.Blobs {
			sz, ok := got[b.Ref]
			switch {
			case !ok:
				missing = append(missing, b.Ref)
			case sz != b.Size:
				wrongSize = append(wrongSize, b.Ref)
			}
			delete(got, b.Ref)
		}
		extra = len(got)
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("%d of %d live blobs missing (first %s)", len(missing), len(base.Blobs), missing[0]))
		}
		if len(wrongSize) > 0 {
			problems = append(problems, fmt.Sprintf("%d with the wrong size (first %s)", len(wrongSize), wrongSize[0]))
		}
	}
	if len(problems) > 0 {
		c.OK, c.Detail = false, strings.Join(problems, "; ")
		return c
	}
	c.Detail = fmt.Sprintf("%d blobs read and hash-verified", len(s.Blobs))
	if base != nil {
		c.Detail += fmt.Sprintf(", all %d live blobs present", len(base.Blobs))
		if extra > 0 {
			c.Detail += fmt.Sprintf(" (%d newer than the baseline)", extra)
		}
	} else {
		c.Detail += " (no baseline to compare against)"
	}
	return c
}

func fileCheck(s *scan, base *Baseline) Check {
	c := Check{Name: "files", OK: true}
	var problems []string
	if len(s.Unread) > 0 {
		problems = append(problems, fmt.Sprintf("%d kept files unreadable (first %s)", len(s.Unread), s.Unread[0]))
	}
	if base != nil {
		var missing, differ []string
		for k, want := range base.Files {
			got, ok := s.Files[k]
			switch {
			case !ok:
				missing = append(missing, k)
			case got.SHA256 != want.SHA256 || got.Size != want.Size:
				differ = append(differ, k)
			}
		}
		sort.Strings(missing)
		sort.Strings(differ)
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("%d of %d kept files missing (first %s)", len(missing), len(base.Files), missing[0]))
		}
		if len(differ) > 0 {
			problems = append(problems, fmt.Sprintf("%d differ from the live server (first %s)", len(differ), differ[0]))
		}
	}
	if len(problems) > 0 {
		c.OK, c.Detail = false, strings.Join(problems, "; ")
		return c
	}
	c.Detail = fmt.Sprintf("%d kept files read back", len(s.Files))
	if base != nil {
		c.Detail += fmt.Sprintf(", all %d match the live server's sha256", len(base.Files))
	}
	return c
}

func indexCheck(ctx context.Context, e *restoreEnv, s *scan, base *Baseline, samples int) Check {
	c := Check{Name: "index", OK: true}
	var problems []string
	res, err := e.owner.Query(ctx, &search.SearchQuery{
		Constraint: &search.Constraint{CamliType: schema.TypePermanode},
		Limit:      -1,
	})
	if err != nil {
		return Check{Name: "index", Detail: fmt.Sprintf("permanode search: %v", err)}
	}
	if n := len(res.Blobs); n != len(s.Permanodes) {
		problems = append(problems, fmt.Sprintf("index finds %d permanodes, storage holds %d", n, len(s.Permanodes)))
	}
	if base != nil && len(s.Permanodes) < len(base.Permanodes) {
		problems = append(problems, fmt.Sprintf("%d permanodes restored, live server has %d", len(s.Permanodes), len(base.Permanodes)))
	}
	keys := make([]string, 0, len(s.Files))
	for k := range s.Files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	found := 0
	tried := pick(keys, samples)
	for _, k := range tried {
		pn, path, _ := strings.Cut(k, "/")
		r, err := e.owner.Query(ctx, &search.SearchQuery{
			Constraint: &search.Constraint{Permanode: &search.PermanodeConstraint{
				Attr:  "camliPath:" + path,
				Value: string(s.Files[k].File),
			}},
		})
		if err != nil {
			problems = append(problems, fmt.Sprintf("search for %s: %v", path, err))
			continue
		}
		hit := false
		for _, b := range r.Blobs {
			hit = hit || b.Blob.String() == pn
		}
		if !hit {
			problems = append(problems, fmt.Sprintf("search for %s does not find its permanode", path))
			continue
		}
		found++
	}
	if len(problems) > 0 {
		c.OK, c.Detail = false, strings.Join(problems, "; ")
		return c
	}
	c.Detail = fmt.Sprintf("%d permanodes indexed, %d of %d sample path searches found", len(s.Permanodes), found, len(tried))
	return c
}

// registryCheck reports where recovery would get the registry from. A
// registry only in the kit, or nowhere, is a warning: nodes approved after
// the kit was made would have to be approved again.
func registryCheck(k *Kit, fromBucket []byte) Check {
	c := Check{Name: "registry", OK: true}
	switch {
	case fromBucket != nil:
		c.Detail = fmt.Sprintf("current registry in the bucket (%d bytes)", len(fromBucket))
	case k.Has(entryRegistry):
		c.Detail = "warning: no registry in the bucket; recovery would use the kit's copy from " + k.Manifest.Created.Format("2006-01-02")
	default:
		c.Detail = "warning: no registry in the bucket or the kit; every node would need approving again"
	}
	return c
}

func writeNew(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
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
	return f.Close()
}

// pick returns up to n evenly spaced elements of sorted keys.
func pick(keys []string, n int) []string {
	if len(keys) <= n {
		return keys
	}
	out := make([]string, 0, n)
	for i := range n {
		out = append(out, keys[i*len(keys)/n])
	}
	return out
}

func sameBucket(a, b string) bool {
	norm := func(s string) string { return strings.TrimRight(strings.TrimSpace(s), "/") }
	return norm(a) == norm(b)
}

func mbps(n int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / 1e6 / d.Seconds()
}
