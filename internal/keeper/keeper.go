package keeper

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Scanner is the part of api.Syncer the keeper uses to tell Syncthing about
// an expired file right away instead of waiting for its watcher.
type Scanner interface {
	Scan(ctx context.Context, folderID, subpath string) error
}

// Config sets up a Keeper for one kept folder on this node.
type Config struct {
	Folder   string // registry folder name
	FolderID string // Syncthing folder ID, for Scanner
	Dir      string // local path of the synced folder
	Self     api.NodeKey
	Policy   api.KeepPolicy

	Registry api.RegistryClient
	Putter   api.BlobPutter
	Clock    api.Clock // defaults to api.SystemClock
	Scanner  Scanner   // optional

	// Exclude, if set, replaces the .stignore/.keepignore matcher. The
	// built-in exclusions (Syncthing temp and state files, .keepsync and the
	// ignore files) always apply.
	Exclude Excluder

	// Interval between passes in Run. Defaults to DefaultInterval.
	Interval time.Duration

	// Logf, if set, receives one line per notable event.
	Logf func(format string, args ...any)
}

// DefaultInterval is how often Run makes a pass.
const DefaultInterval = 5 * time.Minute

// durableBatch caps refs per Durable request.
const durableBatch = 250 // files per Durable request (two refs each)

// Keeper runs the keep loop and expiry for one folder. Every pass is
// idempotent, so an error simply means the next pass tries again.
type Keeper struct {
	cfg Config

	mu     sync.Mutex // serializes passes; guards everything below
	policy api.KeepPolicy
	// dirty holds, per path with an unkept version, the earliest mtime seen
	// for that unkept version. max-defer is measured from it, so a file that
	// is rewritten so often that it never settles is still kept.
	dirty map[string]time.Time
	// putCache remembers what this node uploaded and claimed, so an
	// unchanged file costs a stat, not a hash.
	putCache map[string]cached
}

type cached struct {
	mtime  time.Time
	size   int64
	ref    api.BlobRef
	result api.ClaimResult
}

// New returns a Keeper. The policy must be valid.
func New(cfg Config) (*Keeper, error) {
	if cfg.Registry == nil || cfg.Putter == nil {
		return nil, errors.New("keeper: Registry and Putter are required")
	}
	if cfg.Dir == "" {
		return nil, errors.New("keeper: Dir is required")
	}
	if err := ValidatePolicy(cfg.Policy); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = api.SystemClock{}
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	return &Keeper{
		cfg:      cfg,
		policy:   cfg.Policy,
		dirty:    map[string]time.Time{},
		putCache: map[string]cached{},
	}, nil
}

// SetPolicy replaces the policy, for example when the registry reassigns
// keepers. An invalid policy is rejected and the old one stays.
func (k *Keeper) SetPolicy(p api.KeepPolicy) error {
	if err := ValidatePolicy(p); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if p.Permanode != k.policy.Permanode {
		// A different archive: nothing we remember applies to it.
		k.dirty = map[string]time.Time{}
		k.putCache = map[string]cached{}
	}
	k.policy = p
	return nil
}

// Policy returns the current policy.
func (k *Keeper) Policy() api.KeepPolicy {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.policy
}

// IsKeeper reports whether this node is one of the folder's keepers.
func (k *Keeper) IsKeeper() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.isKeeperLocked()
}

func (k *Keeper) isKeeperLocked() bool {
	for _, n := range k.policy.Keepers {
		if n == k.cfg.Self {
			return true
		}
	}
	return false
}

// Report describes one pass.
type Report struct {
	Skipped bool     // this node is not a keeper for the folder
	Kept    []string // paths claimed this pass (written or already set)
	Forced  []string // the subset of Kept kept by max-defer rather than grace
	InGrace []string // paths with an unkept version still inside grace
	Lagging []string // local copy older than the archive; left for Syncthing
	Expired []string // paths removed from sync this pass
	// ExpiryPaused is set when expiry could not run (server unreachable).
	ExpiryPaused bool
	// Errors holds per-file failures; the pass carried on past them.
	Errors []error
}

// Run makes a pass every Interval until ctx is done. Errors are logged and
// retried on the next pass.
func (k *Keeper) Run(ctx context.Context) error {
	t := time.NewTicker(k.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := k.Pass(ctx); err != nil && ctx.Err() == nil {
			k.logf("keeper %s: pass failed: %v", k.cfg.Folder, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// file is one regular file found by the walk.
type file struct {
	rel   string
	abs   string
	mtime time.Time
	size  int64
}

// Pass walks the folder once: keeps every file past grace (or past
// max-defer), then expires kept, durable files past the expire window. It
// returns an error only when the whole pass could not run; per-file
// failures go in Report.Errors.
func (k *Keeper) Pass(ctx context.Context) (Report, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var rep Report
	if !k.isKeeperLocked() {
		rep.Skipped = true
		return rep, nil
	}
	if err := k.checkMappingLocked(); err != nil {
		return rep, err
	}
	files, err := k.walk()
	if err != nil {
		return rep, err
	}
	tree, err := k.cfg.Registry.Paths(ctx, k.policy.Permanode)
	if err != nil {
		return rep, fmt.Errorf("keeper: fetching kept tree: %w", err)
	}
	now := k.cfg.Clock.Now()
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		seen[f.rel] = true
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		k.keepOne(ctx, f, tree, now, false, &rep)
	}
	// A missing file is never translated into a delete; only forget it.
	for rel := range k.dirty {
		if !seen[rel] {
			delete(k.dirty, rel)
		}
	}
	for rel := range k.putCache {
		if !seen[rel] {
			delete(k.putCache, rel)
		}
	}
	if k.policy.Expire > 0 {
		k.expire(ctx, files, tree, now, &rep)
	}
	return rep, nil
}

// KeepNow keeps the given paths immediately, skipping grace and max-defer.
// The conflict hook uses it to keep both versions before it runs.
func (k *Keeper) KeepNow(ctx context.Context, rels ...string) (Report, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var rep Report
	if err := k.checkMappingLocked(); err != nil {
		return rep, err
	}
	ex, err := k.excluder()
	if err != nil {
		return rep, err
	}
	tree, err := k.cfg.Registry.Paths(ctx, k.policy.Permanode)
	if err != nil {
		return rep, fmt.Errorf("keeper: fetching kept tree: %w", err)
	}
	now := k.cfg.Clock.Now()
	for _, rel := range rels {
		rel = filepath.ToSlash(filepath.Clean(filepath.FromSlash(rel)))
		if rel == "." || !filepath.IsLocal(filepath.FromSlash(rel)) {
			rep.Errors = append(rep.Errors, fmt.Errorf("keeper: path %q is outside the folder", rel))
			continue
		}
		if alwaysExcluded(rel, false) || ex.Excluded(rel, false) {
			rep.Errors = append(rep.Errors, fmt.Errorf("keeper: %s is excluded from keeping", rel))
			continue
		}
		abs := filepath.Join(k.cfg.Dir, filepath.FromSlash(rel))
		fi, err := os.Lstat(abs)
		if err != nil {
			rep.Errors = append(rep.Errors, err)
			continue
		}
		if !fi.Mode().IsRegular() {
			rep.Errors = append(rep.Errors, fmt.Errorf("keeper: %s is not a regular file", rel))
			continue
		}
		k.keepOne(ctx, file{rel: rel, abs: abs, mtime: fi.ModTime(), size: fi.Size()}, tree, now, true, &rep)
	}
	return rep, nil
}

// checkMappingLocked refuses to run when .keepsync names a different
// permanode than the policy, so two archives never get mixed. A missing
// .keepsync is fine: Syncthing may not have delivered it yet.
func (k *Keeper) checkMappingLocked() error {
	m, err := ReadMapping(k.cfg.Dir)
	if errors.Is(err, ErrNoMapping) {
		return nil
	}
	if err != nil {
		return err
	}
	if m.Permanode != k.policy.Permanode {
		return fmt.Errorf("keeper: %s names permanode %s but the registry says %s", MappingFile, m.Permanode, k.policy.Permanode)
	}
	return nil
}

func (k *Keeper) excluder() (Excluder, error) {
	if k.cfg.Exclude != nil {
		return k.cfg.Exclude, nil
	}
	return LoadPatterns(k.cfg.Dir, StIgnoreFile, KeepIgnoreFile)
}

// walk lists the regular files to consider, sorted by path. Symlinks and
// other special files are skipped.
func (k *Keeper) walk() ([]file, error) {
	ex, err := k.excluder()
	if err != nil {
		return nil, err
	}
	canSkip := true
	if p, ok := ex.(*Patterns); ok {
		canSkip = p.canSkipDir()
	}
	var files []file
	err = filepath.WalkDir(k.cfg.Dir, func(abs string, d fs.DirEntry, err error) error {
		if err != nil {
			if abs == k.cfg.Dir {
				return err
			}
			// A file vanishing mid-walk is normal in a synced folder.
			return nil
		}
		if abs == k.cfg.Dir {
			return nil
		}
		rel, err := filepath.Rel(k.cfg.Dir, abs)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if alwaysExcluded(rel, true) || (canSkip && ex.Excluded(rel, true)) {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if alwaysExcluded(rel, false) || ex.Excluded(rel, false) {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		files = append(files, file{rel: rel, abs: abs, mtime: fi.ModTime(), size: fi.Size()})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, err
}

// keptVersion reports whether the archive already holds this exact version
// of f. Matching mtimes are taken as the same version; expiry verifies the
// ref before deleting anything.
func keptVersion(f file, tree map[string]api.PathEntry) bool {
	e, ok := tree[f.rel]
	return ok && e.ModTime.Equal(f.mtime)
}

func (k *Keeper) keepOne(ctx context.Context, f file, tree map[string]api.PathEntry, now time.Time, force bool, rep *Report) {
	if keptVersion(f, tree) {
		delete(k.dirty, f.rel)
		return
	}
	if e, ok := tree[f.rel]; ok && e.ModTime.After(f.mtime) && !force {
		rep.Lagging = append(rep.Lagging, f.rel)
		return
	}
	if c, ok := k.putCache[f.rel]; ok && c.mtime.Equal(f.mtime) && c.size == f.size {
		if c.result == api.ClaimRejectedOlder {
			rep.Lagging = append(rep.Lagging, f.rel)
			return
		}
	}

	forced := false
	if !force {
		first, ok := k.dirty[f.rel]
		if !ok || f.mtime.Before(first) {
			first = f.mtime
			k.dirty[f.rel] = first
		}
		switch {
		case now.Sub(f.mtime) >= k.policy.Grace:
		case now.Sub(first) >= k.policy.MaxDefer:
			forced = true
		default:
			rep.InGrace = append(rep.InGrace, f.rel)
			return
		}
	}

	ref, err := k.cfg.Putter.PutFile(ctx, f.abs)
	if err != nil {
		rep.Errors = append(rep.Errors, fmt.Errorf("keeper: put %s: %w", f.rel, err))
		return
	}
	// If the file changed while uploading, the ref may not match the mtime
	// we would claim. Leave it for the next pass.
	if fi, err := os.Lstat(f.abs); err != nil || !fi.ModTime().Equal(f.mtime) || fi.Size() != f.size {
		return
	}
	res, err := k.cfg.Registry.ClaimPath(ctx, api.PathClaim{
		Permanode: k.policy.Permanode,
		Path:      f.rel,
		File:      ref,
		ModTime:   f.mtime,
	})
	if err != nil {
		rep.Errors = append(rep.Errors, fmt.Errorf("keeper: claim %s: %w", f.rel, err))
		return
	}
	k.putCache[f.rel] = cached{mtime: f.mtime, size: f.size, ref: ref, result: res}
	switch res {
	case api.ClaimWritten, api.ClaimUnchanged:
		// The claim ref is unknown until the next Paths fetch, which keeps
		// a file claimed in this pass from expiring in the same pass.
		if e, ok := tree[f.rel]; !ok || e.File != ref || !e.ModTime.Equal(f.mtime) {
			tree[f.rel] = api.PathEntry{File: ref, ModTime: f.mtime}
		}
		delete(k.dirty, f.rel)
		rep.Kept = append(rep.Kept, f.rel)
		if forced {
			rep.Forced = append(rep.Forced, f.rel)
			k.logf("keeper %s: kept %s at max-defer", k.cfg.Folder, f.rel)
		}
	case api.ClaimRejectedOlder:
		rep.Lagging = append(rep.Lagging, f.rel)
	}
}

// expire removes kept files past the expire window, but only once their
// current version is in the kept tree and the server reports it durable.
// If the server cannot answer, nothing is removed.
func (k *Keeper) expire(ctx context.Context, files []file, tree map[string]api.PathEntry, now time.Time, rep *Report) {
	type cand struct {
		f     file
		ref   api.BlobRef
		claim api.BlobRef
	}
	var cands []cand
	for _, f := range files {
		if now.Sub(f.mtime) < k.policy.Expire {
			continue
		}
		e, ok := tree[f.rel]
		if !ok || !e.ModTime.Equal(f.mtime) || e.Claim == "" {
			continue
		}
		ref, err := k.verifyRef(ctx, f)
		if err != nil {
			rep.Errors = append(rep.Errors, fmt.Errorf("keeper: verify %s: %w", f.rel, err))
			continue
		}
		if ref != e.File {
			// Same mtime, different content: not the kept version.
			continue
		}
		cands = append(cands, cand{f, ref, e.Claim})
	}
	for start := 0; start < len(cands); start += durableBatch {
		end := min(start+durableBatch, len(cands))
		batch := cands[start:end]
		// Both the content and the claim that names it must be durable.
		refs := make([]api.BlobRef, 0, 2*len(batch))
		for _, c := range batch {
			refs = append(refs, c.ref, c.claim)
		}
		ok, err := k.cfg.Registry.Durable(ctx, refs)
		if err == nil && len(ok) != len(refs) {
			err = fmt.Errorf("durable returned %d answers for %d refs", len(ok), len(refs))
		}
		if err != nil {
			rep.ExpiryPaused = true
			k.logf("keeper %s: expiry paused: %v", k.cfg.Folder, err)
			return
		}
		for i, c := range batch {
			if !ok[2*i] || !ok[2*i+1] {
				continue
			}
			if err := k.remove(ctx, c.f); err != nil {
				rep.Errors = append(rep.Errors, fmt.Errorf("keeper: expire %s: %w", c.f.rel, err))
				continue
			}
			rep.Expired = append(rep.Expired, c.f.rel)
		}
	}
}

// verifyRef returns the file's blobref, from the cache when this node
// already put this version, otherwise by putting it again (the server
// already has it, so nothing is re-sent).
func (k *Keeper) verifyRef(ctx context.Context, f file) (api.BlobRef, error) {
	if c, ok := k.putCache[f.rel]; ok && c.mtime.Equal(f.mtime) && c.size == f.size {
		return c.ref, nil
	}
	ref, err := k.cfg.Putter.PutFile(ctx, f.abs)
	if err != nil {
		return "", err
	}
	if fi, err := os.Lstat(f.abs); err != nil || !fi.ModTime().Equal(f.mtime) || fi.Size() != f.size {
		return "", errors.New("file changed while verifying")
	}
	k.putCache[f.rel] = cached{mtime: f.mtime, size: f.size, ref: ref, result: api.ClaimUnchanged}
	return ref, nil
}

// remove deletes the file if it is still the version that was verified.
func (k *Keeper) remove(ctx context.Context, f file) error {
	fi, err := os.Lstat(f.abs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || !fi.ModTime().Equal(f.mtime) || fi.Size() != f.size {
		return errors.New("file changed before removal; skipped")
	}
	if err := os.Remove(f.abs); err != nil {
		return err
	}
	delete(k.putCache, f.rel)
	delete(k.dirty, f.rel)
	k.logf("keeper %s: expired %s", k.cfg.Folder, f.rel)
	if k.cfg.Scanner != nil && k.cfg.FolderID != "" {
		if err := k.cfg.Scanner.Scan(ctx, k.cfg.FolderID, f.rel); err != nil {
			k.logf("keeper %s: scan after expiring %s: %v", k.cfg.Folder, f.rel, err)
		}
	}
	return nil
}

func (k *Keeper) logf(format string, args ...any) {
	if k.cfg.Logf != nil {
		k.cfg.Logf(format, args...)
	}
}
