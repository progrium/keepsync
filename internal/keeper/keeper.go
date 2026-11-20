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

	"golang.org/x/text/unicode/norm"

	"github.com/progrium/keepsync/internal/api"
)

// Scanner is the part of api.Syncer the keeper uses to tell Syncthing about
// an expired file right away instead of waiting for its watcher. Superseded
// by Config.Syncer; kept so callers that only scan still work.
type Scanner interface {
	Scan(ctx context.Context, folderID, subpath string) error
}

// Config sets up a Keeper for one kept folder on this node.
type Config struct {
	Folder   string // registry folder name
	FolderID string // Syncthing folder ID, for Syncer
	Dir      string // local path of the synced folder
	Self     api.NodeKey
	Policy   api.KeepPolicy

	Registry api.RegistryClient
	Putter   api.BlobPutter
	Clock    api.Clock // defaults to api.SystemClock
	Scanner  Scanner   // optional; Syncer takes its place when set

	// Syncer is the node's Syncthing. With it the keeper claims only the
	// version Syncthing calls current, sends its version vector with each
	// claim, and records files deleted in sync as gone. Without it the
	// keeper falls back to mtimes and never marks deletes.
	Syncer api.Syncer

	// Exclude, if set, replaces the .stignore/.keepignore matcher. The
	// built-in exclusions (Syncthing temp and state files, .keepsync, and the
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
const durableBatch = 100 // files per Durable request (a few refs each)

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
	// unmarked holds files this node expired whose expired mark has not
	// reached the server yet, so a retry still says expired.
	unmarked map[string]api.BlobRef
	// unconfirmed holds files this node kept that the server has not yet
	// reported durable (see confirm).
	unconfirmed map[string]*keptFile
}

// keptFile is a version this node kept, waiting to be reported durable.
type keptFile struct {
	mtime time.Time
	size  int64
	ref   api.BlobRef
	kept  time.Time     // when it was kept
	next  time.Time     // earliest time to put it again
	wait  time.Duration // backoff before the put after next
}

// maxReputWait caps the backoff between puts of a file that stays not
// durable, as during a long server or bucket outage.
const maxReputWait = time.Hour

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
	if cfg.Syncer != nil {
		cfg.Scanner = cfg.Syncer
	}
	return &Keeper{
		cfg:      cfg,
		policy:   cfg.Policy,
		dirty:    map[string]time.Time{},
		putCache: map[string]cached{},
		unmarked: map[string]api.BlobRef{},

		unconfirmed: map[string]*keptFile{},
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
		k.unmarked = map[string]api.BlobRef{}
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
	Lagging []string // not the current version (per Syncthing or the archive); left for Syncthing
	Expired []string // paths removed from sync this pass
	// Reput holds kept paths put again this pass because the server
	// still did not report them durable (see Keeper.confirm).
	Reput []string
	// Gone holds the paths marked gone in the kept tree this pass, with why.
	Gone map[string]api.GoneReason
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
	mode  os.FileMode // permission bits
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
	k.confirm(ctx, files, tree, now, &rep)
	if k.policy.Expire > 0 {
		k.expire(ctx, files, tree, now, &rep)
	}
	k.markDeleted(ctx, tree, seen, now, &rep)
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
		rel = archivePath(rel)
		fi, err := os.Lstat(abs)
		if err != nil {
			rep.Errors = append(rep.Errors, err)
			continue
		}
		if !fi.Mode().IsRegular() {
			rep.Errors = append(rep.Errors, fmt.Errorf("keeper: %s is not a regular file", rel))
			continue
		}
		k.keepOne(ctx, file{rel: rel, abs: abs, mtime: fi.ModTime(), size: fi.Size(), mode: fi.Mode().Perm()}, tree, now, true, &rep)
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

// archivePath is rel as it is kept: NFC-normalized, so the NFD names
// Syncthing writes on macOS and the NFC names other systems use are one
// kept file (issue #10). The server normalizes too.
func archivePath(rel string) string { return norm.NFC.String(rel) }

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
		// Kept paths are NFC (see archivePath); abs stays as on disk.
		files = append(files, file{rel: archivePath(rel), abs: abs, mtime: fi.ModTime(), size: fi.Size(), mode: fi.Mode().Perm()})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files, err
}

func (k *Keeper) keepOne(ctx context.Context, f file, tree map[string]api.PathEntry, now time.Time, force bool, rep *Report) {
	st := k.fileState(f.rel)
	if st.notCurrent() {
		// A node that is behind must not claim its stale copy.
		rep.Lagging = append(rep.Lagging, f.rel)
		return
	}
	if keptVersion(f, st, tree) {
		delete(k.dirty, f.rel)
		if tree[f.rel].Gone == api.GoneNone {
			return
		}
		// Back on disk after being marked gone (a restore): the content is
		// already archived, so claim it again now, skipping grace, to tell
		// the server it is back. The server clears the mark.
		force = true
	}
	if laggingVersion(f, st, tree) && !force {
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
		first := k.firstSeen(f, now)
		switch {
		case now.Sub(graceBase(f, first, now)) >= k.policy.Grace:
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
		Version:   st.Version,
		Mode:      f.mode,
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
			tree[f.rel] = api.PathEntry{File: ref, ModTime: f.mtime, Version: st.Version}
		}
		delete(k.dirty, f.rel)
		k.unconfirmed[f.rel] = &keptFile{mtime: f.mtime, size: f.size, ref: ref, kept: now, next: now, wait: k.cfg.Interval}
		rep.Kept = append(rep.Kept, f.rel)
		if forced {
			rep.Forced = append(rep.Forced, f.rel)
			k.logf("keeper %s: kept %s at max-defer", k.cfg.Folder, f.rel)
		}
	case api.ClaimRejectedOlder:
		rep.Lagging = append(rep.Lagging, f.rel)
	}
}

// confirm asks the server, on the passes after a file was kept, whether
// it is durable yet, and puts it again while it is not. A server whose
// ZeroFS died before flushing loses what it had not flushed, content
// included, and only the nodes still have it; putting the file again
// uploads whatever the server lacks (and nothing else). The first check
// waits a pass, giving the server's flush time to run; after that the
// file is put again at most once per wait, doubling up to maxReputWait,
// so an outage doesn't rehash every recent file on every pass. A file
// that changed or went away is dropped: its new version, if any, is
// kept and confirmed on its own.
func (k *Keeper) confirm(ctx context.Context, files []file, tree map[string]api.PathEntry, now time.Time, rep *Report) {
	if len(k.unconfirmed) == 0 {
		return
	}
	cur := make(map[string]file, len(files))
	for _, f := range files {
		cur[f.rel] = f
	}
	var rels []string
	for rel, c := range k.unconfirmed {
		f, ok := cur[rel]
		e, inTree := tree[rel]
		if !ok || !f.mtime.Equal(c.mtime) || f.size != c.size || (inTree && e.File != c.ref) {
			delete(k.unconfirmed, rel)
			continue
		}
		if !c.kept.Before(now) || !inTree || len(e.Claims) == 0 {
			continue // kept this pass, or its claims aren't listed yet
		}
		rels = append(rels, rel)
	}
	sort.Strings(rels) // stable order for reports and logs
	groups := make([][]api.BlobRef, len(rels))
	for i, rel := range rels {
		groups[i] = recoverRefs(tree[rel])
	}
	for start := 0; start < len(rels); start += durableBatch {
		end := min(start+durableBatch, len(rels))
		ok, err := k.allDurable(ctx, groups[start:end])
		if err != nil {
			return // server unreachable: try again next pass
		}
		for i, rel := range rels[start:end] {
			c := k.unconfirmed[rel]
			if ok[i] {
				delete(k.unconfirmed, rel)
				continue
			}
			if now.Before(c.next) {
				continue
			}
			c.next = now.Add(c.wait)
			c.wait = min(2*c.wait, maxReputWait)
			f := cur[rel]
			ref, err := k.cfg.Putter.PutFile(ctx, f.abs)
			if err != nil {
				rep.Errors = append(rep.Errors, fmt.Errorf("keeper: put %s again: %w", rel, err))
				continue
			}
			if ref != c.ref {
				delete(k.unconfirmed, rel) // changed in place; the next pass keeps it
				continue
			}
			rep.Reput = append(rep.Reput, rel)
		}
	}
}

// firstSeen records and returns when the unkept version of f started:
// its mtime, or now if that is in the future, or an earlier time already
// recorded for the path. max-defer is measured from it.
func (k *Keeper) firstSeen(f file, now time.Time) time.Time {
	first, ok := k.dirty[f.rel]
	switch {
	case !ok:
		first = f.mtime
		if first.After(now) {
			first = now // a future mtime counts from when it was first seen
		}
		k.dirty[f.rel] = first
	case f.mtime.Before(first):
		first = f.mtime
		k.dirty[f.rel] = first
	}
	return first
}

// graceBase is the time grace is measured from: the mtime, or, for an
// mtime in the future (a bad clock, bogus dates in an archive), when the
// keeper first saw the file. Otherwise such a file would wait in grace
// until the clock reached its mtime (docs/design.md, Edge cases).
func graceBase(f file, first, now time.Time) time.Time {
	if f.mtime.After(now) {
		return first
	}
	return f.mtime
}

// expire removes kept files past the expire window, but only once their
// current version is in the kept tree and the server reports it durable.
// If the server cannot answer, nothing is removed.
func (k *Keeper) expire(ctx context.Context, files []file, tree map[string]api.PathEntry, now time.Time, rep *Report) {
	type cand struct {
		f    file
		file api.BlobRef
		refs []api.BlobRef
	}
	var cands []cand
	for _, f := range files {
		if now.Sub(f.mtime) < k.policy.Expire {
			continue
		}
		e, ok := tree[f.rel]
		if !ok || len(e.Claims) == 0 {
			continue
		}
		st := k.fileState(f.rel)
		if st.notCurrent() || !keptVersion(f, st, tree) {
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
		cands = append(cands, cand{f, e.File, recoverRefs(e)})
	}
	for start := 0; start < len(cands); start += durableBatch {
		end := min(start+durableBatch, len(cands))
		batch := cands[start:end]
		// The content and every claim and permanode naming it must be durable.
		groups := make([][]api.BlobRef, len(batch))
		for i, c := range batch {
			groups[i] = c.refs
		}
		ok, err := k.allDurable(ctx, groups)
		if err != nil {
			rep.ExpiryPaused = true
			k.logf("keeper %s: expiry paused: %v", k.cfg.Folder, err)
			return
		}
		for i, c := range batch {
			if !ok[i] {
				continue
			}
			if err := k.remove(ctx, c.f); err != nil {
				rep.Errors = append(rep.Errors, fmt.Errorf("keeper: expire %s: %w", c.f.rel, err))
				continue
			}
			rep.Expired = append(rep.Expired, c.f.rel)
			if !k.markGone(ctx, c.f.rel, c.file, api.GoneExpired, rep) {
				k.unmarked[c.f.rel] = c.file
			}
		}
	}
}

// markGone records in the kept tree why path left sync.
func (k *Keeper) markGone(ctx context.Context, rel string, ref api.BlobRef, why api.GoneReason, rep *Report) bool {
	err := k.cfg.Registry.MarkGone(ctx, api.GoneClaim{
		Permanode: k.policy.Permanode,
		Path:      rel,
		Reason:    why,
		File:      ref,
	})
	if err != nil {
		rep.Errors = append(rep.Errors, fmt.Errorf("keeper: mark %s %s: %w", rel, why, err))
		return false
	}
	if rep.Gone == nil {
		rep.Gone = map[string]api.GoneReason{}
	}
	rep.Gone[rel] = why
	return true
}

// markDeleted marks kept paths that have left sync. A path counts only when
// it is missing on disk and Syncthing's current version is a delete, so a
// node that is behind, or has no Syncer, marks nothing.
//
// The reason is expired only when a keeper could have expired the file:
// this node did, or the kept version is past the expire window and the
// server reports it durable, which expiry requires. A file deleted by hand
// past the window but before its flush is a delete, so a plain restore does
// not bring it back. If durability is unknown, those paths wait a pass.
func (k *Keeper) markDeleted(ctx context.Context, tree map[string]api.PathEntry, seen map[string]bool, now time.Time, rep *Report) {
	for rel, ref := range k.unmarked {
		e, ok := tree[rel]
		if seen[rel] || !ok || e.File != ref || e.Gone != api.GoneNone {
			delete(k.unmarked, rel)
			continue
		}
		if k.markGone(ctx, rel, ref, api.GoneExpired, rep) {
			delete(k.unmarked, rel)
		}
	}
	if k.cfg.Syncer == nil {
		return
	}
	rels := make([]string, 0)
	for rel, e := range tree {
		if !seen[rel] && e.Gone == api.GoneNone && e.File != "" && k.unmarked[rel] == "" {
			rels = append(rels, rel)
		}
	}
	sort.Strings(rels)
	var deleted, pastWindow []string
	for _, rel := range rels {
		if ctx.Err() != nil {
			return
		}
		// Not walked can also mean excluded; only a missing file is gone.
		if _, err := os.Lstat(filepath.Join(k.cfg.Dir, filepath.FromSlash(rel))); !errors.Is(err, os.ErrNotExist) {
			continue
		}
		st := k.fileState(rel)
		if !st.ok || !st.Known || !st.Current || !st.Deleted {
			continue
		}
		e := tree[rel]
		if k.policy.Expire > 0 && now.Sub(e.ModTime) >= k.policy.Expire && len(e.Claims) > 0 {
			pastWindow = append(pastWindow, rel)
		} else {
			deleted = append(deleted, rel)
		}
	}
	for _, rel := range deleted {
		k.markGone(ctx, rel, tree[rel].File, api.GoneDeleted, rep)
	}
	for start := 0; start < len(pastWindow); start += durableBatch {
		batch := pastWindow[start:min(start+durableBatch, len(pastWindow))]
		groups := make([][]api.BlobRef, len(batch))
		for i, rel := range batch {
			groups[i] = recoverRefs(tree[rel])
		}
		ok, err := k.allDurable(ctx, groups)
		if err != nil {
			k.logf("keeper %s: gone marks paused: %v", k.cfg.Folder, err)
			return
		}
		for i, rel := range batch {
			why := api.GoneDeleted
			if ok[i] {
				why = api.GoneExpired
			}
			k.markGone(ctx, rel, tree[rel].File, why, rep)
		}
	}
}

// recoverRefs is every blob that must be durable for e to be recoverable
// by name: its content plus the claims and permanodes that name it.
func recoverRefs(e api.PathEntry) []api.BlobRef {
	return append([]api.BlobRef{e.File}, e.Claims...)
}

// allDurable asks the server about every ref in groups in one call and
// reports, for each group, whether all of its refs are durable.
func (k *Keeper) allDurable(ctx context.Context, groups [][]api.BlobRef) ([]bool, error) {
	var refs []api.BlobRef
	for _, g := range groups {
		refs = append(refs, g...)
	}
	ok, err := k.cfg.Registry.Durable(ctx, refs)
	if err == nil && len(ok) != len(refs) {
		err = fmt.Errorf("durable returned %d answers for %d refs", len(ok), len(refs))
	}
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(groups))
	i := 0
	for gi, g := range groups {
		out[gi] = true
		for range g {
			out[gi] = out[gi] && ok[i]
			i++
		}
	}
	return out, nil
}

// verifyRef returns the file's blobref by putting it again (the server
// already has a kept version, so nothing is re-sent). It never trusts the
// put cache: an in-place edit that keeps size and mtime would match the
// cache and then be deleted by expiry (docs/design.md, Edge cases).
func (k *Keeper) verifyRef(ctx context.Context, f file) (api.BlobRef, error) {
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
