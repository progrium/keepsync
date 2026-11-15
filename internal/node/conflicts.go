package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"perkeep.org/pkg/blob"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/hook"
	"github.com/progrium/keepsync/internal/syncer"
)

// conflictDebounce is how long a conflict copy must be quiet before its
// hook runs.
var conflictDebounce = 5 * time.Second

// Hook states, as `keepsync conflicts` shows them.
const (
	HookNone     = "none"          // no hook configured for the folder
	HookPending  = "pending"       // waiting for the debounce or its turn
	HookRunning  = "running"       // the hook is running now
	HookFailed   = "failed"        // the hook ran and left it for a human
	HookResolved = "resolved"      // the hook resolved it
	HookNotMine  = "not-this-node" // another node runs the hook
)

// ConflictInfo is one conflict copy in MethodConflicts.
type ConflictInfo struct {
	Folder   string    `json:"folder"`
	Path     string    `json:"path"`     // the conflict copy, absolute
	Original string    `json:"original"` // the original, absolute
	Node     string    `json:"node"`     // whose change the copy holds
	State    string    `json:"state"`
	Reason   string    `json:"reason,omitempty"`
	LastRun  time.Time `json:"lastRun,omitzero"`
}

// cstate is what the node remembers about one conflict copy.
type cstate struct {
	state   string
	reason  string
	lastRun time.Time
	stamp   fileStamp // the copy when the hook last ran
}

type fileStamp struct {
	size  int64
	mtime time.Time
}

func stampOf(path string) (fileStamp, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{fi.Size(), fi.ModTime()}, nil
}

// conflicts tracks conflict copies and runs the hook for them.
type conflicts struct {
	mu      sync.Mutex
	states  map[string]*cstate     // by absolute copy path
	timers  map[string]*time.Timer // debounce, by absolute copy path
	folders map[string]*sync.Mutex // one hook at a time per folder
	// resolved remembers copies the hook resolved, by absolute original
	// path, so a late identical copy is known as a duplicate.
	resolved map[string][]resolvedCopy
	scanned  bool
}

type resolvedCopy struct {
	name string
	sum  [sha256.Size]byte
	at   time.Time
}

// duplicateWindow is how long a resolved copy's content is remembered.
const duplicateWindow = time.Hour

func pruneResolved(rs []resolvedCopy) []resolvedCopy {
	out := rs[:0]
	for _, r := range rs {
		if time.Since(r.at) < duplicateWindow {
			out = append(out, r)
		}
	}
	return out
}

// duplicateOf returns the name of another copy of orig with the same
// content as the copy abs (whose SHA-256 is sum): one the hook resolved
// recently, or one still present whose name sorts first, so every node
// picks the same survivor. It returns "" if abs is not a duplicate.
func (n *Node) duplicateOf(abs, orig string, sum [sha256.Size]byte) string {
	c := n.conf()
	c.mu.Lock()
	rs := pruneResolved(c.resolved[orig])
	c.resolved[orig] = rs
	for _, r := range rs {
		if r.sum == sum {
			c.mu.Unlock()
			return r.name
		}
	}
	c.mu.Unlock()
	dir, base := filepath.Split(abs)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	for _, e := range ents {
		name := e.Name()
		if name >= base || !e.Type().IsRegular() || !syncer.IsConflict(name) {
			continue
		}
		if o, _ := syncer.ConflictOriginal(filepath.Join(dir, name)); o != orig {
			continue
		}
		if s, err := fileSum(filepath.Join(dir, name)); err == nil && s == sum {
			return name
		}
	}
	return ""
}

func fileSum(path string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	f, err := os.Open(path)
	if err != nil {
		return sum, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return sum, err
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

func (n *Node) conf() *conflicts {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cf == nil {
		n.cf = &conflicts{states: map[string]*cstate{}, timers: map[string]*time.Timer{}, folders: map[string]*sync.Mutex{}, resolved: map[string][]resolvedCopy{}}
	}
	return n.cf
}

// folderByID returns the attached folder name for a Syncthing folder ID.
func (n *Node) folderByID(id string) (string, bool) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	for _, f := range n.cfg.Folders {
		if f.ID == id {
			return f.Name, true
		}
	}
	return "", false
}

// onConflict schedules the hook for a conflict copy after the debounce.
func (n *Node) onConflict(folder, abs string) {
	c := n.conf()
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.states[abs]
	if !ok {
		st = &cstate{}
		c.states[abs] = st
	}
	if st.state != HookRunning && st.state != HookFailed {
		st.state = HookPending
	}
	if t, ok := c.timers[abs]; ok {
		t.Stop()
	}
	c.timers[abs] = time.AfterFunc(conflictDebounce, func() {
		c.mu.Lock()
		delete(c.timers, abs)
		c.mu.Unlock()
		if n.ctx.Err() == nil {
			n.runConflict(n.ctx, folder, abs, false)
		}
	})
}

// scanConflicts schedules every conflict copy in the attached folders, once
// the node knows the membership (startup), and again for a folder when it
// is attached.
func (n *Node) scanConflicts(folders ...string) {
	for _, ci := range n.findConflicts(folders...) {
		n.onConflict(ci.Folder, ci.Path)
	}
}

// findConflicts walks attached folders (all if none named) for copies.
func (n *Node) findConflicts(names ...string) []ConflictInfo {
	n.cfgMu.Lock()
	local := slices.Clone(n.cfg.Folders)
	n.cfgMu.Unlock()
	var out []ConflictInfo
	for _, lf := range local {
		if len(names) > 0 && !slices.Contains(names, lf.Name) {
			continue
		}
		filepath.WalkDir(lf.Path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				if b := d.Name(); b == ".stversions" || b == ".stfolder" {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Type().IsRegular() && syncer.IsConflict(d.Name()) {
				orig, _ := syncer.ConflictOriginal(p)
				out = append(out, ConflictInfo{Folder: lf.Name, Path: p, Original: orig})
			}
			return nil
		})
	}
	return out
}

// hookNode reports whether this node runs the hook for folder: the first
// keeper of a kept folder, else the first member.
func (n *Node) hookNode(folder string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	self := n.tr.Key()
	for _, f := range n.member.Folders {
		if f.Name != folder {
			continue
		}
		if f.Keep != nil {
			return len(f.Keep.Keepers) > 0 && f.Keep.Keepers[0] == self
		}
		return len(f.Members) > 0 && f.Members[0] == self
	}
	return false
}

// conflictNode names the node whose change a conflict copy holds.
func (n *Node) conflictNode(folder, copyPath string) string {
	// The copy's name carries the short ID of the device whose losing
	// version it holds (the vendored Syncthing names it that way, on every
	// node that makes a copy). The copy's own ModifiedBy is no use: a
	// bystander that received both versions makes its own copy.
	short, ok := syncer.ConflictDevice(filepath.Base(copyPath))
	if !ok {
		return ""
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, nd := range n.member.Nodes {
		if strings.HasPrefix(string(nd.Device), short) && nd.Name != "" {
			return nd.Name
		}
	}
	return short
}

func (n *Node) folderKeep(folder string) (*api.KeepPolicy, api.BlobRef) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, f := range n.member.Folders {
		if f.Name == folder {
			return f.Keep, f.Archive
		}
	}
	return nil, ""
}

// runConflict runs the hook for one copy. Unless force (conflicts
// resolve), only the hook node runs it, and a copy whose hook already
// failed is not rerun until the copy changes.
func (n *Node) runConflict(ctx context.Context, folder, abs string, force bool) ConflictInfo {
	c := n.conf()
	orig, _ := syncer.ConflictOriginal(abs)
	info := ConflictInfo{Folder: folder, Path: abs, Original: orig, Node: n.conflictNode(folder, abs)}
	set := func(state, reason string, stamp fileStamp, ran bool) ConflictInfo {
		c.mu.Lock()
		st := c.states[abs]
		if st == nil {
			st = &cstate{}
			c.states[abs] = st
		}
		st.state, st.reason = state, reason
		if ran {
			st.lastRun, st.stamp = time.Now(), stamp
		}
		info.State, info.Reason, info.LastRun = st.state, st.reason, st.lastRun
		if state == HookResolved {
			delete(c.states, abs)
		}
		c.mu.Unlock()
		return info
	}

	lf, err := n.localFolder(folder)
	if err != nil {
		return set(HookFailed, err.Error(), fileStamp{}, false)
	}
	stamp, err := stampOf(abs)
	if errors.Is(err, fs.ErrNotExist) {
		c.mu.Lock()
		delete(c.states, abs)
		c.mu.Unlock()
		info.State = HookResolved
		info.Reason = "the conflict copy is gone"
		return info
	}
	script := n.cfg.HookFor(folder)
	if script == "" {
		return set(HookNone, "", fileStamp{}, false)
	}
	if !force && !n.hookNode(folder) {
		return set(HookNotMine, "", fileStamp{}, false)
	}
	if !force {
		c.mu.Lock()
		st := c.states[abs]
		skip := st != nil && st.state == HookFailed && st.stamp == stamp
		var reason string
		if skip {
			reason = st.reason
		}
		c.mu.Unlock()
		if skip {
			return set(HookFailed, reason, fileStamp{}, false)
		}
	}

	c.mu.Lock()
	fmu := c.folders[folder]
	if fmu == nil {
		fmu = &sync.Mutex{}
		c.folders[folder] = fmu
	}
	c.mu.Unlock()
	fmu.Lock()
	defer fmu.Unlock()
	set(HookRunning, "", fileStamp{}, false)

	origRel, _ := filepath.Rel(lf.Path, orig)
	copyRel, _ := filepath.Rel(lf.Path, abs)
	origRel, copyRel = filepath.ToSlash(origRel), filepath.ToSlash(copyRel)

	// Every node that held the losing version can make a copy of it (a
	// bystander that got both versions does too), so one conflict can
	// show up as several identical copies, at once or one after another.
	// The hook runs once; the duplicates are removed.
	sum, err := fileSum(abs)
	if err != nil {
		return set(HookFailed, err.Error(), stamp, true)
	}
	if dup := n.duplicateOf(abs, orig, sum); dup != "" {
		if err := os.Remove(abs); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return set(HookFailed, "remove the duplicate copy: "+err.Error(), stamp, true)
		}
		n.logf("conflict copy %s in %s duplicates %s; removed it", copyRel, folder, dup)
		return set(HookResolved, "duplicate of "+dup, stamp, true)
	}

	var base string
	var exact bool
	if keep, archive := n.folderKeep(folder); keep != nil {
		// The base is read before KeepNow replaces the kept original with
		// the local version.
		if archive == "" {
			archive = keep.Permanode
		}
		tmp, err := os.MkdirTemp("", "keepsync-base-")
		if err != nil {
			return set(HookFailed, err.Error(), stamp, true)
		}
		defer os.RemoveAll(tmp)
		base, exact, err = n.conflictBase(ctx, lf.ID, archive, origRel, orig, abs, tmp)
		if err != nil {
			return set(HookFailed, err.Error(), stamp, true)
		}
		// Keep both versions first, so whatever the hook does can be undone.
		n.mu.Lock()
		k := n.keepers[folder]
		n.mu.Unlock()
		if k == nil {
			return set(HookFailed, "the folder is kept but its keeper is not running here", stamp, true)
		}
		rep, err := k.k.KeepNow(ctx, origRel, copyRel)
		if err == nil && len(rep.Errors) > 0 {
			err = errors.Join(rep.Errors...)
		}
		if err != nil {
			return set(HookFailed, "keep both versions before the hook: "+err.Error(), stamp, true)
		}
	}

	timeout := time.Duration(n.cfg.ConflictHookTimeout)
	n.logf("conflict hook for %s in %s", copyRel, folder)
	res := hook.Run(ctx, hook.Job{
		Script: script, Folder: folder, Dir: lf.Path,
		Original: orig, Conflict: abs, ConflictNode: info.Node, Base: base, BaseExact: exact,
		Timeout: timeout, Logf: n.logf,
	})
	if res.Resolved {
		n.logf("conflict hook resolved %s in %s", copyRel, folder)
		c.mu.Lock()
		c.resolved[orig] = append(pruneResolved(c.resolved[orig]), resolvedCopy{name: filepath.Base(abs), sum: sum, at: time.Now()})
		c.mu.Unlock()
		return set(HookResolved, "", stamp, true)
	}
	n.logf("conflict hook left %s in %s for a human: %s", copyRel, folder, res.Reason)
	return set(HookFailed, res.Reason, stamp, true)
}

// listConflicts is MethodConflicts.
func (n *Node) listConflicts() []ConflictInfo {
	c := n.conf()
	found := n.findConflicts()
	for i := range found {
		ci := &found[i]
		ci.Node = n.conflictNode(ci.Folder, ci.Path)
		c.mu.Lock()
		st := c.states[ci.Path]
		if st != nil {
			ci.State, ci.Reason, ci.LastRun = st.state, st.reason, st.lastRun
		}
		c.mu.Unlock()
		// Who runs the hook can change with the membership, so states
		// that depend on it are worked out now.
		if st == nil || st.state == HookNone || st.state == HookNotMine || st.state == HookPending {
			switch {
			case n.cfg.HookFor(ci.Folder) == "":
				ci.State, ci.Reason = HookNone, ""
			case !n.hookNode(ci.Folder):
				ci.State, ci.Reason = HookNotMine, ""
			default:
				ci.State = HookPending
			}
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].Path < found[j].Path })
	return found
}

// resolveConflict is MethodConflictResolve: run the hook now on this node.
func (n *Node) resolveConflict(ctx context.Context, path string) (ConflictInfo, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return ConflictInfo{}, err
	}
	if !syncer.IsConflict(filepath.Base(abs)) {
		return ConflictInfo{}, fmt.Errorf("%s is not a conflict copy", path)
	}
	n.cfgMu.Lock()
	var folder string
	for _, f := range n.cfg.Folders {
		if strings.HasPrefix(abs, f.Path+string(filepath.Separator)) {
			folder = f.Name
		}
	}
	n.cfgMu.Unlock()
	if folder == "" {
		return ConflictInfo{}, fmt.Errorf("%s is not in a folder attached here", path)
	}
	if n.cfg.HookFor(folder) == "" {
		return ConflictInfo{}, fmt.Errorf("no conflict hook is configured for %s (conflict_hook in node.toml)", folder)
	}
	return n.runConflict(ctx, folder, abs, true), nil
}

// conflictBase writes the hook's base for the original at origRel (orig on
// disk) and its conflict copy into dir, and reports whether it is exact.
//
// The base is the newest kept version whose content differs from both
// sides and that the original's current Syncthing version W descends from
// (an entry kept without a version counts as an ancestor). It is exact if
// that entry has a version. Failing that, it is the last kept version, as
// a guess; with nothing kept at the path there is no base.
func (n *Node) conflictBase(ctx context.Context, folderID string, archive api.BlobRef, origRel, orig, copyAbs, dir string) (string, bool, error) {
	hist, err := n.reg.History(ctx, archive, origRel)
	if err != nil {
		return "", false, fmt.Errorf("read the kept history: %w", err)
	}
	var w api.Version
	if st, err := n.sy.FileState(folderID, origRel); err == nil && st.Known {
		w = st.Version
	}
	var f blob.Fetcher
	fetch := func(ref api.BlobRef, dst string) error {
		if f == nil {
			var err error
			if f, err = n.fetcher(); err != nil {
				return err
			}
		}
		_, _, err := restoreFile(ctx, f, ref, 0o600, dst, true)
		if err != nil {
			return fmt.Errorf("fetch the base: %w", err)
		}
		return nil
	}
	dst := filepath.Join(dir, filepath.Base(origRel))
	cand := filepath.Join(dir, ".candidate")
	differs := map[api.BlobRef]bool{} // content differs from both sides
	for i := len(hist) - 1; i >= 0; i-- {
		e := hist[i]
		if len(e.Version) > 0 && !w.DescendsFrom(e.Version) {
			continue
		}
		d, seen := differs[e.File]
		if !seen {
			if err := fetch(e.File, cand); err != nil {
				return "", false, err
			}
			same, err := sameContent(cand, orig)
			if err == nil && !same {
				same, err = sameContent(cand, copyAbs)
			}
			if err != nil {
				return "", false, fmt.Errorf("compare the base: %w", err)
			}
			d = !same
			differs[e.File] = d
		}
		if !d {
			continue
		}
		// Only the first fetch of a ref can get here, so cand holds it.
		if err := os.Rename(cand, dst); err != nil {
			return "", false, err
		}
		return dst, len(e.Version) > 0, nil
	}
	os.Remove(cand)

	// No exact ancestor: fall back to the last kept version.
	tree, err := n.reg.Paths(ctx, archive)
	if err != nil {
		return "", false, fmt.Errorf("read the kept tree: %w", err)
	}
	e, ok := tree[origRel]
	if !ok {
		return "", false, nil
	}
	if err := fetch(e.File, dst); err != nil {
		return "", false, err
	}
	return dst, false, nil
}

// sameContent reports whether files a and b hold the same bytes. A
// missing b counts as different.
func sameContent(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer fa.Close()
	fb, err := os.Open(b)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer fb.Close()
	sa, err := fa.Stat()
	if err != nil {
		return false, err
	}
	sb, err := fb.Stat()
	if err != nil {
		return false, err
	}
	if sa.Size() != sb.Size() {
		return false, nil
	}
	ba, bb := make([]byte, 64<<10), make([]byte, 64<<10)
	for {
		na, ea := io.ReadFull(fa, ba)
		nb, eb := io.ReadFull(fb, bb)
		if na != nb || !bytes.Equal(ba[:na], bb[:nb]) {
			return false, nil
		}
		if ea == io.EOF || ea == io.ErrUnexpectedEOF {
			return eb == io.EOF || eb == io.ErrUnexpectedEOF, nil
		}
		if ea != nil {
			return false, ea
		}
		if eb != nil {
			return false, eb
		}
	}
}
