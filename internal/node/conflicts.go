package node

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

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
	scanned bool
}

func (n *Node) conf() *conflicts {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cf == nil {
		n.cf = &conflicts{states: map[string]*cstate{}, timers: map[string]*time.Timer{}, folders: map[string]*sync.Mutex{}}
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
	// The node that lost creates the copy, so the copy's ModifiedBy is
	// the device whose change it holds. The ID in the copy's name is the
	// winner's and is no use here.
	var dev api.DeviceID
	n.cfgMu.Lock()
	f, ok := n.cfg.Folder(folder)
	n.cfgMu.Unlock()
	if ok && n.sy != nil {
		if rel, err := filepath.Rel(f.Path, copyPath); err == nil {
			dev, _ = n.sy.ModifiedBy(f.ID, filepath.ToSlash(rel))
		}
	}
	if dev == "" {
		return ""
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, nd := range n.member.Nodes {
		if nd.Device == dev && nd.Name != "" {
			return nd.Name
		}
	}
	if len(dev) > 7 {
		return string(dev[:7])
	}
	return string(dev)
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

	var base string
	if keep, archive := n.folderKeep(folder); keep != nil {
		// The base is the original as last kept, read before KeepNow
		// replaces it with the local version.
		if archive == "" {
			archive = keep.Permanode
		}
		tree, err := n.reg.Paths(ctx, archive)
		if err != nil {
			return set(HookFailed, "read the kept tree: "+err.Error(), stamp, true)
		}
		if e, ok := tree[origRel]; ok {
			tmp, err := os.MkdirTemp("", "keepsync-base-")
			if err != nil {
				return set(HookFailed, err.Error(), stamp, true)
			}
			defer os.RemoveAll(tmp)
			f, err := n.fetcher()
			if err == nil {
				base = filepath.Join(tmp, filepath.Base(origRel))
				_, _, err = restoreFile(ctx, f, e.File, base, true)
			}
			if err != nil {
				return set(HookFailed, "fetch the base: "+err.Error(), stamp, true)
			}
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
		Original: orig, Conflict: abs, ConflictNode: info.Node, Base: base,
		Timeout: timeout, Logf: n.logf,
	})
	if res.Resolved {
		n.logf("conflict hook resolved %s in %s", copyRel, folder)
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
