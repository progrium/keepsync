package keeper

import (
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// fstate is what Syncthing says about one local file. ok is false when the
// keeper has no Syncer or the lookup failed: the keeper then falls back to
// mtimes alone.
type fstate struct {
	api.FileState
	ok bool
}

func (k *Keeper) fileState(rel string) fstate {
	if k.cfg.Syncer == nil || k.cfg.FolderID == "" {
		return fstate{}
	}
	st, err := k.cfg.Syncer.FileState(k.cfg.FolderID, rel)
	if err != nil {
		return fstate{}
	}
	return fstate{FileState: st, ok: true}
}

// notCurrent reports whether Syncthing says this node does not hold the
// current version of a local file, so it must not be kept or expired.
func (s fstate) notCurrent() bool {
	return s.ok && (!s.Known || !s.Current || s.Deleted || s.Dir)
}

// sameSecond compares mtimes truncated to whole seconds, so filesystems
// with coarse mtimes (FAT, some network mounts) don't look like edits.
func sameSecond(a, b time.Time) bool {
	return a.Truncate(time.Second).Equal(b.Truncate(time.Second))
}

func afterSecond(a, b time.Time) bool {
	return a.Truncate(time.Second).After(b.Truncate(time.Second))
}

// versionOrder compares the local file to the kept entry: by version
// vector when both have one and they are not concurrent, otherwise by
// mtime truncated to the second.
func versionOrder(f file, s fstate, e api.PathEntry) api.Ordering {
	if s.ok && len(s.Version) > 0 && len(e.Version) > 0 {
		if o := s.Version.Compare(e.Version); o != api.Concurrent {
			return o
		}
	}
	switch {
	case sameSecond(f.mtime, e.ModTime):
		return api.Equal
	case afterSecond(f.mtime, e.ModTime):
		return api.Newer
	}
	return api.Older
}

// keptVersion reports whether the archive already holds this version of f.
// Expiry also checks the content ref before deleting anything.
func keptVersion(f file, s fstate, tree map[string]api.PathEntry) bool {
	e, ok := tree[f.rel]
	return ok && versionOrder(f, s, e) == api.Equal
}

// laggingVersion reports whether the archive holds a newer version than f.
func laggingVersion(f file, s fstate, tree map[string]api.PathEntry) bool {
	e, ok := tree[f.rel]
	return ok && versionOrder(f, s, e) == api.Older
}
