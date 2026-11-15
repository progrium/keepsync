package keeper

import (
	"context"
	"fmt"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// State classifies one file for `keepsync keep status`.
type State int

const (
	StateGrace     State = iota // unkept version inside grace (or waiting for max-defer)
	StateDue                    // past grace or max-defer; the next pass keeps it
	StateKept                   // this version is in the kept tree
	StateExpirable              // kept, durable and past expire; the next pass removes it
	StateLagging                // local copy is older than the archive's
	// StateChanged: the kept version's mtime (or version) but different
	// content, so it is not the kept version and is never expired.
	StateChanged
)

func (s State) String() string {
	switch s {
	case StateGrace:
		return "grace"
	case StateDue:
		return "due"
	case StateKept:
		return "kept"
	case StateExpirable:
		return "expirable"
	case StateLagging:
		return "lagging"
	case StateChanged:
		return "changed"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// FileStatus is one file's keep state.
type FileStatus struct {
	Path    string
	State   State
	ModTime time.Time
	Size    int64
	// KeepAt is when the file leaves grace (StateGrace only): the earlier
	// of mtime+grace and the max-defer cap.
	KeepAt time.Time
	// ExpireAt is when a kept file becomes eligible to expire, or zero if
	// the folder never expires.
	ExpireAt time.Time
}

// Status is the folder's keep state.
type Status struct {
	Policy api.KeepPolicy
	Files  []FileStatus
	// DurabilityUnknown is set when the server could not say which kept
	// files are durable, so none are reported expirable.
	DurabilityUnknown bool
}

// Counts tallies files by state.
func (s Status) Counts() map[State]int {
	c := map[State]int{}
	for _, f := range s.Files {
		c[f.State]++
	}
	return c
}

// Status reports every file's keep state without changing anything. Like
// expiry, it re-puts each file it would report expirable to check its
// content is the kept version; the server already has a kept version's
// blobs, so normally nothing is sent. It works on any member node, keeper
// or not.
func (k *Keeper) Status(ctx context.Context) (Status, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	st := Status{Policy: k.policy}
	files, err := k.walk()
	if err != nil {
		return st, err
	}
	tree, err := k.cfg.Registry.Paths(ctx, k.policy.Permanode)
	if err != nil {
		return st, fmt.Errorf("keeper: fetching kept tree: %w", err)
	}
	now := k.cfg.Clock.Now()
	var expIdx []int
	var expRefs [][]api.BlobRef
	byPath := make(map[string]file, len(files))
	for _, f := range files {
		byPath[f.rel] = f
		fs := FileStatus{Path: f.rel, ModTime: f.mtime, Size: f.size}
		e := tree[f.rel]
		fst := k.fileState(f.rel)
		switch {
		case fst.notCurrent():
			fs.State = StateLagging
		case keptVersion(f, fst, tree) && k.cachedOtherRef(f, e.File):
			fs.State = StateChanged
		case keptVersion(f, fst, tree):
			fs.State = StateKept
			if k.policy.Expire > 0 {
				fs.ExpireAt = f.mtime.Add(k.policy.Expire)
				if !now.Before(fs.ExpireAt) && len(e.Claims) > 0 {
					expIdx = append(expIdx, len(st.Files))
					expRefs = append(expRefs, recoverRefs(e))
				}
			}
		case laggingVersion(f, fst, tree):
			fs.State = StateLagging
		case k.cachedRejected(f):
			fs.State = StateLagging
		default:
			first := f.mtime
			if d, ok := k.dirty[f.rel]; ok && d.Before(first) {
				first = d
			}
			keepAt := f.mtime.Add(k.policy.Grace)
			if capAt := first.Add(k.policy.MaxDefer); capAt.Before(keepAt) {
				keepAt = capAt
			}
			if now.Before(keepAt) {
				fs.State = StateGrace
				fs.KeepAt = keepAt
			} else {
				fs.State = StateDue
			}
		}
		st.Files = append(st.Files, fs)
	}
	for start := 0; start < len(expRefs); start += durableBatch {
		end := min(start+durableBatch, len(expRefs))
		ok, err := k.allDurable(ctx, expRefs[start:end])
		if err != nil {
			st.DurabilityUnknown = true
			break
		}
		for i, d := range ok {
			if !d {
				continue
			}
			// Report expirable only what expiry would remove: it checks
			// the content first, and so does this.
			fs := &st.Files[expIdx[start+i]]
			f := byPath[fs.Path]
			ref, err := k.verifyRef(ctx, f)
			switch {
			case err != nil:
				// Leave it as kept; the next pass reports the error.
			case ref != tree[f.rel].File:
				fs.State, fs.ExpireAt = StateChanged, time.Time{}
			default:
				fs.State = StateExpirable
			}
		}
	}
	return st, nil
}

// cachedOtherRef reports whether this keeper put f's exact mtime and size
// and got a different ref than want: same version marker, other content.
func (k *Keeper) cachedOtherRef(f file, want api.BlobRef) bool {
	c, ok := k.putCache[f.rel]
	return ok && c.mtime.Equal(f.mtime) && c.size == f.size && c.ref != want
}

func (k *Keeper) cachedRejected(f file) bool {
	c, ok := k.putCache[f.rel]
	return ok && c.result == api.ClaimRejectedOlder && c.mtime.Equal(f.mtime) && c.size == f.size
}
