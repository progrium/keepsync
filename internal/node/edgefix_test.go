package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

// TestRecoveredRegistryWins checks that a membership from a new registry
// epoch is applied even when its version is lower than the one the node
// last saw, as after recovering an older registry, while a stale version
// from the same epoch is still ignored.
func TestRecoveredRegistryWins(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	m := tn.freg.Membership()
	m.Epoch, m.Version = "old", 500
	tn.apply(tn.ctx, m)

	stale := tn.freg.Membership()
	stale.Epoch, stale.Version = "old", 499
	stale.Folders = nil
	tn.apply(tn.ctx, stale)
	if tn.member.Version != 500 {
		t.Fatalf("stale version from the same epoch applied: %d", tn.member.Version)
	}

	rec := tn.freg.Membership()
	rec.Epoch, rec.Version = "recovered", 3
	rec.Folders = nil
	tn.apply(tn.ctx, rec)
	if tn.member.Epoch != "recovered" || tn.member.Version != 3 || len(tn.member.Folders) != 0 {
		t.Fatalf("recovered registry's membership not applied: %+v", tn.member)
	}
	// The saved copy is the recovered one too, so a restart keeps it.
	if saved, ok := tn.loadMembership(); !ok || saved.Epoch != "recovered" {
		t.Fatalf("saved membership: %+v %v", saved, ok)
	}
}

// flakyLeave fails LeaveFolder while down is set.
type flakyLeave struct {
	api.RegistryClient
	down atomic.Bool
}

func (f *flakyLeave) LeaveFolder(ctx context.Context, name string) error {
	if f.down.Load() {
		return errors.New("server unreachable")
	}
	return f.RegistryClient.LeaveFolder(ctx, name)
}

func isMember(m api.Membership, folder string, key api.NodeKey) bool {
	for _, f := range m.Folders {
		if f.Name == folder {
			return slices.Contains(f.Members, key)
		}
	}
	return false
}

// TestFolderRmRetriesLeave checks that `folder rm` with the server down
// leaves the folder on the server once it answers, so the node does not
// stay a member and keeper of a folder it no longer has.
func TestFolderRmRetriesLeave(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	fl := &flakyLeave{RegistryClient: tn.reg}
	tn.reg = fl
	fl.down.Store(true)
	res, err := tn.folderRm(tn.ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	if res.Left || res.Warning == "" {
		t.Fatalf("rm with the server down: %+v", res)
	}
	self := tn.tr.Key()
	if !isMember(tn.freg.Membership(), "docs", self) {
		t.Fatal("left although the server was down")
	}
	tn.retryLeaves(tn.ctx) // still down: kept for later
	if _, err := os.Stat(filepath.Join(tn.cfg.Dir, pendingLeaveFile)); err != nil {
		t.Fatalf("pending leave not saved: %v", err)
	}

	fl.down.Store(false)
	tn.retryLeaves(tn.ctx)
	if isMember(tn.freg.Membership(), "docs", self) {
		t.Fatal("still a member after the server came back")
	}
	if _, err := os.Stat(filepath.Join(tn.cfg.Dir, pendingLeaveFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending leave not cleared: %v", err)
	}
}

// TestRejoinCancelsPendingLeave checks that joining the folder again before
// the server answers drops the pending leave instead of leaving later.
func TestRejoinCancelsPendingLeave(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	fl := &flakyLeave{RegistryClient: tn.reg}
	tn.reg = fl
	fl.down.Store(true)
	if _, err := tn.folderRm(tn.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	if _, err := tn.attach(tn.ctx, FolderParams{Name: "docs", Path: tn.dir, Merge: true}, tn.reg.JoinFolder); err != nil {
		t.Fatal(err)
	}
	fl.down.Store(false)
	tn.retryLeaves(tn.ctx)
	if !isMember(tn.freg.Membership(), "docs", tn.tr.Key()) {
		t.Fatal("left a folder that was joined again")
	}
}

// TestNewHookRunnerPicksUpConflicts checks that a node that becomes the
// hook runner (keeper failover) runs the hook for conflict copies it set
// aside as another node's job.
func TestNewHookRunnerPicksUpConflicts(t *testing.T) {
	tn := newTestNodeOpts(t, func(pk *fake.Perkeep) api.BlobPutter { return pk }, true) // "other" is Members[0]
	ran := filepath.Join(t.TempDir(), "ran")
	tn.cfg.ConflictHook = hookScript(t, `echo ran >> `+ran+`
exit 1
`)
	tn.conf().scanned = true // no startup scan: only failover may pick the copy up
	tn.sync()
	cp := filepath.Join(tn.dir, copyName)
	writeOld(t, cp, "theirs", time.Minute)
	if ci := tn.runConflict(tn.ctx, "docs", cp, false); ci.State != HookNotMine {
		t.Fatalf("before failover: %+v", ci)
	}

	// The registry makes this node the first member (the hook runner of an
	// unkept folder), as failover does for Keepers[0].
	m := tn.freg.Membership()
	m.Version++
	for i := range m.Folders {
		if m.Folders[i].Name == "docs" {
			m.Folders[i].Members = []api.NodeKey{tn.tr.Key(), "nodekey:other"}
		}
	}
	tn.apply(tn.ctx, m)
	// The hook runs after the conflict debounce.
	deadline := time.Now().Add(conflictDebounce + 5*time.Second)
	for {
		if _, err := os.Stat(ran); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the new hook runner never ran the hook")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// failingPutter refuses one file by name.
type failingPutter struct {
	api.BlobPutter
	bad string
}

func (f failingPutter) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	if filepath.Base(path) == f.bad {
		return "", errors.New("refused for the test")
	}
	return f.BlobPutter.PutFile(ctx, path)
}

// TestKeepStatusShowsFileErrors checks that a file the keeper fails on
// every pass is named in keep status and logged once, not only counted.
func TestKeepStatusShowsFileErrors(t *testing.T) {
	var logged atomic.Int64
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return failingPutter{pk, "bad.txt"} })
	tn.logf = func(format string, args ...any) {
		if strings.Contains(fmt.Sprintf(format, args...), "refused for the test") {
			logged.Add(1)
		}
	}
	writeOld(t, filepath.Join(tn.dir, "bad.txt"), "x", 10*24*time.Hour)
	writeOld(t, filepath.Join(tn.dir, "good.txt"), "y", 10*24*time.Hour)
	tn.keep(t)
	var ks KeepStatus
	waitFor(t, "a few passes", func() bool {
		var err error
		ks, err = tn.keepStatus(tn.ctx, "docs")
		return err == nil && len(ks.Errors) > 0 && logged.Load() > 0
	})
	time.Sleep(100 * time.Millisecond) // several more 20ms passes
	if len(ks.Errors) != 1 || !strings.Contains(ks.Errors[0], "bad.txt") {
		t.Fatalf("keep status errors: %q", ks.Errors)
	}
	if n := logged.Load(); n != 1 {
		t.Fatalf("error logged %d times, want once", n)
	}
}

// TestJoinAtAnotherPath checks that an attached folder is not moved by
// joining it again elsewhere, which would leave Syncthing's index of the
// old directory to read the new one as all deleted.
func TestJoinAtAnotherPath(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	other := filepath.Join(t.TempDir(), "elsewhere")
	_, err := tn.attach(tn.ctx, FolderParams{Name: "docs", Path: other, Merge: true}, tn.reg.JoinFolder)
	if err == nil || !strings.Contains(err.Error(), "folder rm docs") {
		t.Fatalf("join at another path: %v, want a folder rm hint", err)
	}
	if f, _ := tn.cfg.Folder("docs"); f.Path != tn.dir {
		t.Errorf("folder moved to %s", f.Path)
	}
	// After folder rm it can be joined at the new path.
	if _, err := tn.folderRm(tn.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	if _, err := tn.attach(tn.ctx, FolderParams{Name: "docs", Path: other, Merge: true}, tn.reg.JoinFolder); err != nil {
		t.Fatal(err)
	}
	if f, _ := tn.cfg.Folder("docs"); f.Path != other {
		t.Errorf("folder at %s, want %s", f.Path, other)
	}
}

func TestRelativePathRefused(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	tn.folderRm(tn.ctx, "docs")
	_, err := tn.attach(tn.ctx, FolderParams{Name: "docs", Path: "./Demo", Merge: true}, tn.reg.JoinFolder)
	if err == nil || !strings.Contains(err.Error(), "relative") {
		t.Fatalf("join with ./Demo: %v, want a relative path error", err)
	}
	if _, err := os.Stat("Demo"); err == nil {
		t.Error("created Demo in the node's working directory")
	}
}
