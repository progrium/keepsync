package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/memory"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

const copyName = "a.sync-conflict-20260101-120000-OTHERDV.txt"

// orderLog records puts and the hook run in one file, in order.
type orderLog struct {
	mu   sync.Mutex
	path string
}

func (o *orderLog) add(line string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f, _ := os.OpenFile(o.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	f.WriteString(line + "\n")
	f.Close()
}

type loggingPutter struct {
	memPutter
	log *orderLog
}

func (l loggingPutter) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	l.log.add("put " + filepath.Base(path))
	return l.memPutter.PutFile(ctx, path)
}

func hookScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hook.sh")
	os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755)
	return p
}

func TestConflictHookKeepsBothAndGetsBase(t *testing.T) {
	sto := new(memory.Storage)
	ol := &orderLog{path: filepath.Join(t.TempDir(), "order.log")}
	tn := newTestNode(t, func(*fake.Perkeep) api.BlobPutter { return loggingPutter{memPutter{sto}, ol} })
	tn.fetcher = func() (blob.Fetcher, error) { return sto, nil }
	baseOut := filepath.Join(t.TempDir(), "base.out")
	tn.cfg.ConflictHook = hookScript(t, `cp "$KEEPSYNC_BASE" `+baseOut+`
echo "$KEEPSYNC_BASE_EXACT" > `+baseOut+`.exact
echo "hook node=$KEEPSYNC_CONFLICT_NODE" >> `+ol.path+`
cat "$KEEPSYNC_CONFLICT" > "$KEEPSYNC_ORIGINAL"
rm "$KEEPSYNC_CONFLICT"
`)
	orig := filepath.Join(tn.dir, "a.txt")
	writeOld(t, orig, "v1", 10*24*time.Hour)
	pn := tn.keep(t)
	waitFor(t, "v1 kept", func() bool { p, _ := tn.reg.Paths(tn.ctx, pn); _, ok := p["a.txt"]; return ok })
	tn.stopKeepers()
	os.Truncate(ol.path, 0)

	writeOld(t, orig, "v2 mine", time.Hour) // inside grace: only KeepNow keeps it
	cp := filepath.Join(tn.dir, copyName)
	writeOld(t, cp, "theirs", time.Minute)
	ci := tn.runConflict(tn.ctx, "docs", cp, false)
	if ci.State != HookResolved {
		t.Fatalf("%+v", ci)
	}
	if b, _ := os.ReadFile(baseOut); string(b) != "v1" {
		t.Fatalf("base = %q, want the last kept version v1", b)
	}
	if b, _ := os.ReadFile(baseOut + ".exact"); string(b) != "0\n" {
		t.Fatalf("KEEPSYNC_BASE_EXACT = %q, want 0 for a base kept without a version", b)
	}
	if b, _ := os.ReadFile(orig); string(b) != "theirs" {
		t.Fatalf("original = %q", b)
	}
	logb, _ := os.ReadFile(ol.path)
	lines := strings.Split(strings.TrimSpace(string(logb)), "\n")
	if len(lines) != 3 || lines[2] != "hook node=OTHERDV" || !strings.Contains(lines[0]+lines[1], "put a.txt") || !strings.Contains(lines[0]+lines[1], "put "+copyName) {
		t.Fatalf("order: %q", lines)
	}
	paths, _ := tn.reg.Paths(tn.ctx, pn)
	if _, ok := paths[copyName]; !ok {
		t.Fatalf("conflict copy not kept: %v", paths)
	}
}

func TestConflictHookOnlyFirstKeeper(t *testing.T) {
	tn := newTestNodeOpts(t, func(pk *fake.Perkeep) api.BlobPutter { return pk }, true)
	marker := filepath.Join(t.TempDir(), "ran")
	tn.cfg.ConflictHook = hookScript(t, "touch "+marker+"\nrm \"$KEEPSYNC_CONFLICT\"\n")
	tn.keep(t)
	if m := tn.freg.Membership(); m.Folders[0].Keep.Keepers[0] == tn.tr.Key() {
		t.Fatal("setup: this node should not be the first keeper")
	}
	cp := filepath.Join(tn.dir, copyName)
	writeOld(t, cp, "theirs", time.Minute)
	if ci := tn.runConflict(tn.ctx, "docs", cp, false); ci.State != HookNotMine {
		t.Fatalf("%+v", ci)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the hook ran on a node that is not Keepers[0]")
	}
	list := tn.listConflicts()
	if len(list) != 1 || list[0].State != HookNotMine || list[0].Node != "other" {
		t.Fatalf("list %+v", list)
	}
}

func TestConflictHookFailureNotRerun(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	count := filepath.Join(t.TempDir(), "count")
	tn.cfg.ConflictHook = hookScript(t, "echo x >> "+count+"\nexit 1\n")
	cp := filepath.Join(tn.dir, copyName)
	writeOld(t, cp, "theirs", time.Minute)
	tn.sync()
	for range 2 {
		if ci := tn.runConflict(tn.ctx, "docs", cp, false); ci.State != HookFailed {
			t.Fatalf("%+v", ci)
		}
	}
	if b, _ := os.ReadFile(count); strings.Count(string(b), "x") != 1 {
		t.Fatalf("hook ran %d times for an unchanged copy", strings.Count(string(b), "x"))
	}
	writeOld(t, cp, "theirs, edited", time.Second)
	tn.runConflict(tn.ctx, "docs", cp, false)
	if b, _ := os.ReadFile(count); strings.Count(string(b), "x") != 2 {
		t.Fatal("hook did not rerun after the copy changed")
	}
}

// putVersion keeps body at path in pn with Syncthing version v, through
// the putter (so the fetcher can read it back), and returns the file ref.
func putVersion(t *testing.T, tn *testNode, pn api.BlobRef, path, body string, v api.Version) api.BlobRef {
	t.Helper()
	src := filepath.Join(t.TempDir(), filepath.Base(path))
	os.WriteFile(src, []byte(body), 0o644)
	mt := time.Now()
	os.Chtimes(src, mt, mt)
	ref, err := tn.putter.PutFile(tn.ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	res, err := tn.reg.ClaimPath(tn.ctx, api.PathClaim{Permanode: pn, Path: path, File: ref, ModTime: mt, Version: v})
	if err != nil || res != api.ClaimWritten {
		t.Fatalf("claim %s %q: %v %v", path, body, res, err)
	}
	return ref
}

func newBaseTestNode(t *testing.T) (*testNode, api.BlobRef) {
	t.Helper()
	sto := new(memory.Storage)
	tn := newTestNode(t, func(*fake.Perkeep) api.BlobPutter { return memPutter{sto} })
	tn.fetcher = func() (blob.Fetcher, error) { return sto, nil }
	pn := tn.keep(t)
	tn.stopKeepers()
	return tn, pn
}

func vv(pairs ...any) api.Version {
	var v api.Version
	for i := 0; i < len(pairs); i += 2 {
		v = append(v, api.VersionCounter{ID: pairs[i].(string), Value: uint64(pairs[i+1].(int))})
	}
	return v
}

func (tn *testNode) folderID(t *testing.T) string {
	t.Helper()
	lf, err := tn.localFolder("docs")
	if err != nil {
		t.Fatal(err)
	}
	return lf.ID
}

func (tn *testNode) base(t *testing.T, mine, theirs string) (string, bool) {
	t.Helper()
	orig := filepath.Join(tn.dir, "a.txt")
	cp := filepath.Join(tn.dir, copyName)
	os.WriteFile(orig, []byte(mine), 0o644)
	os.WriteFile(cp, []byte(theirs), 0o644)
	_, archive := tn.folderKeep("docs")
	if archive == "" {
		k, _ := tn.folderKeep("docs")
		archive = k.Permanode
	}
	dir := t.TempDir()
	path, exact, err := tn.conflictBase(tn.ctx, tn.folderID(t), archive, "a.txt", orig, cp, dir)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		return "", exact
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b), exact
}

func TestConflictBaseSkipsVersionEqualToOneSide(t *testing.T) {
	tn, pn := newBaseTestNode(t)
	putVersion(t, tn, pn, "a.txt", "v1", vv("AAAAAAA", 1))
	// Mine was kept after the copies diverged: the last kept version is
	// one side, not the base.
	putVersion(t, tn, pn, "a.txt", "mine", vv("AAAAAAA", 2))
	tn.sy.(*fake.Syncer).SetFileState(tn.folderID(t), "a.txt", api.FileState{Known: true, Current: true, Version: vv("AAAAAAA", 2)})
	got, exact := tn.base(t, "mine", "theirs")
	if got != "v1" || !exact {
		t.Fatalf("base = %q exact=%v, want v1 exact", got, exact)
	}
}

func TestConflictBaseSkipsEqualToCopy(t *testing.T) {
	tn, pn := newBaseTestNode(t)
	putVersion(t, tn, pn, "a.txt", "v1", vv("AAAAAAA", 1))
	putVersion(t, tn, pn, "a.txt", "theirs", vv("AAAAAAA", 1, "BBBBBBB", 1))
	// W descends from both kept versions; the newer one is the copy.
	tn.sy.(*fake.Syncer).SetFileState(tn.folderID(t), "a.txt", api.FileState{Known: true, Current: true, Version: vv("AAAAAAA", 2, "BBBBBBB", 1)})
	got, exact := tn.base(t, "mine", "theirs")
	if got != "v1" || !exact {
		t.Fatalf("base = %q exact=%v, want v1 exact", got, exact)
	}
}

func TestConflictBaseNotAncestorFallsBack(t *testing.T) {
	tn, pn := newBaseTestNode(t)
	// The only kept version is concurrent with the original's version, so
	// it is not a known ancestor: fall back to it as a guess.
	putVersion(t, tn, pn, "a.txt", "v1", vv("BBBBBBB", 5))
	tn.sy.(*fake.Syncer).SetFileState(tn.folderID(t), "a.txt", api.FileState{Known: true, Current: true, Version: vv("AAAAAAA", 1)})
	got, exact := tn.base(t, "mine", "theirs")
	if got != "v1" || exact {
		t.Fatalf("base = %q exact=%v, want v1 not exact", got, exact)
	}
}

// TestConflictBaseFallbackEqualToSideIsNone: a guessed base equal to
// either side would make a merge drop that side's edit (issue #4), so there
// is no base instead.
func TestConflictBaseFallbackEqualToSideIsNone(t *testing.T) {
	for _, side := range []string{"mine", "theirs"} {
		t.Run(side, func(t *testing.T) {
			tn, pn := newBaseTestNode(t)
			putVersion(t, tn, pn, "a.txt", side, vv("BBBBBBB", 5))
			tn.sy.(*fake.Syncer).SetFileState(tn.folderID(t), "a.txt", api.FileState{Known: true, Current: true, Version: vv("AAAAAAA", 1)})
			got, exact := tn.base(t, "mine", "theirs")
			if got != "" || exact {
				t.Fatalf("base = %q exact=%v, want none", got, exact)
			}
		})
	}
}

func TestConflictBaseUnversionedNotExact(t *testing.T) {
	tn, pn := newBaseTestNode(t)
	putVersion(t, tn, pn, "a.txt", "v1", nil)
	tn.sy.(*fake.Syncer).SetFileState(tn.folderID(t), "a.txt", api.FileState{Known: true, Current: true, Version: vv("AAAAAAA", 3)})
	got, exact := tn.base(t, "mine", "theirs")
	if got != "v1" || exact {
		t.Fatalf("base = %q exact=%v, want v1 not exact", got, exact)
	}
}

func TestConflictBaseNone(t *testing.T) {
	tn, _ := newBaseTestNode(t)
	got, exact := tn.base(t, "mine", "theirs")
	if got != "" || exact {
		t.Fatalf("base = %q exact=%v, want none", got, exact)
	}
}

func TestConflictDuplicateCopiesRunHookOnce(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	tn.sync()
	runs := filepath.Join(t.TempDir(), "runs")
	tn.cfg.ConflictHook = hookScript(t, `echo "$(basename "$KEEPSYNC_CONFLICT")" >> `+runs+`
cat "$KEEPSYNC_CONFLICT" >> "$KEEPSYNC_ORIGINAL"
rm "$KEEPSYNC_CONFLICT"
`)
	orig := filepath.Join(tn.dir, "a.txt")
	os.WriteFile(orig, []byte("mine\n"), 0o644)
	// Two nodes made the same copy in different seconds.
	c1 := filepath.Join(tn.dir, "a.sync-conflict-20260101-120000-OTHERDV.txt")
	c2 := filepath.Join(tn.dir, "a.sync-conflict-20260101-120001-OTHERDV.txt")
	os.WriteFile(c1, []byte("theirs\n"), 0o644)
	os.WriteFile(c2, []byte("theirs\n"), 0o644)

	// The later name is the duplicate while both are present.
	if ci := tn.runConflict(tn.ctx, "docs", c2, false); ci.State != HookResolved || !strings.Contains(ci.Reason, "duplicate") {
		t.Fatalf("c2: %+v", ci)
	}
	if _, err := os.Stat(c2); !os.IsNotExist(err) {
		t.Fatalf("duplicate c2 still there: %v", err)
	}
	if ci := tn.runConflict(tn.ctx, "docs", c1, false); ci.State != HookResolved {
		t.Fatalf("c1: %+v", ci)
	}
	// A copy that arrives after the hook resolved an identical one is a
	// duplicate too.
	c3 := filepath.Join(tn.dir, "a.sync-conflict-20260101-120005-OTHERDV.txt")
	os.WriteFile(c3, []byte("theirs\n"), 0o644)
	if ci := tn.runConflict(tn.ctx, "docs", c3, false); ci.State != HookResolved || !strings.Contains(ci.Reason, "duplicate") {
		t.Fatalf("c3: %+v", ci)
	}
	// A copy with other content still runs the hook.
	c4 := filepath.Join(tn.dir, "a.sync-conflict-20260101-120009-OTHERDV.txt")
	os.WriteFile(c4, []byte("other\n"), 0o644)
	if ci := tn.runConflict(tn.ctx, "docs", c4, false); ci.State != HookResolved || ci.Reason != "" {
		t.Fatalf("c4: %+v", ci)
	}
	if b, _ := os.ReadFile(runs); string(b) != filepath.Base(c1)+"\n"+filepath.Base(c4)+"\n" {
		t.Fatalf("hook runs %q", b)
	}
	if b, _ := os.ReadFile(orig); string(b) != "mine\ntheirs\nother\n" {
		t.Fatalf("original %q", b)
	}
}

func TestConflictNodeFromCopyName(t *testing.T) {
	tn := newTestNodeOpts(t, func(pk *fake.Perkeep) api.BlobPutter { return pk }, true)
	tn.sync()
	if got := tn.conflictNode("docs", filepath.Join(tn.dir, copyName)); got != "other" {
		t.Fatalf("conflictNode = %q, want other (named in the copy)", got)
	}
	if got := tn.conflictNode("docs", filepath.Join(tn.dir, "a.sync-conflict-20260101-120000-ZZZZZZZ.txt")); got != "ZZZZZZZ" {
		t.Fatalf("unknown device: %q", got)
	}
}
