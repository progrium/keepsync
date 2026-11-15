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
