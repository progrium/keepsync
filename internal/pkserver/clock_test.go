package pkserver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// TestClaimAfterClockWentBack checks that a server whose clock is behind
// the newest claim on a file (after a restart or a recovery on another
// machine) still dates its next claim after that one, so the new version
// wins when the tree is read back from the index.
func TestClaimAfterClockWentBack(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	files := t.TempDir()
	pn, err := e.srv.NewPermanode(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	put := func(name, body string, mt time.Time) api.BlobRef {
		ref, err := e.putter.PutFile(ctx, writeFile(t, files, name, body, mt))
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	claim := func(path string, ref api.BlobRef, mt time.Time) {
		t.Helper()
		if r, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: path, File: ref, ModTime: mt}); err != nil || r != api.ClaimWritten {
			t.Fatalf("claim %s: %v %v", path, r, err)
		}
	}
	claim("a/x", put("v1", "one", t0), t0)

	// A restart forgets the last claim date and the cached trees; the clock
	// is now a day behind the claims above.
	restart := func() {
		e.srv.claimMu.Lock()
		e.srv.lastClaim = time.Time{}
		e.srv.claimMu.Unlock()
		e.srv.treeMu.Lock()
		clear(e.srv.trees)
		e.srv.treeMu.Unlock()
	}
	restart()
	e.srv.now = func() time.Time { return time.Now().Add(-24 * time.Hour) }

	v2 := put("v2", "two", t0.Add(time.Hour))
	claim("a/x", v2, t0.Add(time.Hour)) // a changed file: a claim on its permanode
	y := put("y", "why", t0)
	claim("a/y", y, t0) // a new file: a claim on its directory's permanode

	restart()
	got, err := e.srv.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	if got["a/x"].File != v2 || !got["a/x"].ModTime.Equal(t0.Add(time.Hour)) {
		t.Errorf("a/x after reload: %+v, want %s", got["a/x"], v2)
	}
	if got["a/y"].File != y {
		t.Errorf("a/y after reload: %+v, want %s", got["a/y"], y)
	}
}

// TestModeKept checks that a file's permission bits are kept with it,
// survive a reload from the index, and that a chmod alone is kept.
func TestModeKept(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	pn, err := e.srv.NewPermanode(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	ref, err := e.putter.PutFile(ctx, writeFile(t, t.TempDir(), "run.sh", "#!/bin/sh\n", t0))
	if err != nil {
		t.Fatal(err)
	}
	claim := func(mode os.FileMode, mt time.Time) api.ClaimResult {
		t.Helper()
		r, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "bin/run.sh", File: ref, ModTime: mt, Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	reload := func() api.PathEntry {
		e.srv.treeMu.Lock()
		clear(e.srv.trees)
		e.srv.treeMu.Unlock()
		got, err := e.srv.Paths(ctx, pn)
		if err != nil {
			t.Fatal(err)
		}
		return got["bin/run.sh"]
	}
	claim(0o644, t0)
	if got := reload(); got.Mode != 0o644 {
		t.Fatalf("mode after reload: %v", got.Mode)
	}
	if r := claim(0o755, t0.Add(time.Minute)); r != api.ClaimWritten {
		t.Fatalf("chmod: %v", r)
	}
	if got := reload(); got.Mode != 0o755 || got.File != ref {
		t.Fatalf("after chmod: %+v", got)
	}
	if r := claim(0o755, t0.Add(time.Minute)); r != api.ClaimUnchanged {
		t.Fatalf("same mode again: %v", r)
	}
}
