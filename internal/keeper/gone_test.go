package keeper

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

func ver(pairs ...any) api.Version {
	var v api.Version
	for i := 0; i < len(pairs); i += 2 {
		v = append(v, api.VersionCounter{ID: pairs[i].(string), Value: uint64(pairs[i+1].(int))})
	}
	return v
}

func (e *env) state(rel string, st api.FileState) { e.scans.SetFileState("fake-docs", rel, st) }

func current(v api.Version) api.FileState {
	return api.FileState{Known: true, Current: true, Version: v}
}

func deleted() api.FileState { return api.FileState{Known: true, Current: true, Deleted: true} }

func (e *env) remove(rel string) {
	e.t.Helper()
	if err := os.Remove(filepath.Join(e.dir, filepath.FromSlash(rel))); err != nil {
		e.t.Fatal(err)
	}
}

func TestNotCurrentIsLagging(t *testing.T) {
	for name, st := range map[string]api.FileState{
		"needs update": {Known: true, Current: false, Version: ver("A", 2)},
		"not indexed":  {Known: false},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.write("a.txt", "stale", 3*day)
			e.state("a.txt", st)
			rep := e.pass()
			if !slices.Equal(rep.Lagging, []string{"a.txt"}) || e.pk.Claims != 0 {
				t.Fatalf("rep %+v claims %d", rep, e.pk.Claims)
			}
			s, err := e.k.Status(e.ctx)
			if err != nil || s.Counts()[StateLagging] != 1 {
				t.Fatalf("status %+v err %v", s, err)
			}
		})
	}
}

func TestClaimCarriesVersion(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 3*day)
	e.state("a.txt", current(ver("A", 1)))
	e.pass()
	if got := e.tree()["a.txt"].Version; got.Compare(ver("A", 1)) != api.Equal {
		t.Fatalf("kept version %v", got)
	}
}

func TestRecreatedOlderMtimeNewerVersionIsKept(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "first", 10*day)
	e.state("a.txt", current(ver("A", 1)))
	e.pass()
	// Recreated (say, restored from a backup) with an older mtime, but
	// Syncthing gives it a newer version.
	e.write("a.txt", "second", 20*day)
	e.state("a.txt", current(ver("A", 1, "B", 1)))
	rep := e.pass()
	if !slices.Contains(rep.Kept, "a.txt") || len(rep.Lagging) != 0 {
		t.Fatalf("rep %+v", rep)
	}
	ref, _ := e.pk.PutFile(e.ctx, filepath.Join(e.dir, "a.txt"))
	if e.tree()["a.txt"].File != ref {
		t.Fatal("tree does not hold the recreated file")
	}
}

func TestOlderVersionIsLaggingEvenWithNewerMtime(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "new", 10*day)
	e.state("a.txt", current(ver("A", 2)))
	e.pass()
	e.write("a.txt", "old", 3*day)
	e.state("a.txt", current(ver("A", 1)))
	if rep := e.pass(); !slices.Equal(rep.Lagging, []string{"a.txt"}) || e.pk.Claims != 1 {
		t.Fatalf("rep %+v claims %d", rep, e.pk.Claims)
	}
}

func TestSameVersionIgnoresMtime(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 10*day)
	e.state("a.txt", current(ver("A", 1)))
	e.pass()
	e.write("a.txt", "a", 5*day) // touched; Syncthing kept the version
	rep := e.pass()
	if len(rep.Kept)+len(rep.Lagging)+len(rep.InGrace) != 0 || e.pk.Claims != 1 {
		t.Fatalf("rep %+v claims %d", rep, e.pk.Claims)
	}
}

func TestCoarseMtimeIsSameVersion(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	fine := e.clock.Now().Add(-40 * day).Add(700 * time.Millisecond)
	abs := filepath.Join(e.dir, "a.txt")
	os.Chtimes(abs, fine, fine)
	e.pass()
	// The same file seen through a filesystem with whole-second mtimes.
	coarse := fine.Truncate(time.Second)
	os.Chtimes(abs, coarse, coarse)
	rep := e.pass()
	if len(rep.Kept)+len(rep.Lagging) != 0 || e.pk.Claims != 1 {
		t.Fatalf("rep %+v claims %d", rep, e.pk.Claims)
	}
	if s, _ := e.k.Status(e.ctx); s.Files[0].State != StateKept {
		t.Fatalf("status %v", s.Files[0].State)
	}
	e.pk.MarkDurable()
	if rep = e.pass(); !slices.Equal(rep.Expired, []string{"a.txt"}) {
		t.Fatalf("expired %v", rep.Expired)
	}
}

func TestExpiryMarksExpired(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.pk.MarkDurable()
	rep := e.pass()
	if !slices.Equal(rep.Expired, []string{"a.txt"}) || rep.Gone["a.txt"] != api.GoneExpired {
		t.Fatalf("rep %+v", rep)
	}
	if g := e.tree()["a.txt"].Gone; g != api.GoneExpired {
		t.Fatalf("tree Gone = %q", g)
	}
	// Next pass: Syncthing now reports the delete; nothing more is marked.
	e.state("a.txt", deleted())
	if rep = e.pass(); len(rep.Gone) != 0 {
		t.Fatalf("re-marked: %v", rep.Gone)
	}
}

func TestUserDeleteMarksDeleted(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 3*day)
	e.pass()
	e.remove("a.txt")
	e.state("a.txt", deleted())
	rep := e.pass()
	if rep.Gone["a.txt"] != api.GoneDeleted || e.tree()["a.txt"].Gone != api.GoneDeleted {
		t.Fatalf("rep %+v tree %+v", rep, e.tree()["a.txt"])
	}
}

func TestDeleteAfterExpireWindowCountsExpired(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	e.pass() // kept, and the next pass sees its claims
	e.pk.MarkDurable()
	// Another keeper expired it, and the delete synced here before this
	// node's own pass got to it.
	e.remove("a.txt")
	e.state("a.txt", deleted())
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneExpired {
		t.Fatalf("rep %+v", rep)
	}
}

func TestHandDeleteBeforeFlushPastWindowCountsDeleted(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.pass() // claims known; still not durable, so nobody can expire it
	e.remove("a.txt")
	e.state("a.txt", deleted())
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneDeleted {
		t.Fatalf("rep %+v", rep)
	}
	if g := e.tree()["a.txt"].Gone; g != api.GoneDeleted {
		t.Fatalf("tree Gone = %q", g)
	}
}

func TestPastWindowDeleteWaitsWhenDurabilityUnknown(t *testing.T) {
	e := newEnv(t)
	down := &downRegistry{RegistryClient: e.client}
	e.k = e.newKeeper("nodekey:a", down)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.remove("a.txt")
	e.state("a.txt", deleted())
	down.durableErr = true
	if rep := e.pass(); len(rep.Gone) != 0 {
		t.Fatalf("marked with durability unknown: %v", rep.Gone)
	}
	down.durableErr = false
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneDeleted {
		t.Fatalf("rep %+v", rep)
	}
}

type failMark struct {
	api.RegistryClient
	fail bool
}

func (f *failMark) MarkGone(ctx context.Context, g api.GoneClaim) error {
	if f.fail {
		return errDown
	}
	return f.RegistryClient.MarkGone(ctx, g)
}

func TestFailedExpiredMarkIsRetried(t *testing.T) {
	e := newEnv(t)
	fm := &failMark{RegistryClient: e.client}
	e.k = e.newKeeper("nodekey:a", fm)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.pk.MarkDurable()
	fm.fail = true
	rep, _ := e.k.Pass(e.ctx)
	if !slices.Equal(rep.Expired, []string{"a.txt"}) || len(rep.Gone) != 0 {
		t.Fatalf("rep %+v", rep)
	}
	fm.fail = false
	e.state("a.txt", deleted())
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneExpired {
		t.Fatalf("retry rep %+v", rep)
	}
}

func TestUnknownStateMarksNothing(t *testing.T) {
	for name, st := range map[string]*api.FileState{
		"default (not deleted)":  nil,
		"not in index":           {Known: false},
		"delete not yet current": {Known: true, Current: false, Deleted: true},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.write("a.txt", "a", 3*day)
			e.pass()
			e.remove("a.txt")
			if st != nil {
				e.state("a.txt", *st)
			}
			if rep := e.pass(); len(rep.Gone) != 0 || e.tree()["a.txt"].Gone != api.GoneNone {
				t.Fatalf("marked: %+v", rep.Gone)
			}
		})
	}
}

func TestNoMarksWithoutSyncerOrWhenExcluded(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 3*day)
	e.write("b.txt", "b", 3*day)
	e.pass()
	// b.txt becomes excluded but is still on disk.
	os.WriteFile(filepath.Join(e.dir, KeepIgnoreFile), []byte("b.txt\n"), 0o644)
	e.state("b.txt", deleted())
	e.remove("a.txt")
	e.state("a.txt", deleted())

	k, err := New(Config{Folder: "docs", FolderID: "fake-docs", Dir: e.dir, Self: "nodekey:a", Policy: e.policy,
		Registry: e.client, Putter: e.pk, Clock: e.clock})
	if err != nil {
		t.Fatal(err)
	}
	if rep, _ := k.Pass(e.ctx); len(rep.Gone) != 0 {
		t.Fatalf("marked without a Syncer: %v", rep.Gone)
	}
	notKeeper := e.newKeeper("nodekey:z", e.reg.Client("nodekey:z"))
	if rep, _ := notKeeper.Pass(e.ctx); len(rep.Gone) != 0 {
		t.Fatalf("non-keeper marked: %v", rep.Gone)
	}
	rep := e.pass()
	if len(rep.Gone) != 1 || rep.Gone["a.txt"] != api.GoneDeleted {
		t.Fatalf("gone %v", rep.Gone)
	}
}

func TestRestoreClearsGone(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	e.state("a.txt", current(ver("A", 1)))
	e.pass()
	e.pk.MarkDurable()
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneExpired {
		t.Fatalf("not expired: %+v", rep)
	}
	// Restored with the same content and mtime; Syncthing bumps the version.
	e.write("a.txt", "a", 40*day)
	e.state("a.txt", current(ver("A", 2)))
	e.pk.MarkDurable()
	p := e.policy
	p.Expire = 0 // keep it around so the mark's state is visible
	e.k.SetPolicy(p)
	rep := e.pass()
	if !slices.Contains(rep.Kept, "a.txt") {
		t.Fatalf("restored file not reclaimed: %+v", rep)
	}
	ent := e.tree()["a.txt"]
	if ent.Gone != api.GoneNone {
		t.Fatalf("Gone = %q after restore", ent.Gone)
	}
	claims := e.pk.Claims
	if rep = e.pass(); len(rep.Kept) != 0 || e.pk.Claims != claims {
		t.Fatalf("reclaimed again: %+v", rep)
	}
}

func TestRestoreInsideGraceIsReclaimedNow(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 3*day)
	e.pass()
	e.remove("a.txt")
	e.state("a.txt", deleted())
	e.pass()
	// Restored right away with the kept mtime and content; mtime matches.
	e.write("a.txt", "a", 3*day)
	e.state("a.txt", api.FileState{Known: true, Current: true})
	e.pass()
	if g := e.tree()["a.txt"].Gone; g != api.GoneNone {
		t.Fatalf("Gone = %q", g)
	}
}

// A past-window file deleted before its kept version was durable was not
// expired by any keeper (expiry needs durability), so it was deleted.
func TestDeleteAfterExpireWindowNotDurableCountsDeleted(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.remove("a.txt")
	e.state("a.txt", deleted())
	if rep := e.pass(); rep.Gone["a.txt"] != api.GoneDeleted {
		t.Fatalf("rep %+v", rep)
	}
}
