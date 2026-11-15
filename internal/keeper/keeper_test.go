package keeper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

const (
	hour = time.Hour
	day  = 24 * time.Hour
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type env struct {
	t      *testing.T
	ctx    context.Context
	clock  *fake.Clock
	pk     *fake.Perkeep
	reg    *fake.Registry
	client api.RegistryClient
	dir    string
	policy api.KeepPolicy
	k      *Keeper
	scans  *fake.Syncer
}

// newEnv makes folder "docs" with node a as member and keeper, kept with
// grace 48h, max-defer 7d and expire 30d.
func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, ctx: context.Background(), clock: fake.NewClock(t0), pk: fake.NewPerkeep(), dir: t.TempDir()}
	e.reg = fake.NewRegistry(e.pk)
	e.client = e.reg.Client("nodekey:a")
	if _, err := e.client.AddFolder(e.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	pn, err := Keep(e.ctx, e.client, "docs", e.dir, api.KeepPolicy{Grace: 48 * hour, MaxDefer: 7 * day, Expire: 30 * day})
	if err != nil {
		t.Fatal(err)
	}
	e.policy = *e.reg.Membership().Folders[0].Keep
	if e.policy.Permanode != pn {
		t.Fatalf("permanode %s != %s", e.policy.Permanode, pn)
	}
	e.scans = fake.NewSyncer("dev-a")
	e.k = e.newKeeper("nodekey:a", e.client)
	return e
}

func (e *env) newKeeper(self api.NodeKey, reg api.RegistryClient) *Keeper {
	e.t.Helper()
	k, err := New(Config{
		Folder: "docs", FolderID: "fake-docs", Dir: e.dir, Self: self, Policy: e.policy,
		Registry: reg, Putter: e.pk, Clock: e.clock, Syncer: e.scans,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return k
}

// write creates rel with content, last modified age before the clock.
func (e *env) write(rel, content string, age time.Duration) {
	e.t.Helper()
	abs := filepath.Join(e.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		e.t.Fatal(err)
	}
	mt := e.clock.Now().Add(-age)
	if err := os.Chtimes(abs, mt, mt); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(e.dir, filepath.FromSlash(rel)))
	return err == nil
}

func (e *env) pass() Report {
	e.t.Helper()
	rep, err := e.k.Pass(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, err := range rep.Errors {
		e.t.Errorf("pass error: %v", err)
	}
	return rep
}

func (e *env) tree() map[string]api.PathEntry {
	e.t.Helper()
	tr, err := e.pk.Paths(e.ctx, e.policy.Permanode)
	if err != nil {
		e.t.Fatal(err)
	}
	return tr
}

func TestValidatePolicy(t *testing.T) {
	pn := api.BlobRef("sha224-x")
	tests := []struct {
		name string
		p    api.KeepPolicy
		ok   bool
	}{
		{"defaults never expire", api.KeepPolicy{Permanode: pn, Grace: DefaultGrace, MaxDefer: DefaultMaxDefer}, true},
		{"ordered", api.KeepPolicy{Permanode: pn, Grace: hour, MaxDefer: day, Expire: 30 * day}, true},
		{"zero grace", api.KeepPolicy{Permanode: pn, Grace: 0, MaxDefer: day}, true},
		{"grace equals max-defer", api.KeepPolicy{Permanode: pn, Grace: day, MaxDefer: day}, false},
		{"grace over max-defer", api.KeepPolicy{Permanode: pn, Grace: 2 * day, MaxDefer: day}, false},
		{"max-defer equals expire", api.KeepPolicy{Permanode: pn, Grace: hour, MaxDefer: day, Expire: day}, false},
		{"expire under grace", api.KeepPolicy{Permanode: pn, Grace: 2 * day, MaxDefer: 3 * day, Expire: day}, false},
		{"negative", api.KeepPolicy{Permanode: pn, Grace: -hour, MaxDefer: day}, false},
		{"no permanode", api.KeepPolicy{Grace: hour, MaxDefer: day}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePolicy(tt.p); (err == nil) != tt.ok {
				t.Fatalf("ValidatePolicy = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestGrace(t *testing.T) {
	tests := []struct {
		name string
		age  time.Duration
		kept bool
	}{
		{"just written", 0, false},
		{"inside grace", 47 * hour, false},
		{"exactly grace", 48 * hour, true},
		{"past grace", 72 * hour, true},
		{"future mtime", -hour, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.write("a.txt", "hello", tt.age)
			rep := e.pass()
			_, inTree := e.tree()["a.txt"]
			if inTree != tt.kept || slices.Contains(rep.Kept, "a.txt") != tt.kept {
				t.Fatalf("kept=%v inTree=%v, want %v (report %+v)", rep.Kept, inTree, tt.kept, rep)
			}
			if slices.Contains(rep.InGrace, "a.txt") == tt.kept {
				t.Fatalf("InGrace = %v", rep.InGrace)
			}
		})
	}
}

func TestEditInsideGraceNeverReachesArchive(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "draft", time.Hour)
	e.pass()
	e.clock.Advance(24 * hour)
	e.write("a.txt", "final", 0)
	e.pass()
	e.clock.Advance(48 * hour)
	e.pass()
	tr := e.tree()
	ref, _ := e.pk.PutFile(e.ctx, filepath.Join(e.dir, "a.txt"))
	if tr["a.txt"].File != ref || e.pk.Claims != 1 {
		t.Fatalf("tree %+v claims %d; want only the final version", tr, e.pk.Claims)
	}
}

func TestMaxDefer(t *testing.T) {
	e := newEnv(t)
	// The file is rewritten every hour, so it never passes 48h of grace.
	var kepts []time.Time
	for h := 0; h <= 16*24; h++ {
		e.write("busy.log", "v"+time.Duration(h).String(), 0)
		rep := e.pass()
		if slices.Contains(rep.Kept, "busy.log") {
			if !slices.Contains(rep.Forced, "busy.log") {
				t.Fatalf("kept at hour %d but not by max-defer", h)
			}
			kepts = append(kepts, e.clock.Now())
		}
		e.clock.Advance(hour)
	}
	// Kept at the cap, and again each time it hits the cap.
	want := []time.Time{t0.Add(7 * day), t0.Add(14*day + hour)}
	if !slices.EqualFunc(kepts, want, time.Time.Equal) {
		t.Fatalf("kept at %v, want %v", kepts, want)
	}
}

func TestMissingFileIsNeverDeleted(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 3*day)
	e.write("dir/b.txt", "b", 3*day)
	e.pass()
	if err := os.Remove(filepath.Join(e.dir, "a.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(e.dir, "dir")); err != nil {
		t.Fatal(err)
	}
	claims := e.pk.Claims
	e.clock.Advance(60 * day)
	e.pass()
	tr := e.tree()
	if _, ok := tr["a.txt"]; !ok {
		t.Fatal("a.txt dropped from tree")
	}
	if _, ok := tr["dir/b.txt"]; !ok {
		t.Fatal("dir/b.txt dropped from tree")
	}
	if e.pk.Claims != claims {
		t.Fatalf("claims %d -> %d after delete", claims, e.pk.Claims)
	}
}

func TestExpiryBlockedUntilDurable(t *testing.T) {
	e := newEnv(t)
	e.write("old.txt", "old", 31*day)
	e.write("recent.txt", "recent", 10*day)

	rep := e.pass()
	if !slices.Contains(rep.Kept, "old.txt") {
		t.Fatalf("old.txt not kept: %+v", rep)
	}
	if len(rep.Expired) != 0 || !e.exists("old.txt") {
		t.Fatalf("expired before durable: %+v", rep)
	}

	e.pass()
	if !e.exists("old.txt") {
		t.Fatal("expired before durable on second pass")
	}

	e.pk.MarkDurable()
	rep = e.pass()
	if !slices.Equal(rep.Expired, []string{"old.txt"}) || e.exists("old.txt") {
		t.Fatalf("expired %v, old exists %v", rep.Expired, e.exists("old.txt"))
	}
	if !e.exists("recent.txt") {
		t.Fatal("recent.txt expired early")
	}
	if !slices.Contains(e.scans.Scans, "fake-docs/old.txt") {
		t.Fatalf("no scan after expiry: %v", e.scans.Scans)
	}
	if _, ok := e.tree()["old.txt"]; !ok {
		t.Fatal("expired file left the archive")
	}
	if !e.exists(MappingFile) {
		t.Fatal(".keepsync expired")
	}

	e.clock.Advance(21 * day)
	rep = e.pass()
	if !slices.Equal(rep.Expired, []string{"recent.txt"}) {
		t.Fatalf("expired %v", rep.Expired)
	}
}

func TestExpiryNeedsCurrentVersionDurable(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "v1", 40*day)
	e.pass()
	e.pk.MarkDurable()
	// A newer version arrives, already past expire by mtime. It is kept in
	// this pass, but its blob is not durable yet, so it must stay.
	e.write("a.txt", "v2", 35*day)
	rep := e.pass()
	if !slices.Contains(rep.Kept, "a.txt") || len(rep.Expired) != 0 || !e.exists("a.txt") {
		t.Fatalf("report %+v", rep)
	}
	e.pk.MarkDurable()
	if rep = e.pass(); !slices.Equal(rep.Expired, []string{"a.txt"}) {
		t.Fatalf("expired %v", rep.Expired)
	}
}

// claimsNotDurable reports content blobs as the server does but every
// claim blob as not yet in the bucket.
type claimsNotDurable struct {
	api.RegistryClient
	claims map[api.BlobRef]bool
}

func (c *claimsNotDurable) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	ok, err := c.RegistryClient.Durable(ctx, refs)
	for i, r := range refs {
		if c.claims[r] {
			ok[i] = false
		}
	}
	return ok, err
}

func TestExpiryNeedsClaimDurable(t *testing.T) {
	e := newEnv(t)
	cnd := &claimsNotDurable{RegistryClient: e.client, claims: map[api.BlobRef]bool{}}
	e.k = e.newKeeper("nodekey:a", cnd)
	e.write("a.txt", "a", 40*day)
	e.pass()
	ent := e.tree()["a.txt"]
	if len(ent.Claims) == 0 {
		t.Fatal("fake left Claims empty")
	}
	cnd.claims[ent.Claims[0]] = true
	e.pk.MarkDurable()
	if ok, _ := cnd.Durable(e.ctx, []api.BlobRef{ent.File}); !ok[0] {
		t.Fatal("content should be durable")
	}
	rep := e.pass()
	if len(rep.Expired) != 0 || !e.exists("a.txt") {
		t.Fatalf("expired with claim not durable: %+v", rep)
	}
	if st, _ := e.k.Status(e.ctx); st.Counts()[StateExpirable] != 0 {
		t.Fatal("status reports expirable with claim not durable")
	}
	delete(cnd.claims, ent.Claims[0])
	if rep = e.pass(); !slices.Equal(rep.Expired, []string{"a.txt"}) {
		t.Fatalf("expired %v after claim durable", rep.Expired)
	}
}

func TestNeverExpire(t *testing.T) {
	e := newEnv(t)
	p := e.policy
	p.Expire = 0
	if err := e.k.SetPolicy(p); err != nil {
		t.Fatal(err)
	}
	e.write("a.txt", "a", 400*day)
	e.pass()
	e.pk.MarkDurable()
	e.pass()
	if !e.exists("a.txt") {
		t.Fatal("expired with Expire=0")
	}
}

type downRegistry struct {
	api.RegistryClient
	pathsErr, durableErr bool
}

var errDown = errors.New("server unreachable")

func (d *downRegistry) Paths(ctx context.Context, pn api.BlobRef) (map[string]api.PathEntry, error) {
	if d.pathsErr {
		return nil, errDown
	}
	return d.RegistryClient.Paths(ctx, pn)
}

func (d *downRegistry) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	if d.durableErr {
		return nil, errDown
	}
	return d.RegistryClient.Durable(ctx, refs)
}

func TestServerUnreachablePausesExpiry(t *testing.T) {
	e := newEnv(t)
	down := &downRegistry{RegistryClient: e.client}
	e.k = e.newKeeper("nodekey:a", down)
	e.write("a.txt", "a", 40*day)
	e.pass()
	e.pk.MarkDurable()

	down.durableErr = true
	rep := e.pass()
	if !rep.ExpiryPaused || !e.exists("a.txt") {
		t.Fatalf("report %+v exists %v", rep, e.exists("a.txt"))
	}

	down.pathsErr = true
	if _, err := e.k.Pass(e.ctx); !errors.Is(err, errDown) {
		t.Fatalf("Pass err = %v", err)
	}
	if !e.exists("a.txt") {
		t.Fatal("expired while server down")
	}

	down.pathsErr, down.durableErr = false, false
	if rep = e.pass(); !slices.Equal(rep.Expired, []string{"a.txt"}) {
		t.Fatalf("after recovery expired %v", rep.Expired)
	}
}

func TestExcludes(t *testing.T) {
	e := newEnv(t)
	old := 3 * day
	os.WriteFile(filepath.Join(e.dir, StIgnoreFile), []byte("// comment\n!keep.o\n*.o\n/build\n(?i)SECRET*\n"), 0o644)
	os.WriteFile(filepath.Join(e.dir, KeepIgnoreFile), []byte("scratch/\n"), 0o644)
	for _, rel := range []string{
		"notes.txt", "src/main.c", "keep.o", "report.sync-conflict-20260101-000000-ABCDEFG.txt",
		"src/main.o", "build/out.bin", "secret.key", "scratch/tmp.txt",
		".stversions/notes~20260101.txt", ".stfolder/marker", "src/.syncthing.main.c.tmp",
	} {
		e.write(rel, rel, old)
	}
	// Make the ignore files and mapping old too, so only exclusion saves them.
	for _, f := range []string{StIgnoreFile, KeepIgnoreFile, MappingFile} {
		mt := e.clock.Now().Add(-old)
		os.Chtimes(filepath.Join(e.dir, f), mt, mt)
	}
	rep := e.pass()
	want := []string{"keep.o", "notes.txt", "report.sync-conflict-20260101-000000-ABCDEFG.txt", "src/main.c"}
	if !slices.Equal(rep.Kept, want) {
		t.Fatalf("kept %v, want %v", rep.Kept, want)
	}
}

func TestNotAKeeper(t *testing.T) {
	e := newEnv(t)
	k := e.newKeeper("nodekey:z", e.reg.Client("nodekey:z"))
	e.write("a.txt", "a", 3*day)
	rep, err := k.Pass(e.ctx)
	if err != nil || !rep.Skipped || e.pk.Claims != 0 {
		t.Fatalf("rep %+v err %v claims %d", rep, err, e.pk.Claims)
	}
}

func TestTwoKeepersClaimOnce(t *testing.T) {
	e := newEnv(t)
	p := e.policy
	p.Keepers = []api.NodeKey{"nodekey:a", "nodekey:b"}
	e.policy = p
	ka := e.newKeeper("nodekey:a", e.client)
	kb := e.newKeeper("nodekey:b", e.reg.Client("nodekey:b"))
	e.write("a.txt", "a", 3*day)
	for _, k := range []*Keeper{ka, kb, ka, kb} {
		if _, err := k.Pass(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if e.pk.Claims != 1 {
		t.Fatalf("claims = %d", e.pk.Claims)
	}
}

func TestLaggingCopyIsNotClaimed(t *testing.T) {
	e := newEnv(t)
	ref, _ := e.pk.PutFile(e.ctx, filepath.Join(e.dir, MappingFile)) // any blob
	newer := e.clock.Now().Add(-3 * day)
	e.pk.ClaimPath(e.ctx, api.PathClaim{Permanode: e.policy.Permanode, Path: "a.txt", File: ref, ModTime: newer})
	e.write("a.txt", "older", 5*day)
	rep := e.pass()
	if !slices.Equal(rep.Lagging, []string{"a.txt"}) || e.pk.Claims != 1 {
		t.Fatalf("rep %+v claims %d", rep, e.pk.Claims)
	}
	e.pk.MarkDurable()
	e.clock.Advance(40 * day)
	if rep = e.pass(); len(rep.Expired) != 0 {
		t.Fatalf("lagging copy expired: %+v", rep)
	}
}

func TestMappingMismatchRefuses(t *testing.T) {
	e := newEnv(t)
	if err := WriteMapping(e.dir, Mapping{Folder: "docs", Permanode: "sha224-other"}); err != nil {
		t.Fatal(err)
	}
	e.write("a.txt", "a", 3*day)
	if _, err := e.k.Pass(e.ctx); err == nil {
		t.Fatal("pass ran with a mismatched .keepsync")
	}
	if e.pk.Claims != 0 {
		t.Fatal("claimed into the wrong permanode")
	}
}

func TestKeepIsIdempotentAndWritesMapping(t *testing.T) {
	e := newEnv(t)
	m, err := ReadMapping(e.dir)
	if err != nil || m.Permanode != e.policy.Permanode || m.Folder != "docs" {
		t.Fatalf("mapping %+v err %v", m, err)
	}
	fi1, _ := os.Stat(filepath.Join(e.dir, MappingFile))
	pn, err := Keep(e.ctx, e.client, "docs", e.dir, api.KeepPolicy{Grace: 48 * hour, MaxDefer: 7 * day, Expire: 30 * day})
	if err != nil || pn != e.policy.Permanode {
		t.Fatalf("rekeep %s %v", pn, err)
	}
	fi2, _ := os.Stat(filepath.Join(e.dir, MappingFile))
	if !fi1.ModTime().Equal(fi2.ModTime()) {
		t.Fatal("identical .keepsync rewritten")
	}
	if _, err := Keep(e.ctx, e.client, "docs", e.dir, api.KeepPolicy{Grace: 48 * hour, MaxDefer: hour}); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

func TestKeepJoinsExistingMapping(t *testing.T) {
	// A second node receives .keepsync through Syncthing before the server
	// knows the folder is kept (say, after recovery): Keep reuses it.
	ctx := context.Background()
	pk := fake.NewPerkeep()
	pn, _ := pk.NewPermanode(ctx, "docs")
	reg := fake.NewRegistry(pk)
	c := reg.Client("nodekey:a")
	c.AddFolder(ctx, "docs")
	dir := t.TempDir()
	WriteMapping(dir, Mapping{Folder: "docs", Permanode: pn})
	got, err := Keep(ctx, c, "docs", dir, api.KeepPolicy{})
	if err != nil || got != pn {
		t.Fatalf("Keep = %s, %v; want %s", got, err, pn)
	}
}

func TestKeepNowSkipsGrace(t *testing.T) {
	e := newEnv(t)
	e.write("a.txt", "a", 0)
	e.write("a.sync-conflict-20260101-000000-XYZ.txt", "b", 0)
	rep, err := e.k.KeepNow(e.ctx, "a.txt", "a.sync-conflict-20260101-000000-XYZ.txt", "../etc/passwd", MappingFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Kept) != 2 || len(rep.Errors) != 2 {
		t.Fatalf("rep %+v", rep)
	}
}

func TestStatus(t *testing.T) {
	e := newEnv(t)
	e.write("grace.txt", "g", hour)
	e.write("kept.txt", "k", 3*day)
	e.write("old.txt", "o", 40*day)
	e.pass()
	e.write("due.txt", "d", 3*day) // appeared after the pass
	st, err := e.k.Status(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]State{}
	for _, f := range st.Files {
		got[f.Path] = f.State
	}
	want := map[string]State{"grace.txt": StateGrace, "kept.txt": StateKept, "old.txt": StateKept, "due.txt": StateDue}
	for p, s := range want {
		if got[p] != s {
			t.Errorf("%s: %s, want %s", p, got[p], s)
		}
	}
	e.pk.MarkDurable()
	st, _ = e.k.Status(e.ctx)
	if c := st.Counts(); c[StateExpirable] != 1 {
		t.Fatalf("counts %v", c)
	}
	for _, f := range st.Files {
		if f.Path == "grace.txt" && !f.KeepAt.Equal(f.ModTime.Add(48*hour)) {
			t.Fatalf("KeepAt %v", f.KeepAt)
		}
	}
	if e.pk.Claims != 2 {
		t.Fatalf("Status changed the archive: claims %d", e.pk.Claims)
	}
}

// TestExpiryRehashesSameSizeSameMtimeEdit checks that expiry hashes the
// file before removing it, so an in-place edit that kept both size and
// mtime is not removed on the strength of the put cache.
func TestExpiryRehashesSameSizeSameMtimeEdit(t *testing.T) {
	e := newEnv(t)
	e.write("tags.mp3", "old tags", 31*day)
	if rep := e.pass(); !slices.Contains(rep.Kept, "tags.mp3") {
		t.Fatalf("not kept: %+v", rep)
	}
	e.write("tags.mp3", "new tags", 31*day) // same size, same mtime
	e.pk.MarkDurable()
	if rep := e.pass(); len(rep.Expired) != 0 || !e.exists("tags.mp3") {
		t.Fatalf("an unkept edit was expired: %+v", rep)
	}
}
