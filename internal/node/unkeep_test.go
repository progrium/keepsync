package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/memory"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/keeper"
)

// countingPutter counts uploads.
type countingPutter struct {
	api.BlobPutter
	n atomic.Int64
}

func (c *countingPutter) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	c.n.Add(1)
	return c.BlobPutter.PutFile(ctx, path)
}

// memPutter writes real file schema blobs to an in-memory store, so
// restore can read them back.
type memPutter struct{ sto *memory.Storage }

func (m memPutter) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fi, _ := f.Stat()
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, m.sto, filepath.Base(path), fi.ModTime(), f)
	return api.BlobRef(ref.String()), err
}

type testNode struct {
	*Node
	freg *fake.Registry
	pk   *fake.Perkeep
	dir  string // the synced folder
	ctx  context.Context
	stop context.CancelFunc
}

// newTestNode runs a Node on fakes with one attached folder "docs" and a
// fast keeper interval.
func newTestNode(t *testing.T, putter func(*fake.Perkeep) api.BlobPutter) *testNode {
	t.Helper()
	return newTestNodeOpts(t, putter, false)
}

// newTestNodeOpts is newTestNode; with otherFirst, another node creates
// "docs" first, so it is the folder's first member and keeper.
func newTestNodeOpts(t *testing.T, putter func(*fake.Perkeep) api.BlobPutter, otherFirst bool) *testNode {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pk := fake.NewPerkeep()
	reg := fake.NewRegistry(pk)
	tr := fake.NewNetwork().Transport("a")
	cfg := &config.Node{Dir: t.TempDir(), Name: "a", KeepInterval: config.Duration(20 * time.Millisecond)}
	n := newNode(cfg, t.Logf)
	n.ctx, n.tr, n.reg, n.sy = ctx, tr, reg.Client(tr.Key()), fake.NewSyncer("DEVICE-A")
	n.putter = putter(pk)
	if _, err := n.reg.Register(ctx, api.Node{Name: "a", Key: tr.Key(), Addr: tr.Addr(), Device: "DEVICE-A"}); err != nil {
		t.Fatal(err)
	}
	var f api.Folder
	var err error
	if otherFirst {
		other := reg.Client("nodekey:other")
		other.Register(ctx, api.Node{Name: "other", Key: "nodekey:other", Device: "OTHERDV-XXXXXXX"})
		if _, err := other.AddFolder(ctx, "docs"); err != nil {
			t.Fatal(err)
		}
		f, err = n.reg.JoinFolder(ctx, "docs")
	} else {
		f, err = n.reg.AddFolder(ctx, "docs")
	}
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "docs")
	os.MkdirAll(dir, 0o755)
	cfg.SetFolder(config.Folder{Name: "docs", Path: dir, ID: f.ID})
	n.up.Store(true)
	t.Cleanup(n.shutdown)
	return &testNode{Node: n, freg: reg, pk: pk, dir: dir, ctx: ctx, stop: cancel}
}

func (tn *testNode) sync() { tn.apply(tn.ctx, tn.freg.Membership()) }

func (tn *testNode) keep(t *testing.T) api.BlobRef {
	t.Helper()
	pn, err := keeper.Keep(tn.ctx, tn.reg, "docs", tn.dir, api.KeepPolicy{Grace: time.Hour, MaxDefer: 2 * time.Hour, Expire: 72 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	tn.sync()
	return pn
}

func writeOld(t *testing.T, path, body string, age time.Duration) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Now().Add(-age)
	os.Chtimes(path, mt, mt)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUnkeepStopsPutsAndExpiry(t *testing.T) {
	var cp *countingPutter
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { cp = &countingPutter{BlobPutter: pk}; return cp })
	old := filepath.Join(tn.dir, "old.txt")
	writeOld(t, old, "old", 10*24*time.Hour)
	tn.keep(t)
	waitFor(t, "the first put", func() bool { return cp.n.Load() > 0 })

	if _, err := tn.unkeep(tn.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	tn.mu.Lock()
	nk := len(tn.keepers)
	tn.mu.Unlock()
	if nk != 0 {
		t.Fatalf("%d keepers still running after unkeep", nk)
	}
	// Now the kept file is durable and past expire: a running keeper would
	// expire it, and a new file would be put.
	tn.pk.MarkDurable()
	writeOld(t, filepath.Join(tn.dir, "new.txt"), "new", 5*24*time.Hour)
	before := cp.n.Load()
	tn.sync()
	time.Sleep(200 * time.Millisecond) // ten keeper intervals
	if got := cp.n.Load(); got != before {
		t.Fatalf("puts went from %d to %d after unkeep", before, got)
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatalf("old.txt was expired after unkeep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tn.dir, keeper.MappingFile)); err != nil {
		t.Fatalf(".keepsync should stay: %v", err)
	}
	if err := tn.notKept("docs"); err == nil {
		t.Fatal("notKept returned nil")
	} else if want := "is not kept"; !strings.Contains(err.Error(), want) {
		t.Fatalf("keep status message %q lacks %q", err, want)
	}
}

func TestRestoreAfterUnkeep(t *testing.T) {
	sto := new(memory.Storage)
	tn := newTestNode(t, func(*fake.Perkeep) api.BlobPutter { return memPutter{sto} })
	tn.fetcher = func() (blob.Fetcher, error) { return sto, nil }
	f := filepath.Join(tn.dir, "a.txt")
	writeOld(t, f, "kept body", 10*24*time.Hour)
	pn := tn.keep(t)
	waitFor(t, "a.txt kept", func() bool {
		paths, _ := tn.reg.Paths(tn.ctx, pn)
		_, ok := paths["a.txt"]
		return ok
	})
	if _, err := tn.unkeep(tn.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	tn.sync()
	os.Remove(f)
	res, err := tn.restore(tn.ctx, RestoreParams{Folder: "docs"})
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(f); string(b) != "kept body" || len(res.Restored) != 1 {
		t.Fatalf("restored %q, result %+v", b, res)
	}
}

func TestRekeepContinuesPermanode(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	pn := tn.keep(t)
	if _, err := tn.unkeep(tn.ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	tn.sync()
	if a := tn.archive("docs"); a != pn {
		t.Fatalf("archive after unkeep = %s, want %s", a, pn)
	}
	if again := tn.keep(t); again != pn {
		t.Fatalf("re-keep made permanode %s, want %s", again, pn)
	}
	m, err := keeper.ReadMapping(tn.dir)
	if err != nil || m.Permanode != pn {
		t.Fatalf(".keepsync %+v %v", m, err)
	}
	tn.mu.Lock()
	nk := len(tn.keepers)
	tn.mu.Unlock()
	if nk != 1 {
		t.Fatalf("%d keepers after re-keep, want 1", nk)
	}
}

// stopKeepers stops the background keeper loops but keeps the keepers, so
// a test can call KeepNow without a pass racing it.
func (tn *testNode) stopKeepers() {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	for _, k := range tn.keepers {
		k.cancel()
	}
}
