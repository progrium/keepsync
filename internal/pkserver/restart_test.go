package pkserver

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

// restartingZeroFS is a fake ZeroFS that reports restarts like
// zerofs.Supervisor. With restartOnFlush set, its next Flush "restarts"
// the process before succeeding.
type restartingZeroFS struct {
	*fake.ZeroFS
	restarts       atomic.Uint64
	restartOnFlush atomic.Bool
}

func (z *restartingZeroFS) Restarts() uint64 { return z.restarts.Load() }

func (z *restartingZeroFS) Flush(ctx context.Context) error {
	if z.restartOnFlush.CompareAndSwap(true, false) {
		z.restarts.Add(1)
	}
	return z.ZeroFS.Flush(ctx)
}

// TestFlushAfterZeroFSRestart checks that a flush racing a ZeroFS restart
// doesn't make anything durable, that the next flush repairs the replica
// and goes on, and that blobs seen before the restart need a flush after
// it.
func TestFlushAfterZeroFSRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "server")
	if _, err := Init(dir); err != nil {
		t.Fatal(err)
	}
	zfs := &restartingZeroFS{ZeroFS: &fake.ZeroFS{DirPath: filepath.Join(root, "zerofs")}}
	if err := zfs.Start(ctx); err != nil {
		t.Fatal(err)
	}
	nw := fake.NewNetwork()
	server, node := nw.Transport("server"), nw.Transport("node")
	server.Allow(node.Key())
	srv, err := Start(ctx, Config{Mode: ModeStore, Dir: dir, ZeroFS: zfs, Transport: server, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	e := &env{srv: srv, zfs: zfs.ZeroFS, server: server, node: node, dir: dir}
	if e.putter, err = NewPutter(node, server.Addr()); err != nil {
		t.Fatal(err)
	}

	ref, err := e.putter.PutFile(ctx, writeFile(t, t.TempDir(), "a.txt", strings.Repeat("a", 5000), time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	srv.Durable(ctx, []api.BlobRef{ref})
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := srv.Drain(dctx); err != nil {
		t.Fatal(err)
	}

	zfs.restartOnFlush.Store(true)
	if err := srv.Flush(ctx); err == nil || !strings.Contains(err.Error(), "restarted during the flush") {
		t.Fatalf("flush across a restart: %v, want a restarted error", err)
	}
	if d, _ := srv.Durable(ctx, []api.BlobRef{ref}); d[0] {
		t.Fatal("durable after a flush that a restart cut across")
	}
	if err := srv.Flush(ctx); err != nil {
		t.Fatalf("flush after the restart: %v", err)
	}
	waitDurable(t, e, []api.BlobRef{ref})
}
