package zerofs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/hugelgupf/p9/fsimpl/localfs"
	"github.com/hugelgupf/p9/p9"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/files"
	"perkeep.org/pkg/blobserver/storagetest"
)

// serveNineP serves dir over 9P on a unix socket and returns a client.
func serveNineP(t *testing.T, dir string) *NineP {
	t.Helper()
	sock := filepath.Join(shortTemp(t), "9p.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv := p9.NewServer(localfs.Attacher(dir))
	go srv.ServeContext(ctx, ln)
	n, err := DialNineP(ctx, sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		n.Close()
		cancel()
		ln.Close()
	})
	return n
}

// shortTemp returns a temp dir with a path short enough for a socket.
func shortTemp(t *testing.T) string {
	d, err := os.MkdirTemp("", "9p")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestNinePVFS(t *testing.T) {
	dir := t.TempDir()
	n := serveNineP(t, dir)

	if err := n.MkdirAll("/a/b/c", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := n.MkdirAll("a/b", 0o700); err != nil {
		t.Fatalf("MkdirAll of an existing dir: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "a/b/c")); err != nil || !fi.IsDir() {
		t.Fatalf("a/b/c not made: %v", err)
	}

	w, err := n.TempFile("a/b", "blob.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(w.Name()) != "a/b" {
		t.Errorf("temp name %q not in a/b", w.Name())
	}
	data := []byte("hello over 9p")
	if _, err := w.Write(data[:5]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data[5:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.Rename(w.Name(), "a/b/blob"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a/b/blob")); string(b) != string(data) {
		t.Fatalf("on disk: %q", b)
	}
	fi, err := n.Lstat("a/b/blob")
	if err != nil || fi.Size() != int64(len(data)) || fi.IsDir() || fi.Name() != "blob" {
		t.Fatalf("Lstat = %v, %v", fi, err)
	}
	if fi, err := n.Stat("/a/b"); err != nil || !fi.IsDir() {
		t.Fatalf("Stat dir = %v, %v", fi, err)
	}

	r, err := n.Open("a/b/blob")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seek(6, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(r); err != nil || string(b) != "over 9p" {
		t.Fatalf("read after seek = %q, %v", b, err)
	}
	if off, _ := r.Seek(-2, io.SeekEnd); off != int64(len(data))-2 {
		t.Fatalf("seek end = %d", off)
	}
	r.Close()

	// Rename replaces an existing target.
	if err := os.WriteFile(filepath.Join(dir, "a/other"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := n.Rename("a/other", "a/b/blob"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "a/b/blob")); string(b) != "x" {
		t.Fatalf("rename over: %q", b)
	}

	ents, err := n.ReadDir("a/b")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if e.Name() == "blob" && (e.IsDir() || info.Size() != 1) {
			t.Errorf("blob entry: dir=%v size=%d", e.IsDir(), info.Size())
		}
		if e.Name() == "c" && !e.IsDir() {
			t.Error("c is not a dir")
		}
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"blob", "c"}) {
		t.Fatalf("ReadDir = %v", names)
	}

	for _, err := range []error{
		func() error { _, err := n.Stat("nope"); return err }(),
		func() error { _, err := n.Open("a/nope"); return err }(),
		n.Remove("a/nope"),
		func() error { _, err := n.ReadDir("nope"); return err }(),
	} {
		if !os.IsNotExist(err) || !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("want not-exist, got %v", err)
		}
	}

	if err := n.RemoveDir("a/b"); err == nil {
		t.Error("RemoveDir of a non-empty dir succeeded")
	}
	if err := n.Remove("a/b/blob"); err != nil {
		t.Fatal(err)
	}
	if err := n.RemoveDir("a/b/c"); err != nil {
		t.Fatal(err)
	}

	if err := n.WriteFile("x/y/reg.json", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := n.WriteFile("x/y/reg.json", []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	if b, err := n.ReadFile("/x/y/reg.json"); err != nil || string(b) != `{"v":2}` {
		t.Fatalf("ReadFile = %q, %v", b, err)
	}
	if ents, _ := os.ReadDir(filepath.Join(dir, "x/y")); len(ents) != 1 {
		t.Errorf("WriteFile left temp files: %v", ents)
	}
	if n.Err() != nil {
		t.Fatal(n.Err())
	}
}

func TestNinePBigFile(t *testing.T) {
	n := serveNineP(t, t.TempDir())
	big := make([]byte, 3*ninePMsize+12345) // several 9P messages each way
	for i := range big {
		big[i] = byte(i * 7)
	}
	if err := n.WriteFile("big", big); err != nil {
		t.Fatal(err)
	}
	b, err := n.ReadFile("big")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b, big) {
		t.Fatal("big file differs")
	}
}

// TestNinePBlobStorage runs Perkeep's blob storage conformance tests on a
// files.Storage over NineP.
func TestNinePBlobStorage(t *testing.T) {
	storagetest.Test(t, func(t *testing.T) blobserver.Storage {
		n := serveNineP(t, t.TempDir())
		if err := n.MkdirAll("blobs", 0o700); err != nil {
			t.Fatal(err)
		}
		return files.NewStorage(n, "/blobs")
	})
}

func TestNinePConnRewrites(t *testing.T) {
	var got [][]byte
	c := &ninePConn{rwc: writerFunc(func(p []byte) (int, error) {
		got = append(got, slices.Clone(p))
		return len(p), nil
	})}
	// Tversion asking for 9P2000.L.Google.7, written in two pieces.
	ver := "9P2000.L.Google.7"
	msg := []byte{0, 0, 0, 0, msgTversion, 0xff, 0xff, 0, 0, 0x10, 0, byte(len(ver)), 0}
	msg = append(msg, ver...)
	msg[0] = byte(len(msg))
	c.Write(msg[:7])
	c.Write(msg[7:])
	// Tfsync without datasync, then a Tclunk, in one write.
	fsync := []byte{11, 0, 0, 0, msgTfsync, 1, 0, 5, 0, 0, 0}
	clunk := []byte{11, 0, 0, 0, 120, 2, 0, 5, 0, 0, 0}
	c.Write(append(slices.Clone(fsync), clunk...))
	if len(got) != 3 {
		t.Fatalf("got %d writes", len(got))
	}
	if string(got[0][13:]) != "9P2000.L" || int(got[0][0]) != len(got[0]) || got[0][9] != 0x10 {
		t.Errorf("Tversion = %q", got[0])
	}
	if len(got[1]) != 15 || got[1][0] != 15 || got[1][4] != msgTfsync {
		t.Errorf("Tfsync = %v", got[1])
	}
	if !slices.Equal(got[2], clunk) {
		t.Errorf("Tclunk = %v", got[2])
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
func (f writerFunc) Read([]byte) (int, error)    { return 0, io.EOF }
func (f writerFunc) Close() error                { return nil }

func TestSupervisorNineP(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, events := fakeSupervisor(t, "FAKE_NINEP_ROOT="+root)
	s.cfg.Mount, s.mount = MountNineP, &ninePMount{}

	if s.FS() != nil || s.VFS() != nil {
		t.Fatal("FS before Start")
	}
	if err := s.WriteFile("x", nil); err == nil {
		t.Fatal("WriteFile before Start: want error")
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Dir() != "" {
		t.Errorf("Dir = %q, want empty", s.Dir())
	}
	if _, err := os.Stat(s.cfg.MountDir); !os.IsNotExist(err) {
		t.Errorf("mount dir made in 9p mode: %v", err)
	}
	if s.VFS() == nil {
		t.Fatal("no VFS while running")
	}
	if err := s.WriteFile("registry/state.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(root, "registry/state.json")); err != nil || string(b) != "{}" {
		t.Fatalf("written over 9p: %q, %v", b, err)
	}
	if b, err := s.ReadFile("registry/state.json"); err != nil || string(b) != "{}" {
		t.Fatalf("ReadFile = %q, %v", b, err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if s.FS() != nil {
		t.Error("FS after Stop")
	}
	if got := readEvents(t, events); !slices.Equal(got, []string{"run", "flush", "flush", "term"}) {
		t.Errorf("events = %v", got)
	}
}

// partialWalkFile answers every walk as zerofs does when only the first
// name exists: one QID, no error.
type partialWalkFile struct{ p9.File }

func (f partialWalkFile) Walk(names []string) ([]p9.QID, p9.File, error) {
	return []p9.QID{{}}, closeFile{}, nil
}

type closeFile struct{ p9.File }

func (closeFile) Close() error { return nil }

func TestWalkPartialIsNotExist(t *testing.T) {
	if _, err := walkOne(partialWalkFile{}, []string{"a"}); err != nil {
		t.Fatalf("whole walk: %v", err)
	}
	_, err := walkOne(partialWalkFile{}, []string{"a", "b"})
	n := &NineP{}
	if !errors.Is(n.pathErr("stat", "a/b", err), fs.ErrNotExist) {
		t.Fatalf("partial walk: %v", err)
	}
}

func TestDirentType(t *testing.T) {
	// zerofs puts DT_DIR (4) in the entry's type byte; the QID says dir.
	if typ := direntType(p9.Dirent{Type: 4, QID: p9.QID{Type: p9.TypeDir}}); !typ.IsDir() {
		t.Errorf("zerofs dir entry: %v", typ)
	}
	if typ := direntType(p9.Dirent{Type: 8, QID: p9.QID{Type: p9.TypeRegular}}); typ != 0 {
		t.Errorf("regular entry: %v", typ)
	}
}
