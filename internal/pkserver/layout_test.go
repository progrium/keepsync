package pkserver

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hugelgupf/p9/fsimpl/localfs"
	"github.com/hugelgupf/p9/p9"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/files"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
	"github.com/progrium/keepsync/internal/zerofs"
)

// ninePZeroFS is a fake ZeroFS reached over 9P: its directory is served by
// a 9P server, and the pkserver sees only VFS, never the directory.
type ninePZeroFS struct {
	*fake.ZeroFS
	fs *zerofs.NineP
}

func (z *ninePZeroFS) Dir() string    { return "" }
func (z *ninePZeroFS) VFS() files.VFS { return z.fs }

func newNinePZeroFS(t *testing.T, dir string) *ninePZeroFS {
	t.Helper()
	z := &ninePZeroFS{ZeroFS: &fake.ZeroFS{DirPath: dir}}
	if err := z.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	sd, err := os.MkdirTemp("", "9p")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(sd, "s"))
	if err != nil {
		t.Fatal(err)
	}
	go p9.NewServer(localfs.Attacher(dir)).Serve(ln)
	if z.fs, err = zerofs.DialNineP(context.Background(), filepath.Join(sd, "s")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		z.fs.Close()
		ln.Close()
		os.RemoveAll(sd)
	})
	return z
}

func startWith(t *testing.T, mode, dir string, z api.ZeroFS) *Server {
	t.Helper()
	srv, err := Start(context.Background(), Config{Mode: mode, Dir: dir, ZeroFS: z, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// fetchString fetches ref from the blob store on ZeroFS.
func fetchString(t *testing.T, s *Server, ref api.BlobRef) string {
	t.Helper()
	rc, _, err := s.replica.Fetch(context.Background(), blob.MustParse(string(ref)))
	if err != nil {
		t.Fatalf("fetch %s: %v", ref, err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return string(b)
}

// countFilesBlobs counts blob files in the files layout under dir.
func countFilesBlobs(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	filepath.Walk(filepath.Join(dir, filesRoot), func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() && strings.HasPrefix(fi.Name(), "sha224-") && strings.HasSuffix(fi.Name(), ".dat") {
			n++
		}
		return nil
	})
	return n
}

func TestNinePLayout(t *testing.T) {
	for _, mode := range []string{ModeStore, ModeCache} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir, zdir := filepath.Join(root, "server"), filepath.Join(root, "zerofs")
			if _, err := Init(dir); err != nil {
				t.Fatal(err)
			}
			z := newNinePZeroFS(t, zdir)
			srv := startWith(t, mode, dir, z)
			if srv.layout != LayoutFiles {
				t.Fatalf("layout %q over 9p", srv.layout)
			}
			ref, err := putBytes(srv, "over nine p "+mode)
			if err != nil {
				t.Fatal(err)
			}
			waitDurable(t, &env{srv: srv}, []api.BlobRef{ref})
			if n := countFilesBlobs(t, zdir); n == 0 {
				t.Fatal("no blob files on zerofs")
			}
			if _, err := os.Stat(filepath.Join(zdir, diskpackedMarker)); err == nil {
				t.Fatal("diskpacked store made over 9p")
			}
			if _, err := os.Stat(filepath.Join(dir, "zerofs-9p")); err == nil {
				t.Fatal("placeholder replica dir created")
			}
			srv.Close()

			// A new server (as in a recovery) reads it back from ZeroFS. A
			// fresh dir: perkeepd doesn't let go of its index in-process.
			srv = startWith(t, mode, newServerDir(t), z)
			defer srv.Close()
			if got := fetchString(t, srv, ref); got != "over nine p "+mode {
				t.Fatalf("after restart: %q", got)
			}
		})
	}
}

// TestLayoutDetection checks that a bucket keeps the layout it was written
// in, whichever way it is reached.
func TestLayoutDetection(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir, zdir := filepath.Join(root, "server"), filepath.Join(root, "zerofs")
	if _, err := Init(dir); err != nil {
		t.Fatal(err)
	}

	// Written over 9P in cache mode, then read through a mount: still the
	// files layout, now via localdisk on the mount.
	z := newNinePZeroFS(t, zdir)
	srv := startWith(t, ModeCache, dir, z)
	ref, err := putBytes(srv, "files via mount")
	if err != nil {
		t.Fatal(err)
	}
	srv.Flush(ctx)
	srv.Close()
	mounted := &fake.ZeroFS{DirPath: zdir}
	mounted.Start(ctx)
	srv = startWith(t, ModeCache, newServerDir(t), mounted)
	if srv.layout != LayoutFiles {
		t.Fatalf("layout %q through the mount", srv.layout)
	}
	if got := fetchString(t, srv, ref); got != "files via mount" {
		t.Fatalf("through the mount: %q", got)
	}
	srv.Close()

	// A diskpacked bucket can't be read over 9P.
	pdir := filepath.Join(root, "packed")
	pz := &fake.ZeroFS{DirPath: pdir}
	pz.Start(ctx)
	srv = startWith(t, ModeCache, newServerDir(t), pz)
	if srv.layout != LayoutDiskpacked {
		t.Fatalf("layout %q on an empty mount", srv.layout)
	}
	srv.Close()
	if _, err := Start(ctx, Config{Mode: ModeCache, Dir: newServerDir(t), ZeroFS: newNinePZeroFS(t, pdir), Logf: t.Logf}); err == nil || !strings.Contains(err.Error(), "diskpacked") {
		t.Fatalf("diskpacked over 9p: %v", err)
	}

	// Both layouts at once is an error.
	os.WriteFile(filepath.Join(zdir, diskpackedMarker), nil, 0o600)
	if _, err := Start(ctx, Config{Mode: ModeCache, Dir: newServerDir(t), ZeroFS: mounted, Logf: t.Logf}); err == nil {
		t.Fatal("both layouts: want error")
	}
}

func newServerDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "server")
	if _, err := Init(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

func putBytes(s *Server, data string) (api.BlobRef, error) {
	br := blob.RefFromString(data)
	_, err := blobserver.Receive(context.Background(), s.bs, br, strings.NewReader(data))
	return api.BlobRef(br.String()), err
}
