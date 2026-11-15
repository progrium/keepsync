package pkserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/search"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

type env struct {
	srv    *Server
	zfs    *fake.ZeroFS
	putter *Putter
	node   *fake.Transport
	server *fake.Transport
	dir    string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	e := &env{dir: filepath.Join(root, "server")}
	if _, err := Init(e.dir); err != nil {
		t.Fatal(err)
	}
	e.zfs = &fake.ZeroFS{DirPath: filepath.Join(root, "zerofs")}
	if err := e.zfs.Start(ctx); err != nil {
		t.Fatal(err)
	}
	nw := fake.NewNetwork()
	e.server = nw.Transport("server")
	e.node = nw.Transport("node")
	e.server.Allow(e.node.Key())
	srv, err := Start(ctx, Config{Dir: e.dir, ZeroFS: e.zfs, Transport: e.server, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	e.srv = srv
	e.putter, err = NewPutter(e.node, e.server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func writeFile(t *testing.T, dir, name, body string, mtime time.Time) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestInitIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	a, err := Init(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Init(dir)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a.KeyID == "" {
		t.Fatalf("identity changed or empty: %+v then %+v", a, b)
	}
}

func TestSignPathsAndDurability(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	files := t.TempDir()
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	pn, err := e.srv.NewPermanode(ctx, "photos")
	if err != nil {
		t.Fatal(err)
	}
	if paths, err := e.srv.Paths(ctx, pn); err != nil || len(paths) != 0 {
		t.Fatalf("new permanode: paths %v, err %v", paths, err)
	}

	// A node puts a file over the transport and the server signs its path.
	v1, err := e.putter.PutFile(ctx, writeFile(t, files, "a.txt", "version one", t0))
	if err != nil {
		t.Fatal(err)
	}
	claim := func(file api.BlobRef, mt time.Time) api.ClaimResult {
		t.Helper()
		r, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "dir/a.txt", File: file, ModTime: mt})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := claim(v1, t0); r != api.ClaimWritten {
		t.Fatalf("first claim: got %v, want written", r)
	}
	if r := claim(v1, t0); r != api.ClaimUnchanged {
		t.Fatalf("same claim again: got %v, want unchanged", r)
	}

	v2, err := e.putter.PutFile(ctx, writeFile(t, files, "a2.txt", "version two", t0.Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if r := claim(v2, t0); r != api.ClaimRejectedOlder {
		t.Fatalf("same mtime: got %v, want rejected", r)
	}
	if r := claim(v2, t0.Add(-time.Hour)); r != api.ClaimRejectedOlder {
		t.Fatalf("older mtime: got %v, want rejected", r)
	}
	if r := claim(v2, t0.Add(time.Hour)); r != api.ClaimWritten {
		t.Fatalf("newer mtime: got %v, want written", r)
	}
	paths, err := e.srv.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	got := paths["dir/a.txt"]
	if got.File != v2 || !got.ModTime.Equal(t0.Add(time.Hour)) || len(got.Claims) == 0 {
		t.Fatalf("paths: got %+v, want %s at %v with a claim", got, v2, t0.Add(time.Hour))
	}

	// Nothing is durable before a flush, and the file and its naming
	// claim are durable after one, once replication has caught up.
	refs := append([]api.BlobRef{v2, pn}, got.Claims...)
	d, err := e.srv.Durable(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range d {
		if ok {
			t.Fatalf("%s durable before any flush", refs[i])
		}
	}
	waitDurable(t, e, refs)
	if e.zfs.Flushes == 0 {
		t.Fatal("durable without a flush")
	}

	// A ref the server has never seen is not durable.
	d, err = e.srv.Durable(ctx, []api.BlobRef{"sha224-00000000000000000000000000000000000000000000000000000000"})
	if err != nil || d[0] {
		t.Fatalf("unknown ref: durable %v, err %v", d, err)
	}
}

// waitDurable flushes until refs are durable, for the asynchronous
// replication to the diskpacked replica to catch up.
func waitDurable(t *testing.T, e *env, refs []api.BlobRef) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(20 * time.Second)
	for {
		d, err := e.srv.Durable(ctx, refs)
		if err != nil {
			t.Fatal(err)
		}
		all := true
		for _, ok := range d {
			all = all && ok
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not durable after flushes: %v", d)
		}
		if err := e.srv.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestDrainThenFlush(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	var refs []api.BlobRef
	files := t.TempDir()
	for i := range 20 {
		ref, err := e.putter.PutFile(ctx, writeFile(t, files, fmt.Sprint(i), strings.Repeat(fmt.Sprint(i), 100000), time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	e.srv.Durable(ctx, refs) // ask, so the next flush covers them
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := e.srv.Drain(dctx); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	d, err := e.srv.Durable(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range d {
		if !ok {
			t.Errorf("%s not durable after drain and one flush", refs[i])
		}
	}
}

func TestDurableNeedsFlushAfterReplication(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	ref, err := e.putter.PutFile(ctx, writeFile(t, t.TempDir(), "b.txt", "hello", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	waitDurable(t, e, []api.BlobRef{ref})

	// Once durable, a failed flush doesn't take it back, but a new blob
	// isn't made durable by a flush that fails.
	ref2, err := e.putter.PutFile(ctx, writeFile(t, t.TempDir(), "c.txt", "world", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Durable(ctx, []api.BlobRef{ref2})
	time.Sleep(500 * time.Millisecond) // let replication copy it
	e.zfs.Stop(ctx)
	if err := e.srv.Flush(ctx); err == nil {
		t.Fatal("flush succeeded with zerofs stopped")
	}
	d, err := e.srv.Durable(ctx, []api.BlobRef{ref, ref2})
	if err != nil {
		t.Fatal(err)
	}
	if !d[0] || d[1] {
		t.Fatalf("after failed flush: got %v, want [true false]", d)
	}
	e.zfs.Start(ctx)
	waitDurable(t, e, []api.BlobRef{ref2})
}

func TestNodesCannotSignOrDebug(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return e.node.Dial(ctx, e.server.Addr(), api.PortPerkeep)
		},
	}}
	for _, p := range []string{"/sighelper/camli/sig/sign", "/debug/config", "/debug/vars", "/my-search/camli/search/recent"} {
		req, _ := http.NewRequestWithContext(ctx, "GET", baseURL+p, nil)
		res, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode < 400 {
			t.Errorf("node GET %s: %s, want refused", p, res.Status)
		}
	}
}

// TestPathsSurviveRestart checks that a restarted server reads the tree back
// from the index, mtimes and naming claims included, and still applies the
// newer-mtime rule. perkeepd never closes its sync queues, whose locks are
// held until the process exits, so each run of the server is a child
// process, as in a real restart.
func TestPathsSurviveRestart(t *testing.T) {
	switch os.Getenv("PKSERVER_RESTART_PHASE") {
	case "write":
		restartWrite(t)
		return
	case "check":
		restartCheck(t)
		return
	}
	root := t.TempDir()
	child := func(phase, extra string) string {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPathsSurviveRestart$", "-test.v")
		cmd.Env = append(os.Environ(), "PKSERVER_RESTART_PHASE="+phase, "PKSERVER_RESTART_ROOT="+root, "PKSERVER_RESTART_CASE="+extra)
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "restart child ok") {
			t.Fatalf("%s phase: %v\n%s", phase, err, lastLines(string(out), 30))
		}
		_, after, _ := strings.Cut(string(out), "CASE=")
		line, _, _ := strings.Cut(after, "\n")
		return line
	}
	c := child("write", "")
	child("check", c)
}

type restartCase struct {
	Permanode api.BlobRef
	Want      map[string]api.PathEntry
	T0        time.Time
}

func restartWrite(t *testing.T) {
	ctx := context.Background()
	root := os.Getenv("PKSERVER_RESTART_ROOT")
	dir := filepath.Join(root, "server")
	if _, err := Init(dir); err != nil {
		t.Fatal(err)
	}
	zfs := &fake.ZeroFS{DirPath: filepath.Join(root, "zerofs")}
	zfs.Start(ctx)
	nw := fake.NewNetwork()
	st, nt := nw.Transport("server"), nw.Transport("node")
	st.Allow(nt.Key())
	srv, err := Start(ctx, Config{Dir: dir, ZeroFS: zfs, Transport: st, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	putter, err := NewPutter(nt, st.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 123456789, time.UTC)
	pn, err := srv.NewPermanode(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	files := t.TempDir()
	for i, name := range []string{"x", "y/z", "with space"} {
		mt := t0.Add(time.Duration(i) * time.Minute)
		ref, err := putter.PutFile(ctx, writeFile(t, files, fmt.Sprintf("f%d", i), name, mt))
		if err != nil {
			t.Fatal(err)
		}
		if r, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: name, File: ref, ModTime: mt}); err != nil || r != api.ClaimWritten {
			t.Fatalf("claim %s: %v %v", name, r, err)
		}
	}
	want, err := srv.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := putter.PutFile(ctx, writeFile(t, files, "late", "late", t0)); err == nil {
		t.Fatal("closed server still serving nodes")
	}
	b, _ := json.Marshal(restartCase{Permanode: pn, Want: want, T0: t0})
	fmt.Printf("CASE=%s\n", b)
	if !t.Failed() {
		t.Log("restart child ok")
	}
}

func restartCheck(t *testing.T) {
	ctx := context.Background()
	root := os.Getenv("PKSERVER_RESTART_ROOT")
	var c restartCase
	if err := json.Unmarshal([]byte(os.Getenv("PKSERVER_RESTART_CASE")), &c); err != nil {
		t.Fatal(err)
	}
	zfs := &fake.ZeroFS{DirPath: filepath.Join(root, "zerofs")}
	zfs.Start(ctx)
	srv, err := Start(ctx, Config{Dir: filepath.Join(root, "server"), ZeroFS: zfs, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	got, err := srv.Paths(ctx, c.Permanode)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(c.Want) {
		t.Fatalf("after restart: %d paths, want %d: %v", len(got), len(c.Want), got)
	}
	for k, w := range c.Want {
		g := got[k]
		if g.File != w.File || !slices.Equal(g.Claims, w.Claims) || !g.ModTime.Equal(w.ModTime) {
			t.Errorf("%s: got %+v, want %+v", k, g, w)
		}
	}
	r, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: c.Permanode, Path: "x", File: c.Want["y/z"].File, ModTime: c.T0})
	if err != nil || r != api.ClaimRejectedOlder {
		t.Fatalf("stale claim after restart: %v %v", r, err)
	}
	r, err = srv.ClaimPath(ctx, api.PathClaim{Permanode: c.Permanode, Path: "x", File: c.Want["y/z"].File, ModTime: c.T0.Add(time.Hour)})
	if err != nil || r != api.ClaimWritten {
		t.Fatalf("newer claim after restart: %v %v", r, err)
	}
	if _, err := srv.Paths(ctx, "sha224-00000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("Paths of an unknown permanode succeeded")
	}
	if !t.Failed() {
		t.Log("restart child ok")
	}
}

func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func TestPathsAt(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	files := t.TempDir()
	t0 := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	pn, err := e.srv.NewPermanode(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	beforeAny := time.Now()
	put := func(name, body string, mt time.Time) api.BlobRef {
		t.Helper()
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
	v1 := put("a1", "one", t0)
	claim("a", v1, t0)
	time.Sleep(5 * time.Millisecond)
	afterV1 := time.Now()
	time.Sleep(5 * time.Millisecond)
	v2 := put("a2", "two", t0.Add(time.Hour))
	claim("a", v2, t0.Add(time.Hour))
	claim("b", put("b", "bee", t0), t0)
	afterAll := time.Now()

	at := func(when time.Time) map[string]api.PathEntry {
		t.Helper()
		m, err := e.srv.PathsAt(ctx, pn, when)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	if m := at(beforeAny); len(m) != 0 {
		t.Errorf("before any claim: %v, want empty", m)
	}
	m := at(afterV1)
	if m["a"].File != v1 || !m["a"].ModTime.Equal(t0) || len(m["a"].Claims) == 0 {
		t.Errorf("after v1: a = %+v, want %s", m["a"], v1)
	}
	if _, ok := m["b"]; ok {
		t.Errorf("after v1: b present, but it was claimed later")
	}
	m = at(afterAll)
	cur, err := e.srv.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 || !reflect.DeepEqual(m, cur) {
		t.Errorf("after all: %v, want current %v", m, cur)
	}
	if _, err := e.srv.PathsAt(ctx, "sha224-00000000000000000000000000000000000000000000000000000000", afterAll); err == nil {
		t.Error("PathsAt of an unknown permanode succeeded")
	}
}

func (s *Server) signedCount() int {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	return s.signed
}

// describeAttrs returns pn's attributes as perkeepd's search index has them.
func describeAttrs(t *testing.T, s *Server, pn api.BlobRef) map[string][]string {
	t.Helper()
	br, _ := blob.Parse(string(pn))
	res, err := s.bs.Describe(context.Background(), &search.DescribeRequest{BlobRef: br})
	if err != nil {
		t.Fatal(err)
	}
	m := res.Meta[string(pn)]
	if m == nil || m.Permanode == nil {
		t.Fatalf("%s: not described as a permanode", pn)
	}
	return m.Permanode.Attr
}

func TestKeptTreeLayout(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	files := t.TempDir()
	t0 := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	root, err := e.srv.NewPermanode(ctx, "photos")
	if err != nil {
		t.Fatal(err)
	}
	put := func(name, body string) api.BlobRef {
		t.Helper()
		ref, err := e.putter.PutFile(ctx, writeFile(t, files, name, body, t0))
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	claim := func(path string, ref api.BlobRef, mt time.Time) api.ClaimResult {
		t.Helper()
		r, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: root, Path: path, File: ref, ModTime: mt})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// A new file in two new directories: three permanodes and one claim.
	v1 := put("v1", "one")
	n := e.srv.signedCount()
	if r := claim("2026/trip/a.jpg", v1, t0); r != api.ClaimWritten {
		t.Fatalf("new nested file: %v", r)
	}
	if got := e.srv.signedCount() - n; got != 4 {
		t.Errorf("new file in new dirs signed %d blobs, want 3 permanodes and 1 claim", got)
	}

	// A sibling in an existing directory: one permanode and one claim.
	n = e.srv.signedCount()
	claim("2026/trip/b.jpg", put("b", "bee"), t0)
	if got := e.srv.signedCount() - n; got != 2 {
		t.Errorf("new file in an existing dir signed %d blobs, want 2", got)
	}

	// A change writes exactly one claim; the same file writes nothing; an
	// older or equal mtime is rejected.
	v2 := put("v2", "two")
	n = e.srv.signedCount()
	if r := claim("2026/trip/a.jpg", v2, t0.Add(time.Hour)); r != api.ClaimWritten {
		t.Fatalf("change: %v", r)
	}
	if got := e.srv.signedCount() - n; got != 1 {
		t.Errorf("change signed %d blobs, want 1", got)
	}
	n = e.srv.signedCount()
	if r := claim("2026/trip/a.jpg", v2, t0.Add(2*time.Hour)); r != api.ClaimUnchanged {
		t.Errorf("same file: %v, want unchanged", r)
	}
	if r := claim("2026/trip/a.jpg", v1, t0.Add(time.Hour)); r != api.ClaimRejectedOlder {
		t.Errorf("same mtime: %v, want rejected", r)
	}
	if got := e.srv.signedCount() - n; got != 0 {
		t.Errorf("unchanged and rejected claims signed %d blobs", got)
	}
	if _, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: root, Path: "2026/trip", File: v1, ModTime: t0}); err == nil {
		t.Error("claiming a directory's path as a file succeeded")
	}
	if _, err := e.srv.ClaimPath(ctx, api.PathClaim{Permanode: root, Path: "2026/../x", File: v1, ModTime: t0}); err == nil {
		t.Error("a path with .. was accepted")
	}

	// The cached tree matches a walk of the index.
	paths, err := e.srv.Paths(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	walked, err := e.srv.loadTree(ctx, root, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	fromIndex := flatten(walked)
	if len(paths) != 2 || len(fromIndex) != 2 {
		t.Fatalf("paths %v, from index %v", paths, fromIndex)
	}
	for k, p := range paths {
		w := fromIndex[k]
		if p.File != w.File || !p.ModTime.Equal(w.ModTime) || fmt.Sprint(p.Claims) != fmt.Sprint(w.Claims) {
			t.Errorf("%s: cached %+v, index %+v", k, p, w)
		}
	}
	a := paths["2026/trip/a.jpg"]
	if a.File != v2 || !a.ModTime.Equal(t0.Add(time.Hour)) {
		t.Errorf("a.jpg: %+v", a)
	}
	// root, 2026 and its naming claim, trip and its naming claim, a.jpg's
	// permanode, its naming claim (the claim that made the dirs) and the
	// change: the dirs' naming claims are one claim, so 6 distinct.
	if len(a.Claims) != 6 {
		t.Errorf("a.jpg claims: %d %v, want 6", len(a.Claims), a.Claims)
	}

	// The index shows the tree the Perkeep way.
	rootAttrs := describeAttrs(t, e.srv, root)
	if fmt.Sprint(rootAttrs["camliRoot"]) != "[photos]" || len(rootAttrs["camliPath:2026"]) != 1 {
		t.Errorf("root attrs: %v", rootAttrs)
	}
	trip := describeAttrs(t, e.srv, api.BlobRef(describeAttrs(t, e.srv, api.BlobRef(rootAttrs["camliPath:2026"][0]))["camliPath:trip"][0]))
	filePN := api.BlobRef(trip["camliPath:a.jpg"][0])
	fileAttrs := describeAttrs(t, e.srv, filePN)
	if fmt.Sprint(fileAttrs["camliContent"]) != "["+string(v2)+"]" || len(fileAttrs["keepsyncMtime"]) != 1 {
		t.Errorf("file attrs: %v", fileAttrs)
	}

	// Nothing on the path is durable until it has been flushed.
	refs := append([]api.BlobRef{a.File}, a.Claims...)
	d, err := e.srv.Durable(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ok := range d {
		if ok {
			t.Errorf("%s durable before a flush", refs[i])
		}
	}
	waitDurable(t, e, refs)

	// PathsAt walks the same tree as of a time.
	old, err := e.srv.PathsAt(ctx, root, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 2 || old["2026/trip/a.jpg"].File != v2 {
		t.Errorf("PathsAt now: %v", old)
	}
}
