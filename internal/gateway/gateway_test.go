package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
	"github.com/progrium/keepsync/internal/pkserver"
)

// env is a server with an archive (one kept permanode with one file) and
// a node running a gateway to it.
type env struct {
	srv     *pkserver.Server
	gw      *Gateway
	token   string
	node    *fake.Transport
	pn      api.BlobRef // the kept permanode
	file    api.BlobRef // the file schema of a.txt
	content string      // a.txt now
	old     string      // a.txt as of before
	before  time.Time   // between the two versions of a.txt
	nested  string      // sub/dir/b.txt
	search  string      // the search handler's root, from discovery

	mu     sync.Mutex
	scopes map[api.NodeKey]string
}

func (e *env) grant(scope string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scopes[e.node.Key()] = scope
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "server")
	if _, err := pkserver.Init(dir); err != nil {
		t.Fatal(err)
	}
	zfs := &fake.ZeroFS{DirPath: filepath.Join(root, "zerofs")}
	if err := zfs.Start(ctx); err != nil {
		t.Fatal(err)
	}
	nw := fake.NewNetwork()
	server := nw.Transport("server")
	e := &env{node: nw.Transport("node"), scopes: map[api.NodeKey]string{}, content: "hello from the archive\n",
		old: "first version\n", nested: "deep inside\n"}
	server.Allow(e.node.Key())
	e.grant(pkserver.ScopeBlobs)
	srv, err := pkserver.Start(ctx, pkserver.Config{
		Dir: dir, ZeroFS: zfs, Transport: server, Logf: t.Logf,
		Scope: func(k api.NodeKey) string {
			e.mu.Lock()
			defer e.mu.Unlock()
			return e.scopes[k]
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	e.srv = srv

	// The keeper's path: the node puts the file, the server signs it.
	putter, err := pkserver.NewPutter(e.node, server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	put := func(name, body string) api.BlobRef {
		t.Helper()
		p := filepath.Join(root, "files", name)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		ref, err := putter.PutFile(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	claim := func(path string, file api.BlobRef, mtime time.Time) {
		t.Helper()
		if r, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: e.pn, Path: path, File: file, ModTime: mtime}); err != nil || r != api.ClaimWritten {
			t.Fatalf("claim %s: %v %v", path, r, err)
		}
	}
	if e.pn, err = srv.NewPermanode(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().Add(-time.Hour)
	claim("a.txt", put("v1/a.txt", e.old), t0)
	time.Sleep(20 * time.Millisecond)
	e.before = time.Now()
	time.Sleep(20 * time.Millisecond)
	e.file = put("v2/a.txt", e.content)
	claim("a.txt", e.file, t0.Add(time.Minute))
	claim("sub/dir/b.txt", put("b.txt", e.nested), t0)

	if e.token, err = LoadToken(filepath.Join(root, "node", "gateway-token")); err != nil {
		t.Fatal(err)
	}
	e.gw, err = New(Config{Listen: "127.0.0.1:0", Transport: e.node, Server: server.Addr(), Token: e.token, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.gw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.gw.Close() })

	// Discovery is in every scope.
	code, body := e.do(t, "GET", "/?camli.mode=config", "", "")
	var d struct{ SearchRoot string }
	if err := json.Unmarshal([]byte(body), &d); code != 200 || err != nil || d.SearchRoot == "" {
		t.Fatalf("discovery: %d %v %.300s", code, err, body)
	}
	e.search = d.SearchRoot
	return e
}

// do sends a request to the gateway with the token, and returns the
// status and body.
func (e *env) do(t *testing.T, method, path, contentType, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, e.gw.URL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.SetBasicAuth(User, e.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

const searchQuery = `{"constraint":{"camliType":"permanode"},"describe":{"depth":1}}`

func TestBlobsScopeRefusedSearchAndUI(t *testing.T) {
	e := newEnv(t)
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/ui/", ""},
		{"GET", "/ui/?p=" + string(e.pn), ""},
		{"GET", "/ui/download/" + string(e.file) + "/a.txt", ""},
		{"POST", e.search + "camli/search/query", searchQuery},
		{"GET", e.search + "camli/search/recent", ""},
		{"GET", "/bs/camli/enumerate-blobs", ""},
		{"GET", "/status/", ""},
	} {
		if code, body := e.do(t, c.method, c.path, "application/json", c.body); code != http.StatusForbidden {
			t.Errorf("blobs scope: %s %s: %d, want 403: %.200s", c.method, c.path, code, body)
		}
	}
	// Fetching a blob is in the blobs scope.
	if code, body := e.do(t, "GET", "/bs/camli/"+string(e.file), "", ""); code != 200 || !strings.Contains(body, `"camliType": "file"`) {
		t.Errorf("blobs scope fetch: %d %.200s", code, body)
	}
}

func TestReadScopeServesUIAndSearch(t *testing.T) {
	e := newEnv(t)
	e.grant(pkserver.ScopeRead)

	// The web UI page, its config and its scripts.
	code, body := e.do(t, "GET", "/ui/", "", "")
	if code != 200 || !strings.Contains(body, "camli.mode=config") {
		t.Fatalf("GET /ui/: %d %.300s", code, body)
	}
	code, body = e.do(t, "GET", "/ui/?camli.mode=config&var=CAMLISTORE_CONFIG", "", "")
	if code != 200 || !strings.Contains(body, "searchRoot") {
		t.Fatalf("UI config: %d %.300s", code, body)
	}
	if strings.Contains(body, `"authToken": "`) && !strings.Contains(body, `"authToken": ""`) {
		t.Fatalf("UI config leaks the server's auth token: %.500s", body)
	}

	// The search the UI runs, which finds the kept permanode.
	var res struct {
		Blobs []struct{ Blob string }
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, body = e.do(t, "POST", e.search+"camli/search/query", "application/json", searchQuery)
		if code != 200 {
			t.Fatalf("search: %d %.300s", code, body)
		}
		json.Unmarshal([]byte(body), &res)
		if len(res.Blobs) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(body, string(e.pn)) || !strings.Contains(body, "camliPath:a.txt") {
		t.Fatalf("search did not find the permanode %s: %.500s", e.pn, body)
	}

	// Downloads, as the UI's file links make them.
	if code, body = e.do(t, "GET", "/ui/download/"+string(e.file)+"/a.txt", "", ""); code != 200 || body != e.content {
		t.Fatalf("download: %d %q", code, body)
	}
	if code, _ = e.do(t, "GET", "/bs/camli/enumerate-blobs", "", ""); code != 200 {
		t.Fatalf("enumerate: %d", code)
	}

	// The UI's live-search WebSocket.
	conn, err := net.Dial("tcp", e.gw.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req, _ := http.NewRequest("GET", e.gw.URL()+e.search+"camli/search/ws?authtoken=", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", e.gw.URL())
	req.SetBasicAuth(User, e.token)
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line := make([]byte, 64)
	n, _ := conn.Read(line)
	if !bytes.HasPrefix(line[:n], []byte("HTTP/1.1 101")) {
		t.Fatalf("websocket: %q", line[:n])
	}
}

func TestReadScopeCannotWrite(t *testing.T) {
	e := newEnv(t)
	e.grant(pkserver.ScopeRead)
	for _, c := range []struct{ method, path, body string }{
		{"POST", "/sighelper/camli/sig/sign", "json=%7B%7D"},
		{"POST", "/bs/camli/upload", ""},
		{"PUT", "/bs/camli/sha224-00000000000000000000000000000000000000000000000000000000", "x"},
		{"POST", "/bs/camli/remove", "blob1=" + string(e.file)},
		{"POST", "/ui/?camli.mode=uploadhelper", ""},
		{"POST", "/importer/", ""},
	} {
		if code, body := e.do(t, c.method, c.path, "application/x-www-form-urlencoded", c.body); code < 400 {
			t.Errorf("%s %s: %d, want refused: %.200s", c.method, c.path, code, body)
		}
	}
	// The server refuses signing to read nodes even without the gateway.
	if code := e.srvDirect(t, "POST", "/sighelper/camli/sig/sign", "json=%7B%7D"); code < 400 {
		t.Errorf("direct sign: %d, want refused", code)
	}
	if code := e.srvDirect(t, "GET", "/debug/config", ""); code != http.StatusForbidden {
		t.Errorf("direct debug: %d, want 403", code)
	}
}

// srvDirect makes a request to the server over the node's transport, not
// through the gateway's own checks.
func (e *env) srvDirect(t *testing.T, method, path, body string) int {
	t.Helper()
	c := &http.Client{Transport: e.gw.proxy.Transport}
	req, _ := http.NewRequest(method, upstream+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res.StatusCode
}

func TestGatewayAuth(t *testing.T) {
	e := newEnv(t)
	e.grant(pkserver.ScopeRead)
	get := func(mod func(*http.Request)) int {
		req, _ := http.NewRequest("GET", e.gw.URL()+"/ui/", nil)
		mod(req)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if c := get(func(r *http.Request) {}); c != http.StatusUnauthorized {
		t.Errorf("no token: %d", c)
	}
	if c := get(func(r *http.Request) { r.SetBasicAuth(User, "wrong-token-wrong-token") }); c != http.StatusUnauthorized {
		t.Errorf("bad token: %d", c)
	}
	if c := get(func(r *http.Request) { r.SetBasicAuth(User, e.token); r.Host = "evil.example:3179" }); c != http.StatusForbidden {
		t.Errorf("rebinding host: %d", c)
	}
	if c := get(func(r *http.Request) { r.SetBasicAuth(User, e.token); r.Header.Set("Origin", "http://evil.example") }); c != http.StatusForbidden {
		t.Errorf("cross origin: %d", c)
	}
	if c := get(func(r *http.Request) { r.SetBasicAuth(User, e.token) }); c != 200 {
		t.Errorf("good token: %d", c)
	}
	if _, err := New(Config{Listen: "0.0.0.0:3179", Transport: e.node, Server: "x", Token: e.token}); err == nil {
		t.Error("listening on all interfaces was allowed")
	}
	t2, err := LoadToken(filepath.Join(t.TempDir(), "tok"))
	if err != nil || len(t2) < 32 || t2 == e.token {
		t.Errorf("LoadToken: %q %v", t2, err)
	}
}

func TestWriteClientConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perkeep", "client-config.json")
	other := `{"identity": "ABCD", "servers": {"home": {"server": "https://pk.example", "auth": "userpass:a:b", "default": true}}}`
	os.MkdirAll(filepath.Dir(path), 0o700)
	os.WriteFile(path, []byte(other), 0o600)
	isDefault, err := WriteClientConfig(path, "http://127.0.0.1:3179", "tok")
	if err != nil || isDefault {
		t.Fatalf("existing default: %v %v", isDefault, err)
	}
	var conf struct {
		Identity string
		Servers  map[string]struct {
			Server, Auth string
			Default      bool
		}
	}
	b, _ := os.ReadFile(path)
	if err := json.Unmarshal(b, &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Identity != "ABCD" || !conf.Servers["home"].Default || conf.Servers[ServerAlias].Auth != "userpass:keepsync:tok" {
		t.Fatalf("merged config: %s", b)
	}
	fresh := filepath.Join(t.TempDir(), "client-config.json")
	if isDefault, err := WriteClientConfig(fresh, "http://127.0.0.1:3179", "tok"); err != nil || !isDefault {
		t.Fatalf("fresh config: %v %v", isDefault, err)
	}
}

// buildTools builds the stock Perkeep tools (go.mod's tool directive).
func buildTools(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds pk-get and pk-mount")
	}
	bin := t.TempDir()
	cmd := exec.Command("go", "build", "-o", bin+string(filepath.Separator), "perkeep.org/cmd/pk-get", "perkeep.org/cmd/pk-mount")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building Perkeep tools: %v\n%s", err, out)
	}
	return bin
}

// toolEnv is the environment for a stock tool using the client config
// that WriteClientConfig writes.
func (e *env) toolEnv(t *testing.T) []string {
	t.Helper()
	confDir := t.TempDir()
	if _, err := WriteClientConfig(filepath.Join(confDir, "client-config.json"), e.gw.URL(), e.token); err != nil {
		t.Fatal(err)
	}
	return append(os.Environ(),
		"CAMLI_CONFIG_DIR="+confDir,
		"CAMLI_CACHE_DIR="+t.TempDir(),
		"CAMLI_VAR_DIR="+t.TempDir(),
	)
}

func TestStockTools(t *testing.T) {
	bin := buildTools(t)
	e := newEnv(t)
	e.grant(pkserver.ScopeRead)
	env := e.toolEnv(t)

	t.Run("pk-get", func(t *testing.T) {
		cmd := exec.Command(filepath.Join(bin, "pk-get"), "-contents", string(e.file))
		cmd.Env = env
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("pk-get: %v: %s", err, stderr(err))
		}
		if string(out) != e.content {
			t.Fatalf("pk-get -contents: %q", out)
		}
	})

	t.Run("pk-mount", func(t *testing.T) {
		if _, err := os.Stat("/dev/fuse"); err != nil {
			t.Skip("no /dev/fuse")
		}
		fusermount, err := exec.LookPath("fusermount3")
		if err != nil {
			if fusermount, err = exec.LookPath("fusermount"); err != nil {
				t.Skip("no fusermount")
			}
		}
		mnt := t.TempDir()
		cmd := exec.Command(filepath.Join(bin, "pk-mount"), mnt)
		cmd.Env = env
		var log bytes.Buffer
		cmd.Stdout, cmd.Stderr = &log, &log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			exec.Command(fusermount, "-u", mnt).Run()
			cmd.Process.Kill()
			cmd.Wait()
		}()
		// The kept folder, as pk-mount's roots/ and at/ show it.
		files := map[string]string{
			filepath.Join("roots", "docs", "a.txt"):                                       e.content,
			filepath.Join("roots", "docs", "sub", "dir", "b.txt"):                         e.nested,
			filepath.Join("at", e.before.UTC().Format(time.RFC3339Nano), "docs", "a.txt"): e.old,
			string(e.file): e.content, // and any file by blobref
		}
		for rel, content := range files {
			want := filepath.Join(mnt, rel)
			var got []byte
			var err error
			deadline := time.Now().Add(30 * time.Second)
			for {
				got, err = os.ReadFile(want)
				if err == nil || time.Now().After(deadline) {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("reading %s through pk-mount: %v\n%s", rel, err, log.String())
			}
			if string(got) != content {
				t.Errorf("pk-mount %s: %q, want %q", rel, got, content)
			}
		}
		// at/ shows nothing kept after then.
		if _, err := os.Stat(filepath.Join(mnt, "at", e.before.UTC().Format(time.RFC3339Nano), "docs", "sub")); err == nil {
			t.Error("at/ before sub/ was kept shows sub/")
		}
	})

	t.Run("blobs scope", func(t *testing.T) {
		// pk-get still fetches blobs; it's search and the UI that are
		// refused (TestBlobsScopeRefusedSearchAndUI).
		e.grant(pkserver.ScopeBlobs)
		defer e.grant(pkserver.ScopeRead)
		cmd := exec.Command(filepath.Join(bin, "pk-get"), "-contents", string(e.file))
		cmd.Env = env
		if out, err := cmd.Output(); err != nil || string(out) != e.content {
			t.Fatalf("pk-get with blobs scope: %q %v: %s", out, err, stderr(err))
		}
	})
}

func stderr(err error) string {
	if ee, ok := err.(*exec.ExitError); ok {
		return string(ee.Stderr)
	}
	return fmt.Sprint(err)
}

// walk reads the kept tree under permanode pn through the gateway, as
// pk-mount and the web UI do: describe each permanode, follow camliPath
// to child permanodes, and download each file's camliContent. at, if not
// zero, reads the tree as of then. It returns path → contents.
func (e *env) walk(t *testing.T, pn string, at time.Time) map[string]string {
	t.Helper()
	out := map[string]string{}
	var visit func(pn, prefix string)
	visit = func(pn, prefix string) {
		req := map[string]any{"blobref": pn, "depth": 3}
		if !at.IsZero() {
			req["at"] = at.UTC().Format(time.RFC3339Nano)
		}
		body, _ := json.Marshal(req)
		code, res := e.do(t, "POST", e.search+"camli/search/describe", "application/json", string(body))
		if code != 200 {
			t.Fatalf("describe %s: %d %.300s", pn, code, res)
		}
		var d struct {
			Meta map[string]struct {
				Permanode *struct{ Attr map[string][]string }
			}
		}
		if err := json.Unmarshal([]byte(res), &d); err != nil {
			t.Fatal(err)
		}
		self := d.Meta[pn]
		if self.Permanode == nil {
			t.Fatalf("%s (%q) is not described as a permanode", pn, prefix)
		}
		for k, v := range self.Permanode.Attr {
			name, ok := strings.CutPrefix(k, "camliPath:")
			if !ok || len(v) == 0 {
				continue
			}
			child := d.Meta[v[0]]
			if child.Permanode == nil {
				t.Errorf("%s%s: camliPath names %s, which is not a permanode", prefix, name, v[0])
				continue
			}
			if c := child.Permanode.Attr["camliContent"]; len(c) > 0 {
				code, body := e.do(t, "GET", "/ui/download/"+c[0]+"/"+name, "", "")
				if code != 200 {
					t.Fatalf("download %s%s: %d", prefix, name, code)
				}
				out[prefix+name] = body
				continue
			}
			visit(v[0], prefix+name+"/")
		}
	}
	visit(pn, "")
	return out
}

func TestKeptTreeThroughGateway(t *testing.T) {
	e := newEnv(t)
	e.grant(pkserver.ScopeRead)
	want := map[string]string{"a.txt": e.content, "sub/dir/b.txt": e.nested}
	if got := e.walk(t, string(e.pn), time.Time{}); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("tree now: %q, want %q", got, want)
	}
	old := map[string]string{"a.txt": e.old}
	if got := e.walk(t, string(e.pn), e.before); fmt.Sprint(got) != fmt.Sprint(old) {
		t.Fatalf("tree as of before: %q, want %q", got, old)
	}
	// The root is a camliRoot, which pk-mount's roots/ lists.
	code, body := e.do(t, "GET", e.search+"camli/search/permanodeattr?attr=camliRoot&value=docs", "", "")
	if code != 200 || !strings.Contains(body, string(e.pn)) {
		t.Fatalf("camliRoot search: %d %.300s", code, body)
	}
}
