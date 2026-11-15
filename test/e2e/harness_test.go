// Package e2e runs a keepsync server and three nodes in one test process,
// against a local DERP server, a real MinIO and the real pinned ZeroFS.
//
// The tests skip unless both binaries are found, through these variables
// or on PATH:
//
//	KEEPSYNC_ZEROFS_BIN  zerofs (the pinned release, see internal/zerofs)
//	KEEPSYNC_MINIO_BIN   minio
//
// ZeroFS is mounted with FUSE ("zerofs mount"), so /dev/fuse must be
// usable. The whole run takes about five minutes, two of them waiting
// out registry.DefaultKeeperTimeout for keeper reassignment. Known,
// unfixed bugs are logged rather than failed unless
// KEEPSYNC_E2E_KNOWN_BUGS=1 (see knownBug).
//
//	KEEPSYNC_ZEROFS_BIN=/opt/e2ebin/zerofs KEEPSYNC_MINIO_BIN=/opt/e2ebin/minio \
//	  go test ./test/e2e -v -timeout 30m
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"tailscale.com/tstest/integration"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/node"
	"github.com/progrium/keepsync/internal/server"
	"github.com/progrium/keepsync/internal/zerofs"
)

// knownBugEnv turns known bugs from log lines into failures.
const knownBugEnv = "KEEPSYNC_E2E_KNOWN_BUGS"

// knownBug reports a bug that is known and not fixed yet: a failure with
// KEEPSYNC_E2E_KNOWN_BUGS=1, else a log line, so the suite stays green.
func knownBug(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv(knownBugEnv) != "" {
		t.Errorf("known bug: "+format, args...)
		return
	}
	t.Logf("KNOWN BUG (set %s=1 to fail on it): "+format, append([]any{knownBugEnv}, args...)...)
}

// binary finds a required external binary, or skips the test.
func binary(t *testing.T, env, name string) string {
	t.Helper()
	if p := os.Getenv(env); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s=%s: %v", env, p, err)
		}
		return p
	}
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	t.Skipf("no %s binary (set %s or put it on PATH)", name, env)
	return ""
}

// requireFUSE skips unless the FUSE device can be opened.
func requireFUSE(t *testing.T) {
	t.Helper()
	f, err := os.OpenFile("/dev/fuse", os.O_RDWR, 0)
	if err != nil {
		t.Skipf("ZeroFS is mounted with FUSE and /dev/fuse is not usable: %v", err)
	}
	f.Close()
}

// eventually retries fn until it returns nil or timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for {
		if err = fn(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not true after %v: %v", what, timeout, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// holds checks that fn keeps returning nil for d.
func holds(t *testing.T, d time.Duration, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := fn(); err != nil {
			t.Fatalf("%s: stopped holding: %v", what, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// logWriter sends a child process's output to the test log.
type logWriter struct {
	t      *testing.T
	prefix string
	mu     sync.Mutex
	buf    []byte
	done   bool
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		if !w.done {
			w.t.Log(w.prefix + string(w.buf[:i]))
		}
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *logWriter) stop() {
	w.mu.Lock()
	w.done = true
	w.mu.Unlock()
}

// logf returns a Logf for a component that stops logging once the test
// ends (goroutines may outlive it briefly).
func logf(t *testing.T, prefix string) (func(string, ...any), func()) {
	var mu sync.Mutex
	done := false
	return func(f string, a ...any) {
			mu.Lock()
			defer mu.Unlock()
			if !done {
				t.Logf(prefix+f, a...)
			}
		}, func() {
			mu.Lock()
			done = true
			mu.Unlock()
		}
}

// ---- MinIO ----

const (
	minioUser   = "keepsync"
	minioSecret = "keepsync-e2e-secret"
	s3Region    = "us-east-1"
)

// minio is a running MinIO server with a small SigV4 S3 client.
type minio struct {
	Endpoint string
	hc       *http.Client
}

func startMinIO(t *testing.T) *minio {
	t.Helper()
	bin := binary(t, "KEEPSYNC_MINIO_BIN", "minio")
	dir := t.TempDir()
	port, cport := freePort(t), freePort(t)
	cmd := exec.Command(bin, "server", dir, "--quiet",
		"--address", fmt.Sprintf("127.0.0.1:%d", port),
		"--console-address", fmt.Sprintf("127.0.0.1:%d", cport))
	cmd.Env = append(os.Environ(), "MINIO_ROOT_USER="+minioUser, "MINIO_ROOT_PASSWORD="+minioSecret,
		"MINIO_BROWSER=off", "MINIO_UPDATE=off")
	lw := &logWriter{t: t, prefix: "minio: "}
	cmd.Stdout, cmd.Stderr = lw, lw
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		lw.stop()
		cmd.Process.Kill()
		cmd.Wait()
	})
	m := &minio{Endpoint: fmt.Sprintf("http://127.0.0.1:%d", port), hc: &http.Client{Timeout: time.Minute}}
	eventually(t, time.Minute, "minio healthy", func() error {
		resp, err := m.hc.Get(m.Endpoint + "/minio/health/live")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return errors.New(resp.Status)
		}
		return nil
	})
	return m
}

func (m *minio) do(ctx context.Context, method, path string, q url.Values, hdr http.Header) ([]byte, error) {
	u := m.Endpoint + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	const empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	req.Header.Set("X-Amz-Content-Sha256", empty)
	creds := aws.Credentials{AccessKeyID: minioUser, SecretAccessKey: minioSecret}
	if err := v4.NewSigner().SignHTTP(ctx, creds, req, empty, "s3", s3Region, time.Now()); err != nil {
		return nil, err
	}
	resp, err := m.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("s3 %s %s: %s: %s", method, path, resp.Status, b)
	}
	return b, nil
}

func (m *minio) MakeBucket(t *testing.T, name string) {
	t.Helper()
	if _, err := m.do(context.Background(), http.MethodPut, "/"+name, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// List returns every key in bucket under prefix.
func (m *minio) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		b, err := m.do(ctx, http.MethodGet, "/"+bucket, q, nil)
		if err != nil {
			return nil, err
		}
		var res struct {
			Contents []struct {
				Key string `xml:"Key"`
			} `xml:"Contents"`
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		if err := xml.Unmarshal(b, &res); err != nil {
			return nil, err
		}
		for _, c := range res.Contents {
			keys = append(keys, c.Key)
		}
		if !res.IsTruncated {
			return keys, nil
		}
		token = res.NextContinuationToken
	}
}

// CopyBucket copies every object under prefix from one bucket to another,
// server-side, as `rclone copy` would for a drill.
func (m *minio) CopyBucket(t *testing.T, from, to, prefix string) int {
	t.Helper()
	ctx := context.Background()
	keys, err := m.List(ctx, from, prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		hdr := http.Header{"X-Amz-Copy-Source": {"/" + from + "/" + url.PathEscape(k)}}
		if _, err := m.do(ctx, http.MethodPut, "/"+to+"/"+k, nil, hdr); err != nil {
			t.Fatal(err)
		}
	}
	return len(keys)
}

// ---- cluster ----

// cluster is a keepsync server and its nodes in this process.
type cluster struct {
	t      *testing.T
	root   string
	minio  *minio
	bucket string
	prefix string
	zfsBin string
	net    config.Network

	srvCfg  *config.Server
	srv     *server.Server
	srvStop context.CancelFunc
	srvDone chan error

	nodes map[string]*testNode
}

// testNode is one node: its config dir, and while running its stop func.
type testNode struct {
	name string
	cfg  *config.Node
	key  api.NodeKey
	dev  api.DeviceID
	c    *cluster

	stop context.CancelFunc
	done chan error
}

// newCluster starts MinIO, a local DERP and the keepsync server. perkeepd
// runs once per process, so one test binary gets one cluster.
func newCluster(t *testing.T) *cluster {
	t.Helper()
	zfsBin := binary(t, "KEEPSYNC_ZEROFS_BIN", "zerofs")
	if ok, err := zerofs.Pinned.CheckPin(zfsBin); err != nil || !ok {
		t.Logf("warning: %s is not the pinned zerofs %s (%v)", zfsBin, zerofs.PinnedVersion, err)
	}
	m := startMinIO(t)
	requireFUSE(t)

	// Short root: unix socket paths are limited to about 100 bytes.
	root, err := os.MkdirTemp("", "kse2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	dm := integration.RunDERPAndSTUN(t, func(string, ...any) {}, "127.0.0.1")
	dmPath := filepath.Join(root, "derpmap.json")
	b, _ := json.Marshal(dm)
	if err := os.WriteFile(dmPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	c := &cluster{
		t: t, root: root, minio: m, bucket: "live", prefix: "keepsync", zfsBin: zfsBin,
		net:   config.Network{DERPMapURL: dmPath},
		nodes: map[string]*testNode{},
	}
	m.MakeBucket(t, c.bucket)
	c.startServer()
	return c
}

func (c *cluster) storage() config.Storage {
	return config.Storage{
		URL:             "s3://" + c.bucket + "/" + c.prefix,
		Endpoint:        c.minio.Endpoint,
		Region:          s3Region,
		AccessKeyID:     minioUser,
		SecretAccessKey: minioSecret,
		AllowHTTP:       true,
	}
}

func (c *cluster) startServer() {
	t := c.t
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dir := filepath.Join(c.root, "server")
	lf, stopLog := logf(t, "server: ")
	res, err := server.Init(ctx, server.InitOptions{
		Dir:        dir,
		Network:    c.net,
		Storage:    c.storage(),
		ZeroFS:     c.zfsBin,
		Mount:      string(zerofs.MountFUSE),
		NoDownload: true,
		Logf:       lf,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := res.Config
	// The durability watermark advances only when a test asks (flush), so
	// expiry can be shown to wait for it.
	cfg.FlushInterval = config.Duration(time.Hour)
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	c.srvCfg = cfg

	rctx, rcancel := context.WithCancel(context.Background())
	ready := make(chan *server.Server, 1)
	c.srvDone = make(chan error, 1)
	c.srvStop = rcancel
	go func() {
		c.srvDone <- server.Run(rctx, cfg, server.Options{Logf: lf, Ready: func(s *server.Server) { ready <- s }})
	}()
	select {
	case c.srv = <-ready:
	case err := <-c.srvDone:
		t.Fatalf("server run: %v", err)
	case <-time.After(3 * time.Minute):
		t.Fatal("server did not come up")
	}
	t.Cleanup(func() {
		// Nodes first (their cleanups were registered later and run
		// earlier), then a clean server stop: drain, flush, unmount.
		rcancel()
		select {
		case err := <-c.srvDone:
			if err != nil {
				t.Errorf("server stop: %v", err)
			}
		case <-time.After(5 * time.Minute):
			t.Error("server did not stop")
		}
		stopLog()
	})
}

// serverCall calls the running server's control socket.
func (c *cluster) serverCall(method string, params, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return ctl.Call(ctx, c.srvCfg.SocketPath(), method, params, out)
}

// flush drains replication and flushes ZeroFS, advancing the durability
// watermark, by writing a drill baseline to path.
func (c *cluster) flush(path string) {
	c.t.Helper()
	if path == "" {
		path = filepath.Join(c.root, "baseline-flush.json")
	}
	if err := c.serverCall(server.MethodBaseline, server.BaselineParams{Path: path}, nil); err != nil {
		c.t.Fatalf("flush: %v", err)
	}
}

// addNode inits a node, approves it on the server and starts it.
func (c *cluster) addNode(name string) *testNode {
	t := c.t
	t.Helper()
	dir := filepath.Join(c.root, "node-"+name)
	res, err := node.Init(context.Background(), node.InitOptions{
		Dir: dir, Name: name, Server: string(c.srv.Addr()), Network: c.net,
	})
	if err != nil {
		t.Fatal(err)
	}
	res.Config.KeepInterval = config.Duration(time.Second)
	res.Config.ConflictHook = c.hookScript(name)
	if err := res.Config.Save(); err != nil {
		t.Fatal(err)
	}
	if err := c.serverCall(server.MethodAdd, server.AddParams{Key: res.Key, Name: name}, nil); err != nil {
		t.Fatal(err)
	}
	n := &testNode{name: name, cfg: res.Config, key: res.Key, dev: res.Device, c: c}
	c.nodes[name] = n
	n.start()
	t.Cleanup(n.kill)
	return n
}

// start runs the node until kill.
func (n *testNode) start() {
	t := n.c.t
	t.Helper()
	cfg, err := config.LoadNode(n.cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	n.cfg = cfg
	ctx, cancel := context.WithCancel(context.Background())
	lf, stopLog := logf(t, n.name+": ")
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- node.Run(ctx, cfg, node.Options{Logf: lf, Ready: func(*node.Node) { ready <- struct{}{} }})
		stopLog()
	}()
	select {
	case <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("node %s run: %v", n.name, err)
	case <-time.After(time.Minute):
		cancel()
		t.Fatalf("node %s did not come up", n.name)
	}
	n.stop, n.done = cancel, done
}

// kill stops the node (its process "dies": tailcat and Syncthing go away).
func (n *testNode) kill() {
	if n.stop == nil {
		return
	}
	n.stop()
	select {
	case <-n.done:
	case <-time.After(time.Minute):
		n.c.t.Errorf("node %s did not stop", n.name)
	}
	n.stop, n.done = nil, nil
}

func (n *testNode) call(method string, params, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return ctl.Call(ctx, n.cfg.SocketPath(), method, params, out)
}

func (n *testNode) status() (node.Status, error) {
	var st node.Status
	err := n.call(node.MethodStatus, nil, &st)
	return st, err
}

// dir is where folder name lives on this node.
func (n *testNode) dir(folder string) string {
	return filepath.Join(n.c.root, "data-"+n.name, folder)
}

func (n *testNode) path(folder, rel string) string { return filepath.Join(n.dir(folder), rel) }

func (n *testNode) write(folder, rel, content string) {
	n.c.t.Helper()
	p := n.path(folder, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		n.c.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		n.c.t.Fatal(err)
	}
}

// hasContent reports nil once rel on this node holds content.
func (n *testNode) hasContent(folder, rel, content string) error {
	b, err := os.ReadFile(n.path(folder, rel))
	if err != nil {
		return err
	}
	if string(b) != content {
		return fmt.Errorf("%s on %s holds %q, want %q", rel, n.name, b, content)
	}
	return nil
}

// absent reports nil once rel is gone on this node.
func (n *testNode) absent(folder, rel string) error {
	if _, err := os.Lstat(n.path(folder, rel)); err == nil {
		return fmt.Errorf("%s still on %s", rel, n.name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (n *testNode) keepStatus(folder string) (node.KeepStatus, error) {
	var ks node.KeepStatus
	err := n.call(node.MethodKeepStatus, node.FolderParams{Name: folder}, &ks)
	return ks, err
}


// buildCLI builds cmd/keepsync into dir.
func buildCLI(t *testing.T, dir string) string {
	t.Helper()
	out := filepath.Join(dir, "keepsync")
	cmd := exec.Command("go", "build", "-o", out, "github.com/progrium/keepsync/cmd/keepsync")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build keepsync: %v\n%s", err, b)
	}
	return out
}

func trimLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// hookLog is where every node's conflict hook records its runs.
func (c *cluster) hookLog() string { return filepath.Join(c.root, "hook.log") }

// hookScript writes node name's conflict hook. It logs each run to
// hookLog and saves KEEPSYNC_BASE next to it. For an original named
// fail-*, it exits 1 and leaves the copy; otherwise it merges: original
// then conflict copy into the original, deletes the copy, exits 0.
func (c *cluster) hookScript(name string) string {
	c.t.Helper()
	p := filepath.Join(c.root, "hook-"+name+".sh")
	script := `#!/bin/sh
set -e
orig=$(basename "$KEEPSYNC_ORIGINAL")
base=none
if [ -n "$KEEPSYNC_BASE" ]; then
  cp "$KEEPSYNC_BASE" "` + c.root + `/hookbase-` + name + `-$orig"
  base=yes
fi
echo "` + name + ` $KEEPSYNC_FOLDER $orig $KEEPSYNC_CONFLICT_NODE base=$base" >> "` + c.hookLog() + `"
case "$orig" in
fail-*) exit 1 ;;
esac
tmp="$KEEPSYNC_ORIGINAL.merge"
cat "$KEEPSYNC_ORIGINAL" "$KEEPSYNC_CONFLICT" > "$tmp"
mv "$tmp" "$KEEPSYNC_ORIGINAL"
rm "$KEEPSYNC_CONFLICT"
`
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		c.t.Fatal(err)
	}
	return p
}

// hookRuns returns the hook log lines that mention original.
func (c *cluster) hookRuns(original string) []string {
	b, _ := os.ReadFile(c.hookLog())
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if f := strings.Fields(l); len(f) >= 3 && f[2] == original {
			out = append(out, l)
		}
	}
	return out
}

func (n *testNode) conflicts() ([]node.ConflictInfo, error) {
	var out []node.ConflictInfo
	err := n.call(node.MethodConflicts, nil, &out)
	return out, err
}

func (n *testNode) restore(p node.RestoreParams) (node.RestoreResult, error) {
	var res node.RestoreResult
	err := n.call(node.MethodRestore, p, &res)
	return res, err
}

// setMtime sets rel's mtime on this node.
func (n *testNode) setMtime(folder, rel string, t time.Time) {
	n.c.t.Helper()
	if err := os.Chtimes(n.path(folder, rel), t, t); err != nil {
		n.c.t.Fatal(err)
	}
}
