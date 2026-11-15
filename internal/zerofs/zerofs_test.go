package zerofs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeExe(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestFindBinaryOrder(t *testing.T) {
	root := t.TempDir()
	explicit := filepath.Join(root, "explicit", "zerofs")
	onPath := filepath.Join(root, "path", "zerofs")
	dataDir := filepath.Join(root, "data")
	local := LocalPath(dataDir)
	for _, p := range []string{explicit, onPath, local} {
		writeExe(t, p)
	}
	t.Setenv("PATH", filepath.Dir(onPath))

	check := func(name, explicit, want string) {
		t.Helper()
		got, err := FindBinary(explicit, dataDir)
		if err != nil || got != want {
			t.Fatalf("%s: got %q, %v; want %q", name, got, err, want)
		}
	}
	check("explicit wins", explicit, explicit)
	check("then PATH", "", onPath)
	t.Setenv("PATH", filepath.Join(root, "empty"))
	check("then data dir", "", local)

	if _, err := FindBinary(filepath.Join(root, "missing"), dataDir); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("missing explicit path: want a specific error, got %v", err)
	}
	os.Remove(local)
	if _, err := FindBinary("", dataDir); !errors.Is(err, ErrNotFound) {
		t.Fatalf("nothing anywhere: got %v, want ErrNotFound", err)
	}
}

func sha(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// testRelease builds a tarball holding a binary for this platform and one
// for another, and a Release describing it.
func testRelease(t *testing.T, bin []byte) (Release, []byte) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	add := func(name string, data []byte) {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		tw.Write(data)
	}
	add("zerofs-other-pgo", []byte("other platform"))
	add("zerofs-this-pgo", bin)
	tw.Close()
	gz.Close()
	tarball := buf.Bytes()
	return Release{
		Version:    "vtest",
		TarballSHA: sha(tarball),
		Binaries:   map[string]Binary{Platform(): {Name: "zerofs-this-pgo", SHA256: sha(bin)}},
	}, tarball
}

func TestInstall(t *testing.T) {
	bin := []byte("#!/bin/sh\necho zerofs\n")
	rel, tarball := testRelease(t, bin)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(tarball) }))
	defer srv.Close()
	rel.URL = srv.URL

	t.Run("ok", func(t *testing.T) {
		dataDir := t.TempDir()
		got, err := rel.Install(context.Background(), srv.Client(), dataDir)
		if err != nil {
			t.Fatal(err)
		}
		if got != LocalPath(dataDir) {
			t.Fatalf("installed at %s", got)
		}
		b, _ := os.ReadFile(got)
		if !bytes.Equal(b, bin) {
			t.Fatal("wrong contents")
		}
		if st, _ := os.Stat(got); st.Mode().Perm()&0o100 == 0 {
			t.Fatal("not executable")
		}
		if ok, err := rel.CheckPin(got); !ok || err != nil {
			t.Fatalf("CheckPin = %v, %v", ok, err)
		}
	})
	t.Run("tarball hash mismatch", func(t *testing.T) {
		bad := rel
		bad.TarballSHA = strings.Repeat("0", 64)
		dataDir := t.TempDir()
		if _, err := bad.Install(context.Background(), srv.Client(), dataDir); err == nil || !strings.Contains(err.Error(), "tarball SHA-256") {
			t.Fatalf("got %v", err)
		}
		assertNoInstall(t, dataDir)
	})
	t.Run("binary hash mismatch", func(t *testing.T) {
		bad := rel
		bad.Binaries = map[string]Binary{Platform(): {Name: "zerofs-this-pgo", SHA256: strings.Repeat("0", 64)}}
		dataDir := t.TempDir()
		if _, err := bad.Install(context.Background(), srv.Client(), dataDir); err == nil || !strings.Contains(err.Error(), "zerofs-this-pgo SHA-256") {
			t.Fatalf("got %v", err)
		}
		assertNoInstall(t, dataDir)
	})
	t.Run("unsupported platform", func(t *testing.T) {
		bad := rel
		bad.Binaries = nil
		if _, err := bad.Install(context.Background(), srv.Client(), t.TempDir()); !errors.Is(err, ErrUnsupportedPlatform) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("http error", func(t *testing.T) {
		bad := rel
		bad.URL = srv.URL + "/missing"
		srv404 := httptest.NewServer(http.NotFoundHandler())
		defer srv404.Close()
		bad.URL = srv404.URL
		if _, err := bad.Install(context.Background(), nil, t.TempDir()); err == nil {
			t.Fatal("want error")
		}
	})
}

func assertNoInstall(t *testing.T, dataDir string) {
	t.Helper()
	entries, _ := os.ReadDir(filepath.Dir(LocalPath(dataDir)))
	if len(entries) != 0 {
		t.Fatalf("left files behind: %v", entries)
	}
}

func TestPinnedCoversServerPlatforms(t *testing.T) {
	for _, p := range []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64", "freebsd/amd64"} {
		if b, ok := Pinned.Binaries[p]; !ok || len(b.SHA256) != 64 {
			t.Errorf("no pinned binary for %s", p)
		}
	}
	if !strings.Contains(Pinned.URL, Pinned.Version) {
		t.Errorf("URL %s does not name version %s", Pinned.URL, Pinned.Version)
	}
}

func TestPasswords(t *testing.T) {
	a, err := GeneratePassword()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := GeneratePassword()
	if len(a) != 2*PasswordBytes || a == b {
		t.Fatalf("generated %q and %q", a, b)
	}
	if err := ValidatePassword(a); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "pw")
	os.WriteFile(p, []byte("existing-poc-password\n"), 0o600)
	got, err := ImportPassword(p)
	if err != nil || got != "existing-poc-password" {
		t.Fatalf("import = %q, %v", got, err)
	}
	for _, bad := range []string{"", "short\n", "CHANGEME"} {
		os.WriteFile(p, []byte(bad), 0o600)
		if _, err := ImportPassword(p); err == nil {
			t.Errorf("import %q: want error", bad)
		}
	}
}

func TestRenderConfig(t *testing.T) {
	cfg := renderConfig(configParams{
		Storage: Storage{
			URL: "s3://bucket/keepsync", Endpoint: "http://127.0.0.1:9000", Region: "auto",
			AccessKeyID: "AKID-SECRET-VALUE", SecretAccessKey: "SECRET-VALUE", AllowHTTP: true,
		},
		CacheDir: "/data/cache", CacheSizeGB: 10, MemoryGB: 0.5,
		NinePSocket: "/data/run/9p.sock", RPCSocket: "/data/run/rpc.sock",
	})
	for _, want := range []string{
		"[cache]\ndir = \"/data/cache\"\ndisk_size_gb = 10\nmemory_size_gb = 0.5\n",
		"url = \"s3://bucket/keepsync\"\nencryption_password = \"${KEEPSYNC_ZEROFS_PASSWORD}\"",
		"[servers.ninep]\nunix_socket = \"/data/run/9p.sock\"",
		"[servers.rpc]\nunix_socket = \"/data/run/rpc.sock\"",
		"[telemetry]\nenabled = false",
		"access_key_id = \"${KEEPSYNC_ZEROFS_ACCESS_KEY_ID}\"",
		"endpoint = \"http://127.0.0.1:9000\"",
		"default_region = \"auto\"",
		"allow_http = \"true\"",
	} {
		if !strings.Contains(cfg, want) {
			t.Errorf("config missing %q:\n%s", want, cfg)
		}
	}
	for _, never := range []string{"SECRET-VALUE", "servers.nfs", "servers.nbd", "webui"} {
		if strings.Contains(cfg, never) {
			t.Errorf("config contains %q:\n%s", never, cfg)
		}
	}
}

func TestNewRejects(t *testing.T) {
	ok := Config{Binary: "zerofs", DataDir: t.TempDir(), Storage: Storage{URL: "s3://b/p"}, Password: "0123456789"}
	if _, err := New(ok); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Config){
		"no binary":   func(c *Config) { c.Binary = "" },
		"no data dir": func(c *Config) { c.DataDir = "" },
		"no url":      func(c *Config) { c.Storage.URL = "" },
		"bad pw":      func(c *Config) { c.Password = "short" },
		"dollar":      func(c *Config) { c.Storage.URL = "s3://b/$HOME" },
		"bad mode":    func(c *Config) { c.Mount = "nfs" },
	} {
		c := ok
		mut(&c)
		if _, err := New(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func readEvents(t *testing.T, path string) []string {
	t.Helper()
	b, _ := os.ReadFile(path)
	return strings.Fields(string(b))
}

func TestSupervisorLifecycle(t *testing.T) {
	ctx := context.Background()
	s, events := fakeSupervisor(t)
	m := s.mount.(*dirMount)

	if err := s.Flush(ctx); err == nil {
		t.Fatal("Flush before Start: want error")
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(ctx); err == nil {
		t.Fatal("second Start: want error")
	}
	if err := os.WriteFile(filepath.Join(s.Dir(), "pack-00000.blobs"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	st, err := os.Stat(s.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("config mode %v, want 0600", st.Mode().Perm())
	}
	b, _ := os.ReadFile(s.ConfigPath())
	for _, secret := range []string{s.cfg.Password, "test-secret-value", "test-akid-value"} {
		if strings.Contains(string(b), secret) {
			t.Errorf("config contains secret %q", secret)
		}
	}
	if st, _ := os.Stat(filepath.Dir(s.ninePSocket)); st.Mode().Perm() != 0o700 {
		t.Errorf("socket dir mode %v, want 0700", st.Mode().Perm())
	}

	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if h := s.Health(); h.State != HealthStopped {
		t.Fatalf("health after Stop = %+v", h)
	}
	// Stop flushes before signalling.
	if got := strings.Join(readEvents(t, events), " "); got != "run flush flush term" {
		t.Fatalf("events = %q", got)
	}
	if m.mounts != 1 || m.unmounts != 1 {
		t.Fatalf("mounts=%d unmounts=%d", m.mounts, m.unmounts)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	// Restart against the same data dir, as server run does after a stop.
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.Dir(), "pack-00000.blobs")); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func killFake(t *testing.T, s *Supervisor) {
	t.Helper()
	c, err := dialUnix(s.rpcSocket)
	if err != nil {
		t.Fatal(err)
	}
	c.Write([]byte("die\n"))
	c.Close()
}

func waitHealth(t *testing.T, s *Supervisor, state string) Health {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		h := s.Health()
		if h.State == state {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatalf("health %+v, want %s", h, state)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSupervisorZerofsDies: a zerofs that exits is restarted and
// remounted, and the supervisor carries on.
func TestSupervisorZerofsDies(t *testing.T) {
	ctx := context.Background()
	s, events := fakeSupervisor(t)
	s.cfg.CheckInterval = 50 * time.Millisecond
	m := s.mount.(*dirMount)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	killFake(t, s)
	deadline := time.Now().Add(10 * time.Second)
	for m.mounts < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("not remounted: mounts=%d health=%+v", m.mounts, s.Health())
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitHealth(t, s, HealthUp)
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush after reconnect: %v", err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Done():
		t.Fatal("Done closed though zerofs came back")
	default:
	}
	if got := strings.Join(readEvents(t, events), " "); got != "run died run flush flush term" {
		t.Fatalf("events = %q", got)
	}
}

// TestSupervisorGivesUp: a zerofs that won't come back within the window
// leaves the supervisor down, with Flush and Stop failing and Done closed.
func TestSupervisorGivesUp(t *testing.T) {
	ctx := context.Background()
	flag := filepath.Join(t.TempDir(), "fail")
	s, _ := fakeSupervisor(t, "FAKE_FAIL_START_FILE="+flag)
	s.cfg.CheckInterval = 50 * time.Millisecond
	s.cfg.ReconnectWindow = 1500 * time.Millisecond
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(flag, nil, 0o600)
	killFake(t, s)
	h := waitHealth(t, s, HealthReconnecting)
	if err := s.Flush(ctx); err == nil || !strings.Contains(err.Error(), "reconnecting") {
		t.Fatalf("Flush while reconnecting = %v (health %+v)", err, h)
	}
	select {
	case <-s.Done():
	case <-time.After(15 * time.Second):
		t.Fatalf("Done not closed; health %+v", s.Health())
	}
	h = s.Health()
	if h.State != HealthDown || h.Attempts < 1 || !strings.Contains(h.Error(), "restart `keepsync server run`") {
		t.Fatalf("health = %+v (%s)", h, h.Error())
	}
	if err := s.Flush(ctx); err == nil || !strings.Contains(err.Error(), "gave up") {
		t.Fatalf("Flush when down = %v", err)
	}
	if err := s.Stop(ctx); err == nil {
		t.Fatal("Stop when down: want an error, data may not be flushed")
	}
}

func TestSupervisorMountFailure(t *testing.T) {
	ctx := context.Background()
	s, _ := fakeSupervisor(t)
	m := s.mount.(*dirMount)
	m.err = errors.New("transport endpoint is not connected")
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err == nil {
		t.Fatal("Flush with a dead mount: want error")
	}
	m.err = nil
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorStartFails(t *testing.T) {
	s, _ := fakeSupervisor(t, "FAKE_FAIL_START=1")
	start := time.Now()
	err := s.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exited during startup") {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("Start did not notice the exit promptly")
	}
	log, _ := os.ReadFile(filepath.Join(s.cfg.DataDir, "zerofs.log"))
	if !strings.Contains(string(log), "failing on purpose") {
		t.Fatalf("zerofs output not logged: %q", log)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop after failed Start: %v", err)
	}
}

func TestMountPointDetection(t *testing.T) {
	dir := t.TempDir()
	if m, err := isMountPoint(dir); err != nil || m {
		t.Fatalf("temp dir: %v, %v", m, err)
	}
	if err := (&externalMount{}).Mount(context.Background(), mountSpec{dir: dir}); err == nil {
		t.Fatal("external mode accepted a plain directory")
	}
	if m, err := isMountPoint("/proc"); err == nil && !m {
		if _, statErr := os.Stat("/proc/self"); statErr == nil {
			t.Fatal("/proc not detected as a mount point")
		}
	}
}
