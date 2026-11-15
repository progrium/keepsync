package kit

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/pkserver"
	"github.com/progrium/keepsync/internal/tcnet"
	"github.com/progrium/keepsync/internal/zerofs"
)

func init() { scryptWorkFactor = 10 }

const testPassword = "0123456789abcdef0123456789abcdef"

// newServerDir makes a server dir in config's layout, with a Perkeep
// identity, a tailcat key, a server.toml and a stand-in zerofs binary.
func newServerDir(t *testing.T, dir, bucket string) *config.Server {
	t.Helper()
	cfg := &config.Server{Dir: dir}
	cfg.Storage.URL = bucket
	cfg.Storage.AccessKeyID = "ro-key"
	cfg.Storage.SecretAccessKey = "ro-secret"
	cfg.ZeroFS.Password = testPassword
	cfg.ZeroFS.Binary = filepath.Join(dir, "zerofs-bin")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ZeroFS.Binary, []byte("#!/bin/sh\necho fake zerofs\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := pkserver.Init(cfg.PerkeepDir()); err != nil {
		t.Fatal(err)
	}
	if err := tcnet.SaveKey(cfg.KeyPath(), tcnet.NewKey()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.RegistryPath(), []byte(`{"members":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestBuildOpen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := newServerDir(t, filepath.Join(root, "server"), "s3://bucket/keepsync")
	out := filepath.Join(root, FileName)
	pass, err := Write(ctx, cfg, out, "tc-test-addr")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[a-z2-7]{5}(-[a-z2-7]{5}){5}$`).MatchString(pass) {
		t.Errorf("passphrase %q not in the expected form", pass)
	}
	if fi, err := os.Stat(out); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("kit file: %v %v", fi, err)
	}

	if _, err := Open(out, "wrong-pass"); !errors.Is(err, ErrPassphrase) {
		t.Fatalf("wrong passphrase: %v", err)
	}
	k, err := Open(out, pass)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	m := k.Manifest
	if m.Format != FormatVersion || m.ServerAddr != "tc-test-addr" || m.Storage.URL != "s3://bucket/keepsync" ||
		m.Storage.AccessKeyID != "ro-key" || m.ZeroFSPassword != testPassword || m.ZeroFSVersion != zerofs.PinnedVersion ||
		m.ZeroFSPinned || !m.HasConfig || !m.HasRegistry || m.PerkeepKeyID == "" {
		t.Errorf("manifest: %+v", m)
	}
	for _, e := range []string{entryManifest, entrySums, entryRecoverMD, entryKeepsync, entryZeroFS, entrySecring,
		entryPerkeepd, entryTailcatKey, entryConfig, entryRegistry, entryZFSLicense, entryZFSSource} {
		if !k.Has(e) {
			t.Errorf("kit has no %s", e)
		}
	}
	md, _ := k.read(entryRecoverMD)
	if !strings.Contains(string(md), "tc-test-addr") || !strings.Contains(string(md), "did NOT match the pin") {
		t.Errorf("RECOVER.md:\n%s", md)
	}
	lic, _ := k.read(entryZFSLicense)
	if !strings.Contains(string(lic), "GNU AFFERO GENERAL PUBLIC LICENSE") {
		t.Error("no AGPL text")
	}

	dst := filepath.Join(root, "restored", "key.json")
	if err := k.Extract(entryTailcatKey, dst, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tcnet.LoadKey(dst); err != nil {
		t.Errorf("extracted key: %v", err)
	}
	if err := k.Extract(entryTailcatKey, dst, 0o600); err == nil {
		t.Error("Extract overwrote an existing file")
	}
	tmp := k.tmp
	k.Close()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Errorf("Close left %s", tmp)
	}
}

func TestVerifyRejectsBadSums(t *testing.T) {
	root := t.TempDir()
	cases := map[string]map[string]string{
		"unlisted entry": {entrySums: "", "a": "x"},
		"wrong sum":      {entrySums: strings.Repeat("0", 64) + "  a\n", "a": "x"},
		"missing entry":  {entrySums: sha("x") + "  a\n" + sha("y") + "  b\n", "a": "x"},
	}
	for name, entries := range cases {
		path := filepath.Join(root, strings.ReplaceAll(name, " ", "-"))
		var zb bytes.Buffer
		zw := zip.NewWriter(&zb)
		for n, body := range entries {
			w, _ := zw.Create(n)
			w.Write([]byte(body))
		}
		zw.Close()
		f, _ := os.Create(path)
		r, _ := age.NewScryptRecipient("pw")
		r.SetWorkFactor(10)
		aw, _ := age.Encrypt(f, r)
		aw.Write(zb.Bytes())
		aw.Close()
		f.Close()
		if k, err := Open(path, "pw"); err == nil {
			k.Close()
			t.Errorf("%s: opened", name)
		}
	}
	if _, err := parseSums([]byte("nothex  a\n")); err == nil {
		t.Error("parseSums accepted a bad line")
	}
}

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func TestDrillRefusesLiveBucket(t *testing.T) {
	k := &Kit{Manifest: Manifest{Storage: zerofs.Storage{URL: "s3://b/p"}}}
	for _, b := range []string{"", "s3://b/p", "s3://b/p/"} {
		if _, err := Drill(context.Background(), k, DrillOptions{Bucket: b}); err == nil {
			t.Errorf("Drill(%q) ran", b)
		}
	}
}

func TestChecksReportMissingAndDiffering(t *testing.T) {
	base := &Baseline{
		Blobs: []SizedRef{{"sha224-a", 1}, {"sha224-b", 2}, {"sha224-c", 3}},
		Files: map[string]FileSum{
			"pn/x": {File: "f1", Size: 1, SHA256: "aa"},
			"pn/y": {File: "f2", Size: 1, SHA256: "bb"},
			"pn/z": {File: "f3", Size: 1, SHA256: "cc"},
		},
	}
	s := &scan{Baseline: Baseline{
		Blobs: []SizedRef{{"sha224-a", 1}, {"sha224-b", 5}, {"sha224-d", 1}},
		Files: map[string]FileSum{"pn/x": {File: "f1", Size: 1, SHA256: "aa"}, "pn/y": {File: "f2", Size: 1, SHA256: "zz"}},
	}}
	if c := blobCheck(s, base); c.OK || !strings.Contains(c.Detail, "1 of 3 live blobs missing (first sha224-c)") || !strings.Contains(c.Detail, "wrong size") {
		t.Errorf("blob check: %+v", c)
	}
	if c := fileCheck(s, base); c.OK || !strings.Contains(c.Detail, "missing (first pn/z)") || !strings.Contains(c.Detail, "1 differ") {
		t.Errorf("file check: %+v", c)
	}
	ok := &scan{Baseline: Baseline{Blobs: append(base.Blobs, SizedRef{"sha224-new", 1}), Files: base.Files}}
	if c := blobCheck(ok, base); !c.OK || !strings.Contains(c.Detail, "1 newer") {
		t.Errorf("blob check with extra: %+v", c)
	}
	if c := fileCheck(ok, base); !c.OK {
		t.Errorf("file check: %+v", c)
	}
	if c := blobCheck(&scan{BadBlobs: []api.BlobRef{"sha224-x"}}, nil); c.OK {
		t.Errorf("bad hash passed: %+v", c)
	}
}

// fakeBuckets makes a NewZeroFS whose "bucket" for URL s3://test/<name> is
// the directory root/<name>, so a copy of the bucket is a copy of a dir.
func fakeBuckets(root string) NewZeroFS {
	return func(c zerofs.Config) (api.ZeroFS, error) {
		name, ok := strings.CutPrefix(c.Storage.URL, "s3://test/")
		if !ok {
			return nil, fmt.Errorf("unexpected bucket %s", c.Storage.URL)
		}
		if c.Password != testPassword {
			return nil, errors.New("wrong zerofs password")
		}
		if _, err := os.Stat(c.Binary); err != nil {
			return nil, err
		}
		return &fake.ZeroFS{DirPath: filepath.Join(root, name)}, nil
	}
}

// TestRecoverAndDrill runs a live server, makes a kit and a baseline, then
// drills against a copy of the bucket, recovers onto a fresh dir and runs
// the recovered server. perkeepd runs once per process, so each step is a
// child process.
func TestRecoverAndDrill(t *testing.T) {
	if phase := os.Getenv("KIT_PHASE"); phase != "" {
		root := os.Getenv("KIT_ROOT")
		switch phase {
		case "live":
			phaseLive(t, root)
		case "drill":
			phaseDrill(t, root)
		case "recover":
			phaseRecover(t, root)
		case "run":
			phaseRun(t, root)
		case "corrupt":
			phaseCorrupt(t, root)
		}
		if !t.Failed() {
			t.Log("kit child ok")
		}
		return
	}
	if testing.Short() {
		t.Skip("starts perkeepd four times")
	}
	root := t.TempDir()
	child := func(phase string) string {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRecoverAndDrill$", "-test.v")
		cmd.Env = append(os.Environ(), "KIT_PHASE="+phase, "KIT_ROOT="+root)
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "kit child ok") {
			t.Fatalf("%s phase: %v\n%s", phase, err, lastLines(string(out), 40))
		}
		return string(out)
	}
	child("live")
	// The drill's copy of the bucket, as rclone copy would make it.
	if out, err := exec.Command("cp", "-a", filepath.Join(root, "buckets", "live"), filepath.Join(root, "buckets", "drill")).CombinedOutput(); err != nil {
		t.Fatalf("copy bucket: %v %s", err, out)
	}
	out := child("drill")
	_, rep, _ := strings.Cut(out, "REPORT<<")
	rep, _, _ = strings.Cut(rep, ">>REPORT")
	t.Logf("drill report:\n%s", rep)
	// A damaged copy fails the drill.
	bad := filepath.Join(root, "buckets", "corrupt")
	if out, err := exec.Command("cp", "-a", filepath.Join(root, "buckets", "live"), bad).CombinedOutput(); err != nil {
		t.Fatalf("copy bucket: %v %s", err, out)
	}
	corruptPack(t, bad)
	if out := child("corrupt"); !strings.Contains(out, "corrupt bucket: err=") {
		t.Error("corrupt phase did not report")
	} else {
		_, line, _ := strings.Cut(out, "corrupt bucket: ")
		line, _, _ = strings.Cut(line, "\n")
		t.Logf("corrupt bucket: %s", line)
	}
	child("recover")
	child("run")
}

// corruptPack flips a byte in the middle of the largest diskpacked pack.
func corruptPack(t *testing.T, dir string) {
	t.Helper()
	var biggest string
	var size int64
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(p, ".blobs") && fi.Size() > size {
			biggest, size = p, fi.Size()
		}
		return nil
	})
	if biggest == "" {
		t.Fatal("no pack file in the bucket")
	}
	f, err := os.OpenFile(biggest, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	f.ReadAt(b, size/2)
	b[0] ^= 0xff
	f.WriteAt(b, size/2)
}

func phaseCorrupt(t *testing.T, root string) {
	var c liveCase
	readJSON(t, filepath.Join(root, "case.json"), &c)
	var base Baseline
	readJSON(t, filepath.Join(root, "baseline.json"), &base)
	k, err := Open(filepath.Join(root, FileName), c.Pass)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	rep, err := Drill(context.Background(), k, DrillOptions{
		Bucket:    "s3://test/corrupt",
		Baseline:  &base,
		NewZeroFS: fakeBuckets(filepath.Join(root, "buckets")),
		Logf:      t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() || rep.Checks[0].OK || !strings.Contains(rep.Checks[0].Detail, "fail their hash check") {
		t.Fatalf("drill on a corrupt bucket:\n%s", rep)
	}
	t.Logf("corrupt bucket: err=%s", strings.ReplaceAll(rep.String(), "\n", " | "))
}

type liveCase struct {
	Pass      string
	Permanode api.BlobRef
	Want      map[string]api.PathEntry
	Addr      api.Addr
}

func phaseLive(t *testing.T, root string) {
	ctx := context.Background()
	cfg := newServerDir(t, filepath.Join(root, "live"), "s3://test/live")
	zfs := &fake.ZeroFS{DirPath: filepath.Join(root, "buckets", "live")}
	zfs.Start(ctx)
	nw := fake.NewNetwork()
	st, nt := nw.Transport("server"), nw.Transport("node")
	st.Allow(nt.Key())
	srv, err := pkserver.Start(ctx, pkserver.Config{Dir: cfg.PerkeepDir(), ZeroFS: zfs, Transport: st, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	putter, err := pkserver.NewPutter(nt, st.Addr())
	if err != nil {
		t.Fatal(err)
	}
	pn, err := srv.NewPermanode(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.NewPermanode(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	files := t.TempDir()
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	big := strings.Repeat("0123456789abcdef", 1<<16) // 1 MiB, several chunks
	for i, name := range []string{"a.txt", "dir/b.txt", "with space", "big.bin"} {
		body := name + " contents"
		if name == "big.bin" {
			body = big
		}
		p := filepath.Join(files, fmt.Sprint(i))
		os.WriteFile(p, []byte(body), 0o644)
		ref, err := putter.PutFile(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: name, File: ref, ModTime: t0.Add(time.Duration(i) * time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	// A later edit: only the newer version is current.
	p := filepath.Join(files, "edit")
	os.WriteFile(p, []byte("a.txt v2"), 0o644)
	ref, _ := putter.PutFile(ctx, p)
	if r, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "a.txt", File: ref, ModTime: t0.Add(time.Hour)}); err != nil || r != api.ClaimWritten {
		t.Fatal(r, err)
	}
	want, _ := srv.Paths(ctx, pn)

	// The drill procedure: drain and flush, snapshot, then copy the bucket.
	if err := srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := srv.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := Snapshot(ctx, srv)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Files) != 4 || len(base.Permanodes) != 2 {
		t.Fatalf("baseline: %d files, %d permanodes", len(base.Files), len(base.Permanodes))
	}
	writeJSON(t, filepath.Join(root, "baseline.json"), base)

	pass, err := Write(ctx, cfg, filepath.Join(root, FileName), st.Addr())
	if err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(root, "case.json"), liveCase{Pass: pass, Permanode: pn, Want: want, Addr: st.Addr()})
}

func phaseDrill(t *testing.T, root string) {
	ctx := context.Background()
	var c liveCase
	readJSON(t, filepath.Join(root, "case.json"), &c)
	var base Baseline
	readJSON(t, filepath.Join(root, "baseline.json"), &base)
	k, err := Open(filepath.Join(root, FileName), c.Pass)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	before := dirSnapshot(t, filepath.Join(root, "buckets", "live"))
	rep, err := Drill(ctx, k, DrillOptions{
		Bucket:    "s3://test/drill",
		Baseline:  &base,
		NewZeroFS: fakeBuckets(filepath.Join(root, "buckets")),
		Logf:      t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("REPORT<<%s>>REPORT\n", rep)
	if !rep.OK() {
		t.Fatalf("drill failed:\n%s", rep)
	}
	if rep.Files != 4 || rep.Permanodes != 2 || rep.Blobs != len(base.Blobs) {
		t.Errorf("restored %d files %d permanodes %d blobs; baseline %d blobs", rep.Files, rep.Permanodes, rep.Blobs, len(base.Blobs))
	}
	if after := dirSnapshot(t, filepath.Join(root, "buckets", "live")); after != before {
		t.Error("the drill touched the live bucket")
	}
}

func phaseRecover(t *testing.T, root string) {
	ctx := context.Background()
	var c liveCase
	readJSON(t, filepath.Join(root, "case.json"), &c)
	k, err := Open(filepath.Join(root, FileName), c.Pass)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	// The live server is gone; recover from its bucket onto a fresh dir.
	dir := filepath.Join(root, "recovered")
	var gotCreds zerofs.Storage
	newZFS := func(cfg zerofs.Config) (api.ZeroFS, error) {
		gotCreds = cfg.Storage
		return fakeBuckets(filepath.Join(root, "buckets"))(cfg)
	}
	st, err := RecoverDir(ctx, k, dir, RecoverOptions{
		Credentials: Credentials{AccessKeyID: "rw-key", SecretAccessKey: "rw-secret"},
		NewZeroFS:   newZFS,
		Logf:        t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotCreds.AccessKeyID != "rw-key" || gotCreds.SecretAccessKey != "rw-secret" || gotCreds.URL != "s3://test/live" {
		t.Errorf("zerofs storage: %+v", gotCreds)
	}
	if st.Blobs == 0 {
		t.Error("nothing restored")
	}
	cfg, err := config.LoadServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.URL != "s3://test/live" || cfg.ZeroFS.Password != testPassword {
		t.Errorf("recovered config: %+v", cfg)
	}
	if _, err := tcnet.LoadKey(cfg.KeyPath()); err != nil {
		t.Error(err)
	}
	if _, err := os.Stat(cfg.RegistryPath()); err != nil {
		t.Error(err)
	}
	// Recovering again over it is refused.
	if _, err := RecoverDir(ctx, k, dir, RecoverOptions{NewZeroFS: newZFS}); err == nil {
		t.Error("recovered over an existing server")
	}
}

func phaseRun(t *testing.T, root string) {
	ctx := context.Background()
	var c liveCase
	readJSON(t, filepath.Join(root, "case.json"), &c)
	cfg, err := config.LoadServer(filepath.Join(root, "recovered"))
	if err != nil {
		t.Fatal(err)
	}
	zfs := &fake.ZeroFS{DirPath: filepath.Join(root, "buckets", "live")}
	zfs.Start(ctx)
	srv, err := pkserver.Start(ctx, pkserver.Config{Dir: cfg.PerkeepDir(), ZeroFS: zfs, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	got, err := srv.Paths(ctx, c.Permanode)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(c.Want) {
		t.Fatalf("recovered tree %v, want %v", got, c.Want)
	}
	for k, w := range c.Want {
		if g := got[k]; g.File != w.File || g.Claim != w.Claim || !g.ModTime.Equal(w.ModTime) {
			t.Errorf("%s: got %+v want %+v", k, g, w)
		}
	}
	// The recovered server signs as the old one: the newer-mtime rule
	// holds across the recovery.
	r, err := srv.ClaimPath(ctx, api.PathClaim{Permanode: c.Permanode, Path: "a.txt", File: c.Want["with space"].File, ModTime: c.Want["a.txt"].ModTime})
	if err != nil || r != api.ClaimRejectedOlder {
		t.Errorf("stale claim after recovery: %v %v", r, err)
	}
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// dirSnapshot is every file's path, size and mtime under dir.
func dirSnapshot(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err == nil {
			fmt.Fprintf(&b, "%s %d %d\n", p, fi.Size(), fi.ModTime().UnixNano())
		}
		return nil
	})
	return b.String()
}

func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
