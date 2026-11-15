package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := &Server{Dir: dir, Storage: Storage{URL: "s3://b/p", AccessKeyID: "k"}, ZeroFS: ZeroFS{Password: "pw"}, FlushInterval: Duration(30 * time.Second)}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, ServerFile))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	got, err := LoadServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Storage != s.Storage || got.ZeroFS != s.ZeroFS || got.Flush() != 30*time.Second || got.Dir != dir {
		t.Fatalf("got %+v", got)
	}

	n := &Node{Dir: dir, Name: "a", Server: "tcX"}
	n.SetFolder(Folder{Name: "docs", Path: "/d"})
	n.SetFolder(Folder{Name: "docs", Path: "/d2", ID: "x"})
	n.SetFolder(Folder{Name: "pics", Path: "/p"})
	if err := n.Save(); err != nil {
		t.Fatal(err)
	}
	gn, err := LoadNode(dir)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := gn.Folder("docs"); !ok || f.Path != "/d2" || len(gn.Folders) != 2 {
		t.Fatalf("got %+v", gn)
	}
	if !gn.RemoveFolder("docs") || gn.RemoveFolder("docs") || len(gn.Folders) != 1 {
		t.Fatal("remove")
	}
}

func TestNotInitialized(t *testing.T) {
	if _, err := LoadNode(t.TempDir()); err == nil {
		t.Fatal("want error")
	}
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{"7d": 7 * 24 * time.Hour, "48h": 48 * time.Hour, "1.5d": 36 * time.Hour, "0": 0} {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("%s: %v %v", in, got, err)
		}
	}
	if _, err := ParseDuration("xd"); err == nil {
		t.Error("want error")
	}
}

func TestGatewayListen(t *testing.T) {
	dir := t.TempDir()
	n := &Node{Dir: dir}
	if n.GatewayListen() != DefaultGatewayListen {
		t.Fatal(n.GatewayListen())
	}
	off := ""
	n.Gateway.Listen = &off
	if err := n.Save(); err != nil {
		t.Fatal(err)
	}
	got, err := LoadNode(dir)
	if err != nil || got.GatewayListen() != "" {
		t.Fatalf("disabled gateway did not round-trip: %v %v", got.Gateway.Listen, err)
	}
}

func TestServerModeAndCache(t *testing.T) {
	c := &Server{}
	if c.StorageMode() != ModeStore || c.CacheSizeGB() != StoreCacheSizeGB || c.Validate() != nil {
		t.Fatalf("store defaults: %s %g %v", c.StorageMode(), c.CacheSizeGB(), c.Validate())
	}
	c.Mode = ModeCache
	if c.CacheSizeGB() != DefaultCacheModeGB {
		t.Fatalf("cache default %g", c.CacheSizeGB())
	}
	c.ZeroFS.CacheSizeGB = 50
	if c.CacheSizeGB() != 50 || c.Validate() != nil {
		t.Fatal("explicit cache size")
	}
	c.ZeroFS.CacheSizeGB = 1
	if c.Validate() == nil {
		t.Fatal("cache below minimum accepted")
	}
	c.ZeroFS.CacheSizeGB = 0
	c.Mode = "mirror"
	if c.Validate() == nil {
		t.Fatal("unknown mode accepted")
	}
}

func TestModeFile(t *testing.T) {
	cfg := &Server{Dir: t.TempDir(), Mode: ModeCache}
	if err := cfg.WriteModeFile(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.CheckModeFile(); err != nil {
		t.Fatal(err)
	}
	cfg.Mode = ""
	if err := cfg.CheckModeFile(); err == nil {
		t.Fatal("switching cache to store mode was allowed")
	}
	// A server from before modes is store mode.
	if err := (&Server{Dir: t.TempDir()}).CheckModeFile(); err != nil {
		t.Fatal(err)
	}
	if err := (&Server{Dir: t.TempDir(), Mode: ModeCache}).CheckModeFile(); err == nil {
		t.Fatal("pre-mode server switched to cache mode")
	}
}
