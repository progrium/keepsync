package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/progrium/keepsync/internal/config"
)

func TestResolveServerAddr(t *testing.T) {
	a, err := ResolveServer(context.Background(), " tcpGFwWCDJ ")
	if err != nil || a != "tcpGFwWCDJ" {
		t.Fatalf("%q %v", a, err)
	}
	if _, err := ResolveServer(context.Background(), ""); err == nil {
		t.Fatal("want error")
	}
}

func TestInitOffline(t *testing.T) {
	dir := t.TempDir()
	res, err := Init(context.Background(), InitOptions{Dir: dir, Name: "laptop", Server: "tcXYZ"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.Name != "laptop" || res.Device == "" || len(res.Key) < 10 {
		t.Fatalf("%+v", res)
	}
	cfg, err := config.LoadNode(dir)
	if err != nil || cfg.Server != "tcXYZ" {
		t.Fatalf("%+v %v", cfg, err)
	}
	if _, err := Init(context.Background(), InitOptions{Dir: dir, Server: "tcXYZ"}); err == nil {
		t.Fatal("second init should fail")
	}
}

// TestInitRefusesLongDir: a dir too long for the control socket fails init
// before anything is created (issue #5).
func TestInitRefusesLongDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("d", config.MaxSocketPath))
	if _, err := Init(context.Background(), InitOptions{Dir: dir, Server: "tcXYZ"}); err == nil || !strings.Contains(err.Error(), "socket path") {
		t.Fatalf("got %v, want a socket path error", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("init created %s: %v", dir, err)
	}
}
