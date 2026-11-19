package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/progrium/keepsync/internal/config"
)

// TestInitRefusesLongDir: a dir too long for zerofs's sockets fails init
// before the dir, kit, or passphrase exist, not on the first run (issue #5).
func TestInitRefusesLongDir(t *testing.T) {
	base := t.TempDir()
	// Long enough for zerofs/run/rpc.sock to overflow, short enough for
	// ctl.sock to fit, so the zerofs check is the one that fires.
	n := config.MaxSocketPath - len(base) - len("/zerofs/run/rpc.sock") + 1
	dir := filepath.Join(base, strings.Repeat("d", n-1))
	if len(filepath.Join(dir, "ctl.sock")) > config.MaxSocketPath {
		t.Fatalf("test dir too long: %d", len(dir))
	}
	_, err := Init(context.Background(), InitOptions{Dir: dir, Storage: config.Storage{URL: "file://" + base}})
	if err == nil || !strings.Contains(err.Error(), "rpc.sock") {
		t.Fatalf("got %v, want an rpc.sock path error", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("init created %s: %v", dir, err)
	}
}
