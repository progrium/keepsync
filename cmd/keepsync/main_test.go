package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/node"
)

func TestParseInterspersed(t *testing.T) {
	fs := flag.NewFlagSet("keep", flag.ContinueOnError)
	var g time.Duration
	fs.Var(durationFlag{&g}, "grace", "")
	pos, err := parse(fs, []string{"docs", "--grace", "2d"}, 1, 1, "")
	if err != nil || len(pos) != 1 || pos[0] != "docs" || g != 48*time.Hour {
		t.Fatalf("%v %v %v", pos, g, err)
	}
	if _, err := parse(flag.NewFlagSet("x", flag.ContinueOnError), []string{"a", "b"}, 1, 1, ""); err == nil {
		t.Fatal("want arity error")
	}
}

func TestFmtDur(t *testing.T) {
	for d, want := range map[time.Duration]string{48 * time.Hour: "2d", 36 * time.Hour: "36h", 90 * time.Minute: "1h30m", 0: "0"} {
		if got := fmtDur(d); got != want {
			t.Errorf("%v: %q, want %q", d, got, want)
		}
	}
}

func TestNotRunning(t *testing.T) {
	var out, errb bytes.Buffer
	e := &env{stdout: &out, stderr: &errb}
	err := run(e, []string{"--dir", t.TempDir(), "status"})
	if err == nil || !strings.Contains(err.Error(), "node run") {
		t.Fatalf("got %v", err)
	}
	err = run(e, []string{"--dir", t.TempDir(), "server", "nodes"})
	if err == nil || !strings.Contains(err.Error(), "server run") {
		t.Fatalf("got %v", err)
	}
}

func TestParseAt(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	if got, err := parseAt("3d", now); err != nil || !got.Equal(now.Add(-72*time.Hour)) {
		t.Fatalf("3d: %v %v", got, err)
	}
	if got, err := parseAt("2026-11-01T10:00:00Z", now); err != nil || !got.Equal(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("rfc3339: %v %v", got, err)
	}
	if got, err := parseAt("2026-11-01", now); err != nil || got.Day() != 1 || got.Location() != time.Local {
		t.Fatalf("date: %v %v", got, err)
	}
	if _, err := parseAt("yesterday", now); err == nil {
		t.Fatal("want error")
	}
}

func TestPeerPath(t *testing.T) {
	two := node.PeerInfo{Connected: true, Tunnels: []api.Tunnel{
		{Path: &api.Path{Direct: true, Endpoint: "71.116.60.17:54425"}, Traffic: api.Traffic{InRate: 20e6, OutRate: 1e3}},
		{Out: true, Path: &api.Path{Relay: "nyc"}, Idle: true},
	}}
	for _, tc := range []struct {
		p    node.PeerInfo
		long bool
		want string
	}{
		{node.PeerInfo{}, false, "offline"},
		{node.PeerInfo{Connected: true}, false, "connected"},
		{node.PeerInfo{Connected: true, Path: &api.Path{Direct: true, Endpoint: "203.0.113.5:41641"}}, false, "direct 203.0.113.5"},
		{node.PeerInfo{Connected: true, Path: &api.Path{Direct: true, Endpoint: "203.0.113.5:41641"}}, true, "direct 203.0.113.5:41641"},
		{node.PeerInfo{Connected: true, Path: &api.Path{Relay: "nyc"}}, false, "relay nyc"},
		{node.PeerInfo{Connected: true, Tunnels: []api.Tunnel{{Idle: true}}}, false, "idle"},
		{node.PeerInfo{Connected: true, Tunnels: []api.Tunnel{{Out: true, Path: &api.Path{Relay: "nyc"}}}}, false, "relay nyc"},
		{two, false, "in direct 71.116.60.17, out relay nyc (idle)"},
		{two, true, "in direct 71.116.60.17:54425 ↓ 20.0 MB/s  ↑ 1.0 kB/s, out relay nyc (idle)"},
	} {
		if got := peerPath(tc.p, tc.long); got != tc.want {
			t.Errorf("peerPath(%+v, %v) = %q, want %q", tc.p, tc.long, got, tc.want)
		}
	}
}

func TestSyncShort(t *testing.T) {
	for _, tc := range []struct {
		fs   *api.FolderSync
		want string
	}{
		{nil, "-"},
		{&api.FolderSync{State: "idle"}, "up to date"},
		{&api.FolderSync{State: "syncing", Local: api.SyncCompletion{Pct: 63.7, NeedBytes: 1.2e9, NeedItems: 340}}, "syncing 63%, 1.2 GB left"},
		{&api.FolderSync{State: "idle", Local: api.SyncCompletion{Pct: 99, NeedDeletes: 2}}, "behind 99%, 2 items left"},
		{&api.FolderSync{State: "error", Error: "folder marker missing (this indicates potential data loss)"}, "error (see folder status)"},
		{&api.FolderSync{State: "idle", Peers: []api.PeerSync{{Connected: true, Known: true, SyncCompletion: api.SyncCompletion{NeedItems: 1}}}}, "up to date, peer behind"},
	} {
		if got := syncShort(tc.fs); got != tc.want {
			t.Errorf("syncShort(%+v) = %q, want %q", tc.fs, got, tc.want)
		}
	}
}

func TestSyncSummary(t *testing.T) {
	done := api.SyncCompletion{Pct: 100, GlobalBytes: 4.2e9, GlobalItems: 1300, GlobalFiles: 1234}
	behind := api.SyncCompletion{Pct: 63.7, GlobalBytes: 3e9, NeedBytes: 1.2e9, NeedItems: 340}
	for _, tc := range []struct {
		fs   *api.FolderSync
		want string
	}{
		{nil, "-"},
		{&api.FolderSync{State: "idle", Local: done}, "up to date (4.2 GB, 1,234 files)"},
		{&api.FolderSync{State: "scanning", Local: done}, "scanning"},
		{&api.FolderSync{State: "syncing", Local: behind}, "syncing 63% (1.2 GB, 340 files left)"},
		{&api.FolderSync{State: "idle", Local: api.SyncCompletion{Pct: 99.9, NeedItems: 1, NeedDeletes: 2}}, "behind 99% (1 file, 2 deletes left)"},
		{&api.FolderSync{State: "error", Error: "folder path missing"}, "error: folder path missing"},
		{&api.FolderSync{State: "idle", Local: done, Peers: []api.PeerSync{
			{Name: "laptop", Connected: true, Known: true, SyncCompletion: api.SyncCompletion{Pct: 80, NeedItems: 3}},
			{Name: "nas", Connected: false, Known: true, SyncCompletion: api.SyncCompletion{Pct: 10, NeedItems: 9}},
			{Name: "phone", Connected: true, Known: true, SyncCompletion: done},
		}}, "up to date (4.2 GB, 1,234 files); laptop at 80%"},
	} {
		if got := syncSummary(tc.fs); got != tc.want {
			t.Errorf("syncSummary(%+v) = %q, want %q", tc.fs, got, tc.want)
		}
	}
	if got := traffic(&api.Traffic{InRate: 3.4e6, OutRate: 120e3}); got != "↓ 3.4 MB/s  ↑ 120.0 kB/s" {
		t.Errorf("traffic = %q", got)
	}
	if got := traffic(nil); got != "-" {
		t.Errorf("traffic(nil) = %q", got)
	}
}

func TestCLIPath(t *testing.T) {
	wd, _ := os.Getwd()
	home, _ := os.UserHomeDir()
	for in, want := range map[string]string{
		"./Demo":    filepath.Join(wd, "Demo"),
		"Demo":      filepath.Join(wd, "Demo"),
		"/abs/x":    "/abs/x",
		"~/Sync/x":  filepath.Join(home, "Sync/x"),
		"~":         home,
		"../a/../b": filepath.Join(filepath.Dir(wd), "b"),
	} {
		if got, err := cliPath(in); err != nil || got != want {
			t.Errorf("cliPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	long := &api.FolderSync{State: "error", Error: "folder marker missing (this indicates potential data loss, search docs/forum to get information about how to proceed)"}
	if got := syncSummary(long); got != "error: folder marker missing (this indicates… (folder status for more)" {
		t.Errorf("syncSummary(long error) = %q", got)
	}
	if got := syncText(long, true); got != "error: "+long.Error {
		t.Errorf("syncText(full) = %q", got)
	}
}

func TestThousands(t *testing.T) {
	for n, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 1234567: "1,234,567", -4500: "-4,500"} {
		if got := thousands(n); got != want {
			t.Errorf("thousands(%d) = %q, want %q", n, got, want)
		}
	}
}
