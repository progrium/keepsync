package main

import (
	"bytes"
	"flag"
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

func TestPeerState(t *testing.T) {
	for _, tc := range []struct {
		p    node.PeerInfo
		want string
	}{
		{node.PeerInfo{}, "disconnected"},
		{node.PeerInfo{Connected: true}, "connected"},
		{node.PeerInfo{Connected: true, Path: &api.Path{Direct: true, Endpoint: "203.0.113.5:41641"}}, "connected (direct 203.0.113.5:41641)"},
		{node.PeerInfo{Connected: true, Path: &api.Path{Relay: "nyc"}}, "connected (relay nyc)"},
	} {
		if got := peerState(tc.p); got != tc.want {
			t.Errorf("peerState(%+v) = %q, want %q", tc.p, got, tc.want)
		}
	}
}

func TestSyncSummary(t *testing.T) {
	done := api.SyncCompletion{Pct: 100, GlobalBytes: 10, GlobalItems: 2}
	behind := api.SyncCompletion{Pct: 63.7, GlobalBytes: 3e9, NeedBytes: 1.2e9, NeedItems: 340}
	for _, tc := range []struct {
		fs   *api.FolderSync
		want string
	}{
		{nil, "-"},
		{&api.FolderSync{State: "idle", Local: done}, "up to date"},
		{&api.FolderSync{State: "scanning", Local: done}, "scanning"},
		{&api.FolderSync{State: "syncing", Local: behind}, "syncing 63% (1.2 GB, 340 files left)"},
		{&api.FolderSync{State: "idle", Local: api.SyncCompletion{Pct: 99.9, NeedItems: 1, NeedDeletes: 2}}, "behind 99% (1 file, 2 deletes left)"},
		{&api.FolderSync{State: "error", Error: "folder path missing"}, "error: folder path missing"},
		{&api.FolderSync{State: "idle", Local: done, Peers: []api.PeerSync{
			{Name: "laptop", Connected: true, Known: true, SyncCompletion: api.SyncCompletion{Pct: 80, NeedItems: 3}},
			{Name: "nas", Connected: false, Known: true, SyncCompletion: api.SyncCompletion{Pct: 10, NeedItems: 9}},
			{Name: "phone", Connected: true, Known: true, SyncCompletion: done},
		}}, "up to date; laptop at 80%"},
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
