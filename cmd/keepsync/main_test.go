package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
	"time"
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
