package hook

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func script(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hook.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) logf(f string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, strings.TrimSpace(strings.ReplaceAll(sprintf(f, a...), "\n", " ")))
}

func job(t *testing.T, s string) (Job, *logs) {
	dir := t.TempDir()
	orig := filepath.Join(dir, "a.txt")
	conf := filepath.Join(dir, "a.sync-conflict-20260101-120000-ABCDEFG.txt")
	base := filepath.Join(t.TempDir(), "base")
	os.WriteFile(orig, []byte("mine\n"), 0o644)
	os.WriteFile(conf, []byte("theirs\n"), 0o644)
	os.WriteFile(base, []byte("base\n"), 0o644)
	l := &logs{}
	return Job{Script: s, Folder: "docs", Dir: dir, Original: orig, Conflict: conf, ConflictNode: "laptop", Base: base, Logf: l.logf}, l
}

func TestEnvAndResolve(t *testing.T) {
	s := script(t, `echo "folder=$KEEPSYNC_FOLDER node=$KEEPSYNC_CONFLICT_NODE cwd=$(pwd)"
cat "$KEEPSYNC_BASE" "$KEEPSYNC_CONFLICT" > "$KEEPSYNC_ORIGINAL"
rm "$KEEPSYNC_CONFLICT"
`)
	j, l := job(t, s)
	res := Run(context.Background(), j)
	if !res.Resolved || res.ExitCode != 0 {
		t.Fatalf("%+v", res)
	}
	b, _ := os.ReadFile(j.Original)
	if string(b) != "base\ntheirs\n" {
		t.Fatalf("original = %q", b)
	}
	wantCwd, _ := filepath.EvalSymlinks(j.Dir)
	got := strings.Join(l.lines, "|")
	if !strings.Contains(got, "folder=docs node=laptop") || !strings.Contains(got, "cwd="+wantCwd) {
		t.Fatalf("output %q", got)
	}
}

func TestLeftForHuman(t *testing.T) {
	for name, body := range map[string]string{
		"nonzero":   "rm \"$KEEPSYNC_CONFLICT\"; exit 3\n",
		"copy kept": "exit 0\n",
	} {
		t.Run(name, func(t *testing.T) {
			j, _ := job(t, script(t, body))
			if res := Run(context.Background(), j); res.Resolved || res.Reason == "" {
				t.Fatalf("%+v", res)
			}
		})
	}
}

func TestTimeoutKillsGroup(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "child-alive")
	j, _ := job(t, script(t, "(sleep 2; touch "+marker+") &\nsleep 30\n"))
	j.Timeout = 200 * time.Millisecond
	start := time.Now()
	res := Run(context.Background(), j)
	if res.Resolved || !strings.Contains(res.Reason, "timed out") || time.Since(start) > 5*time.Second {
		t.Fatalf("%+v after %v", res, time.Since(start))
	}
	time.Sleep(2500 * time.Millisecond)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the hook's child outlived the timeout")
	}
}

func sprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }

func TestBaseExact(t *testing.T) {
	s := script(t, `echo "exact=$KEEPSYNC_BASE_EXACT"; exit 1`)
	for _, tc := range []struct {
		exact, noBase bool
		want          string
	}{
		{true, false, "exact=1"},
		{false, false, "exact=0"},
		{true, true, "exact=0"}, // no base is never exact
	} {
		j, l := job(t, s)
		j.BaseExact = tc.exact
		if tc.noBase {
			j.Base = ""
		}
		Run(context.Background(), j)
		if got := strings.Join(l.lines, "|"); !strings.Contains(got, tc.want) {
			t.Fatalf("%+v: output %q", tc, got)
		}
	}
}
