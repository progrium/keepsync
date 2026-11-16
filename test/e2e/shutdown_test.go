package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"tailscale.com/tstest/integration"
)

// TestServerLeavesNoZeroFS runs `keepsync server run` as its own process
// against real ZeroFS and checks that no zerofs outlives it: after a
// SIGINT, SIGTERM or SIGHUP, a SIGINT during startup, two SIGINTs and a
// SIGKILL of keepsync, and that a zerofs left
// running on the server's config by an earlier run is stopped at startup.
func TestServerLeavesNoZeroFS(t *testing.T) {
	zfsBin := binary(t, "KEEPSYNC_ZEROFS_BIN", "zerofs")
	m := startMinIO(t)
	root, err := os.MkdirTemp("", "kssig")
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
	m.MakeBucket(t, "shut")
	cli := buildCLI(t, root)
	base := filepath.Join(root, "base")
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(cli, append([]string{"--dir", base}, args...)...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("keepsync %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	run("server", "init", "--bucket", "s3://shut/keepsync", "--endpoint", m.Endpoint, "--allow-http",
		"--region", s3Region, "--access-key", minioUser, "--secret-key", minioSecret,
		"--derp-map", dmPath, "--zerofs", zfsBin, "--no-download", "--kit", filepath.Join(root, "kit.age"))

	zerofsPids := func() []string {
		out, _ := exec.Command("pgrep", "-f", "run --config "+base+"/").Output()
		return strings.Fields(string(out))
	}
	t.Cleanup(func() {
		for _, p := range zerofsPids() {
			exec.Command("kill", "-9", p).Run()
		}
	})

	var zenv []string // zerofs's environment, read from the first run
	start := func() (*exec.Cmd, chan error) {
		t.Helper()
		cmd := exec.Command(cli, "--dir", base, "server", "run")
		cmd.Dir = root
		lf, stop := logf(t, "server: ")
		w := lineWriter{lf}
		cmd.Stdout, cmd.Stderr = w, w
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait(); stop() }()
		eventually(t, 2*time.Minute, "server up", func() error {
			select {
			case err := <-done:
				t.Fatalf("server exited: %v", err)
			default:
			}
			c := exec.Command(cli, "--dir", base, "server", "status")
			if out, err := c.CombinedOutput(); err != nil || !strings.Contains(string(out), "zerofs:") {
				return fmt.Errorf("%v: %s", err, out)
			}
			return nil
		})
		p := zerofsPids()
		if len(p) != 1 {
			t.Fatalf("zerofs pids %v, want one", p)
		}
		if zenv == nil {
			b, err := os.ReadFile("/proc/" + p[0] + "/environ")
			if err != nil {
				t.Fatal(err)
			}
			zenv = strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		}
		return cmd, done
	}
	noZeroFS := func(after string) {
		t.Helper()
		eventually(t, 30*time.Second, "no zerofs after "+after, func() error {
			if p := zerofsPids(); len(p) > 0 {
				return fmt.Errorf("zerofs still running: %v", p)
			}
			return nil
		})
	}

	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		cmd, done := start()
		cmd.Process.Signal(sig)
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server exit after %v: %v", sig, err)
			}
		case <-time.After(3 * time.Minute):
			t.Fatalf("server did not stop on %v", sig)
		}
		noZeroFS(sig.String())
	}

	// SIGINT while the server is still starting, as soon as zerofs runs.
	{
		cmd := exec.Command(cli, "--dir", base, "server", "run")
		cmd.Dir = root
		lf, stop := logf(t, "server: ")
		w := lineWriter{lf}
		cmd.Stdout, cmd.Stderr = w, w
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		eventually(t, time.Minute, "zerofs started", func() error {
			if len(zerofsPids()) == 0 {
				return fmt.Errorf("no zerofs yet")
			}
			return nil
		})
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
		stop()
		noZeroFS("SIGINT during startup")
	}

	// A second SIGINT quits at once, without the clean stop.
	{
		cmd, done := start()
		cmd.Process.Signal(syscall.SIGINT)
		time.Sleep(50 * time.Millisecond)
		cmd.Process.Signal(syscall.SIGINT)
		select {
		case <-done:
		case <-time.After(time.Minute):
			t.Fatal("server did not quit on a second SIGINT")
		}
		noZeroFS("two SIGINTs")
	}

	// keepsync killed outright: zerofs must not outlive it.
	cmd, done := start()
	cmd.Process.Kill()
	<-done
	noZeroFS("SIGKILL")

	// A zerofs from an earlier run, still on this config (as left by an
	// older keepsync): startup stops it rather than running a second one
	// against the same bucket.
	cfgPath := ""
	filepath.WalkDir(base, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.Name() == "zerofs.toml" {
			cfgPath = p
		}
		return nil
	})
	stale := exec.Command(zfsBin, "run", "--config", cfgPath)
	stale.Env = zenv
	stale.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := stale.Start(); err != nil {
		t.Fatal(err)
	}
	go stale.Wait()
	eventually(t, 30*time.Second, "stale zerofs up", func() error {
		if len(zerofsPids()) != 1 {
			return fmt.Errorf("pids %v", zerofsPids())
		}
		return nil
	})
	cmd, done = start() // fails unless exactly one zerofs remains
	if p := zerofsPids(); len(p) != 1 || p[0] == fmt.Sprint(stale.Process.Pid) {
		t.Fatalf("zerofs pids %v; the stale one was %d", p, stale.Process.Pid)
	}
	cmd.Process.Signal(syscall.SIGINT)
	<-done
	noZeroFS("the last SIGINT")
}

// lineWriter sends a process's output to a log function, line by line.
type lineWriter struct{ logf func(string, ...any) }

func (w lineWriter) Write(p []byte) (int, error) {
	for _, l := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		w.logf("%s", l)
	}
	return len(p), nil
}
