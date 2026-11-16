//go:build unix

package zerofs

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// staleZeroFS finds zerofs processes running on configPath that this
// supervisor did not start: left by a keepsync that was killed (on systems
// without a parent-death signal, or by a keepsync older than it). It looks
// at the pid file and, where /proc exists, at every process.
func staleZeroFS(configPath, pidFile string) []int {
	var cands []int
	if b, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && pid > 0 {
			cands = append(cands, pid)
		}
	}
	cands = append(cands, allPids()...)
	var found []int
	self := os.Getpid()
	for _, pid := range cands {
		if pid == self || slices.Contains(found, pid) || !runsConfig(cmdline(pid), configPath) {
			continue
		}
		found = append(found, pid)
	}
	return found
}

// runsConfig reports whether argv is `zerofs run --config configPath`.
func runsConfig(argv []string, configPath string) bool {
	i := slices.Index(argv, "--config")
	return i >= 1 && i+1 < len(argv) && argv[i+1] == configPath && slices.Contains(argv[1:i], "run")
}

// stopStale stops zerofs processes left running on this config: two on
// one bucket would each write it as its own. Each gets SIGTERM, which
// makes zerofs flush and exit, then SIGKILL if it is still there after
// wait.
func (s *Supervisor) stopStale(wait time.Duration) error {
	pidFile := s.pidFile()
	pids := staleZeroFS(s.configPath, pidFile)
	if len(pids) == 0 {
		os.Remove(pidFile)
		return nil
	}
	for _, pid := range pids {
		s.log.Warn("stopping a zerofs left running on this server's config by an earlier run", "pid", pid)
		_ = signalPid(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(wait)
	for _, pid := range pids {
		for alive(pid) && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		if alive(pid) {
			s.log.Warn("stale zerofs did not exit on SIGTERM; killing it", "pid", pid)
			_ = signalPid(pid, syscall.SIGKILL)
			for i := 0; i < 50 && alive(pid); i++ {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if alive(pid) && runsConfig(cmdline(pid), s.configPath) {
			return fmt.Errorf("zerofs: an earlier zerofs (pid %d) is still running on %s and could not be stopped", pid, s.configPath)
		}
	}
	if err := os.Remove(pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
