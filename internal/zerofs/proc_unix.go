//go:build unix && !linux

package zerofs

import (
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// setDeathSig does nothing here: only Linux can tie a child's life to its
// parent's. A zerofs left behind by a killed keepsync is stopped when the
// server next starts (see stopStale).
func setDeathSig(*syscall.SysProcAttr) {}

// cmdline returns pid's argv as ps prints it, split on spaces, or nil.
func cmdline(pid int) []string {
	out, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

// allPids returns nil: without /proc, stopStale relies on the pid file.
func allPids() []int { return nil }
