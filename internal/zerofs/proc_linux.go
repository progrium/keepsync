package zerofs

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

// setDeathSig makes the kernel send zerofs SIGTERM when keepsync dies,
// however it dies (SIGKILL, a crash, the OOM killer), so zerofs never
// outlives it. SIGTERM makes zerofs flush and exit on its own.
//
// The signal follows the OS thread that started zerofs, not the process:
// if that thread exits, zerofs gets it early. Go's runtime keeps its
// threads, except one whose goroutine locked it and exited without
// unlocking, which keepsync never does on these paths; and an early
// SIGTERM would only look like a zerofs exit, which the supervisor
// restarts.
func setDeathSig(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGTERM
}

// cmdline returns pid's argv, or nil if it is gone or unreadable.
func cmdline(pid int) []string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil || len(b) == 0 {
		return nil
	}
	var args []string
	for _, a := range bytes.Split(bytes.TrimRight(b, "\x00"), []byte{0}) {
		args = append(args, string(a))
	}
	return args
}

// allPids lists every process this user can see.
func allPids() []int {
	ents, _ := filepath.Glob("/proc/[0-9]*")
	var pids []int
	for _, e := range ents {
		if pid, err := strconv.Atoi(filepath.Base(e)); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}
