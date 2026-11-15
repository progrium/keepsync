//go:build unix

package zerofs

import (
	"os"
	"os/exec"
	"syscall"
)

// setProcAttr puts zerofs in its own process group, so a Ctrl+C at the
// terminal reaches keepsync only and keepsync controls the shutdown order
// (perkeepd drains, then flush, then unmount, then zerofs).
func setProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func deviceOf(fi os.FileInfo) (uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Dev), true //nolint:unconvert // Dev's type varies by OS
}
