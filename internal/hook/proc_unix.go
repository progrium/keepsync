//go:build unix

package hook

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the hook in its own process group, so a timeout
// kills everything it started.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
