//go:build !unix

package zerofs

import (
	"os"
	"os/exec"
)

// ZeroFS ships no build for these platforms; the package still compiles so
// the keepsync binary does, but a server cannot run here.

func setProcAttr(*exec.Cmd) {}

func deviceOf(os.FileInfo) (uint64, bool) { return 0, false }

func syncDir(string) error { return nil }
