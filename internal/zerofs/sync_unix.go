//go:build unix && !linux

package zerofs

import "syscall"

// syncDir flushes dirty pages. Only Linux has syncfs, so sync everything.
func syncDir(string) error {
	syscall.Sync()
	return nil
}
