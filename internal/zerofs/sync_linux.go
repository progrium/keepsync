package zerofs

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDir flushes every dirty page of the filesystem holding dir.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}
