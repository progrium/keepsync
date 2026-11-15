//go:build unix

package server

import (
	"io/fs"
	"os"
	"syscall"
)

func devOf(p string) (uint64, bool) {
	fi, err := os.Lstat(p)
	if err != nil {
		return 0, false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Dev), true //nolint:unconvert // Dev's type varies by OS
}

// diskBytes is the space a file takes on disk, so sparse and preallocated
// files count as what they use.
func diskBytes(fi fs.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512 //nolint:unconvert
	}
	return fi.Size()
}

func volumeSpace(p string) (free, size uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(p, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(st.Bsize)                                     //nolint:unconvert
	return uint64(st.Bavail) * bs, uint64(st.Blocks) * bs, nil //nolint:unconvert
}
