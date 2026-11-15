//go:build !unix

package server

import (
	"errors"
	"io/fs"
)

func devOf(string) (uint64, bool)    { return 0, false }
func diskBytes(fi fs.FileInfo) int64 { return fi.Size() }

func volumeSpace(string) (uint64, uint64, error) {
	return 0, 0, errors.New("volume space is not available on this platform")
}
