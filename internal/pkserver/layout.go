package pkserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"go4.org/jsonconfig"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/files"
)

// The blob layout on ZeroFS. It is a property of the bucket, found by
// looking at what is there, so a server or a recovery reading an existing
// bucket uses whatever layout wrote it.
const (
	// LayoutDiskpacked is Perkeep's diskpacked store at the root of the
	// filesystem: append-only pack files plus an index. Its files must be
	// reached through a mount.
	LayoutDiskpacked = "diskpacked"
	// LayoutFiles is one file per blob under blobs/ (Perkeep's files
	// storage, as localdisk lays it out). It works through a mount or over
	// 9P with no mount.
	LayoutFiles = "files"
)

const (
	// diskpackedMarker is the first pack file, which diskpacked makes
	// when it first opens its directory.
	diskpackedMarker = "pack-00000.blobs"
	// filesRoot holds the files layout.
	filesRoot = "blobs"
)

// zeroFSVFS is a ZeroFS that is reached over 9P rather than a mount (see
// zerofs.Supervisor.VFS). VFS returns nil while there is no connection or
// when the ZeroFS is mounted.
type zeroFSVFS interface {
	VFS() files.VFS
}

// zeroFSAccess is how the server reaches ZeroFS: through a mount at dir, or
// over vfs.
type zeroFSAccess struct {
	dir string
	vfs files.VFS
}

func accessOf(z any, dir string) zeroFSAccess {
	if v, ok := z.(zeroFSVFS); ok {
		if fs := v.VFS(); fs != nil {
			return zeroFSAccess{vfs: fs}
		}
	}
	return zeroFSAccess{dir: dir}
}

func (a zeroFSAccess) exists(name string) (bool, error) {
	var err error
	if a.vfs != nil {
		_, err = a.vfs.Stat(name)
	} else {
		_, err = os.Stat(filepath.Join(a.dir, name))
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// detectLayout returns the layout already on ZeroFS. An empty filesystem
// gets LayoutFiles over 9P and LayoutDiskpacked through a mount, which is
// what servers wrote before 9P access.
func detectLayout(a zeroFSAccess) (string, error) {
	packs, err := a.exists(diskpackedMarker)
	if err != nil {
		return "", fmt.Errorf("pkserver: looking for the blob layout on zerofs: %w", err)
	}
	blobs, err := a.exists(filesRoot)
	if err != nil {
		return "", fmt.Errorf("pkserver: looking for the blob layout on zerofs: %w", err)
	}
	switch {
	case packs && blobs:
		return "", fmt.Errorf("pkserver: zerofs holds both a diskpacked store (%s) and a files store (%s/)", diskpackedMarker, filesRoot)
	case packs:
		if a.vfs != nil {
			return "", errors.New("pkserver: this bucket holds a diskpacked blob store, which needs zerofs mounted; set zerofs.mount to fuse, v9fs or external")
		}
		return LayoutDiskpacked, nil
	case blobs:
		return LayoutFiles, nil
	case a.vfs != nil:
		return LayoutFiles, nil
	}
	return LayoutDiskpacked, nil
}

// ninePType is the perkeepd storage type of a files store over a
// registered VFS. Its config is {"vfs": key, "root": dir}.
const ninePType = "keepsync-9p"

var vfsRegistry sync.Map // key string -> files.VFS

func registerVFS(fs files.VFS) (key string, unregister func()) {
	var b [8]byte
	rand.Read(b[:])
	key = hex.EncodeToString(b[:])
	vfsRegistry.Store(key, fs)
	return key, func() { vfsRegistry.Delete(key) }
}

func init() {
	blobserver.RegisterStorageConstructor(ninePType, func(_ blobserver.Loader, conf jsonconfig.Obj) (blobserver.Storage, error) {
		key := conf.RequiredString("vfs")
		root := conf.RequiredString("root")
		if err := conf.Validate(); err != nil {
			return nil, err
		}
		v, ok := vfsRegistry.Load(key)
		if !ok {
			return nil, fmt.Errorf("%s: no VFS %q", ninePType, key)
		}
		fs := v.(files.VFS)
		if err := fs.MkdirAll(root, 0o700); err != nil {
			return nil, err
		}
		return vfsStorage{files.NewStorage(fs, root)}, nil
	})
}

// vfsStorage is a files.Storage that turns a panic in a fetch into an
// error. files.Storage dereferences the FileInfo of a failed Stat unless
// the error is not-exist, and over 9P a Stat can fail otherwise (a broken
// connection, or one closed while perkeepd's sync goroutines still run).
type vfsStorage struct{ *files.Storage }

func (s vfsStorage) Fetch(ctx context.Context, br blob.Ref) (rc io.ReadCloser, size uint32, err error) {
	defer recoverFetch(br, &err)
	return s.Storage.Fetch(ctx, br)
}

func (s vfsStorage) SubFetch(ctx context.Context, br blob.Ref, offset, length int64) (rc io.ReadCloser, err error) {
	defer recoverFetch(br, &err)
	return s.Storage.SubFetch(ctx, br, offset, length)
}

func recoverFetch(br blob.Ref, err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("%s: fetching %s: %v", ninePType, br, r)
	}
}

// replicaHandler returns the perkeepd low-level handler for the blob store
// on ZeroFS: the prefix's "handler" and "handlerArgs". vfsKey is the
// registered VFS when a reaches ZeroFS over 9P.
func replicaHandler(a zeroFSAccess, layout, vfsKey string) (string, map[string]any) {
	switch {
	case layout == LayoutDiskpacked:
		return "storage-diskpacked", map[string]any{"path": a.dir}
	case a.vfs != nil:
		return "storage-" + ninePType, map[string]any{"vfs": vfsKey, "root": "/" + filesRoot}
	}
	return "storage-filesystem", map[string]any{"path": filepath.Join(a.dir, filesRoot)}
}
