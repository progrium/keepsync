package zerofs

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"perkeep.org/pkg/blobserver/files"
)

// NineP is a filesystem client for ZeroFS's 9P server, used in MountNineP
// mode in place of a mount. It implements Perkeep's files.VFS, so a
// files.Storage on it is a blob store on ZeroFS with no kernel or FUSE
// mount in between. Paths are slash-separated and relative to the root of
// the ZeroFS filesystem; a leading slash is ignored. It is safe for
// concurrent use.
//
// Sync on a written file, and SyncDir, do nothing: a 9P write returns once
// zerofs has the data, and Supervisor.Flush (a zerofs flush) is what makes
// it durable in the bucket. A real 9P fsync costs a full zerofs flush per
// file (tens of milliseconds), which per blob would be the bulk of the
// write cost.
type NineP struct {
	conn io.Closer
	c    *p9.Client
	root p9.File

	broken atomic.Pointer[error] // first connection error
}

var (
	_ files.VFS       = (*NineP)(nil)
	_ files.DirSyncer = (*NineP)(nil)
)

// ninePMsize is the 9P message size asked for; zerofs v2.3.7 offers 1 MiB.
const ninePMsize = 1 << 20

// DialNineP connects to the 9P server on the unix socket at socket.
func DialNineP(ctx context.Context, socket string) (*NineP, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	n, err := newNineP(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return n, nil
}

func newNineP(conn io.ReadWriteCloser) (*NineP, error) {
	c, err := p9.NewClient(&ninePConn{rwc: conn}, p9.WithMessageSize(ninePMsize))
	if err != nil {
		return nil, fmt.Errorf("9p: version: %w", err)
	}
	root, err := c.Attach("")
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("9p: attach: %w", err)
	}
	return &NineP{conn: conn, c: c, root: root}, nil
}

// Close closes the connection.
func (n *NineP) Close() error {
	n.root.Close()
	return n.c.Close()
}

// Err reports the error that broke the connection, if one has.
func (n *NineP) Err() error {
	if p := n.broken.Load(); p != nil {
		return *p
	}
	return nil
}

// pathErr wraps a 9P error for op on name. ENOENT and EEXIST become
// fs.ErrNotExist and fs.ErrExist, so os.IsNotExist and errors.Is work.
func (n *NineP) pathErr(op, name string, err error) error {
	if err == nil {
		return nil
	}
	var ce p9.ConnError
	if errors.As(err, &ce) {
		e := fmt.Errorf("9p connection: %w", err)
		n.broken.CompareAndSwap(nil, &e)
	}
	var errno linux.Errno
	if errors.As(err, &errno) {
		switch errno {
		case linux.ENOENT:
			err = fs.ErrNotExist
		case linux.EEXIST:
			err = fs.ErrExist
		}
	}
	return &fs.PathError{Op: op, Path: name, Err: err}
}

// split returns the path elements of name.
func split(name string) []string {
	name = strings.Trim(path.Clean("/"+name), "/")
	if name == "" {
		return nil
	}
	return strings.Split(name, "/")
}

// maxWalk is the most names one Twalk may carry (MAXWELEM).
const maxWalk = 16

// walk returns a new fid for the file at the path elements names.
func (n *NineP) walk(names []string) (p9.File, error) {
	f, err := walkOne(n.root, names[:min(len(names), maxWalk)])
	for names = names[min(len(names), maxWalk):]; err == nil && len(names) > 0; names = names[min(len(names), maxWalk):] {
		var next p9.File
		next, err = walkOne(f, names[:min(len(names), maxWalk)])
		f.Close()
		f = next
	}
	return f, err
}

// walkOne walks from f to names (at most maxWalk of them). In 9P a walk
// whose first name exists but a later one does not succeeds with fewer
// QIDs than names and no new fid; the p9 client returns a File for it
// anyway, so that case is turned into ENOENT here.
func walkOne(f p9.File, names []string) (p9.File, error) {
	qids, nf, err := f.Walk(names)
	if err != nil {
		return nil, err
	}
	if len(qids) < len(names) {
		// The server made no fid; the clunk fails, and the client drops
		// the fid number rather than reuse it.
		clunkAsync(nf)
		return nil, linux.ENOENT
	}
	return nf, nil
}

// walkParent returns a fid for name's parent directory, and name's base.
func (n *NineP) walkParent(op, name string) (p9.File, string, error) {
	el := split(name)
	if len(el) == 0 {
		return nil, "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	dir, err := n.walk(el[:len(el)-1])
	if err != nil {
		return nil, "", n.pathErr(op, name, err)
	}
	return dir, el[len(el)-1], nil
}

func (n *NineP) unlink(op, name string, flags uint32) error {
	dir, base, err := n.walkParent(op, name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return n.pathErr(op, name, dir.UnlinkAt(base, flags))
}

// atRemoveDir is Linux's AT_REMOVEDIR.
const atRemoveDir = 0x200

// Remove removes the file name.
func (n *NineP) Remove(name string) error { return n.unlink("remove", name, 0) }

// RemoveDir removes the empty directory name.
func (n *NineP) RemoveDir(name string) error { return n.unlink("rmdir", name, atRemoveDir) }

// Stat returns name's FileInfo. 9P walks do not follow symlinks, so Stat
// is Lstat; ZeroFS holds no symlinks that keepsync made.
func (n *NineP) Stat(name string) (fs.FileInfo, error) { return n.Lstat(name) }

// Lstat returns name's FileInfo.
func (n *NineP) Lstat(name string) (fs.FileInfo, error) {
	f, err := n.walk(split(name))
	if err != nil {
		return nil, n.pathErr("stat", name, err)
	}
	defer clunkAsync(f)
	return n.stat(f, name)
}

func (n *NineP) stat(f p9.File, name string) (fs.FileInfo, error) {
	_, _, attr, err := f.GetAttr(p9.AttrMask{Mode: true, Size: true, MTime: true})
	if err != nil {
		return nil, n.pathErr("stat", name, err)
	}
	return fileInfo{
		name:  path.Base("/" + name),
		size:  int64(attr.Size),
		mode:  attr.Mode.OSMode(),
		mtime: time.Unix(int64(attr.MTimeSeconds), int64(attr.MTimeNanoSeconds)),
	}, nil
}

type fileInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	mtime time.Time
}

func (fi fileInfo) Name() string       { return fi.name }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return fi.mtime }
func (fi fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi fileInfo) Sys() any           { return nil }

// Open opens name for reading.
func (n *NineP) Open(name string) (files.ReadableFile, error) {
	f, err := n.walk(split(name))
	if err != nil {
		return nil, n.pathErr("open", name, err)
	}
	fi, err := n.stat(f, name)
	if err == nil && fi.IsDir() {
		err = &fs.PathError{Op: "open", Path: name, Err: errors.New("is a directory")}
	}
	if err == nil {
		_, _, err = f.Open(p9.ReadOnly)
		err = n.pathErr("open", name, err)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return &readFile{n: n, f: f, name: name, size: fi.Size()}, nil
}

type readFile struct {
	n    *NineP
	f    p9.File
	name string
	size int64 // at open, for Seek from the end
	off  int64 // of the next byte Read returns

	// buf holds file bytes from bufOff, read ahead in large 9P reads:
	// callers such as io.ReadAll read in small pieces, and each 9P read
	// is a round trip.
	buf    []byte
	bufOff int64
}

// readAhead is the most a Read asks zerofs for at once.
const readAhead = ninePMsize - 4096

func (r *readFile) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off < r.bufOff || r.off >= r.bufOff+int64(len(r.buf)) {
		want := int64(readAhead)
		if rest := r.size - r.off; rest > 0 && rest < want {
			want = rest // files.Storage reads blobs whole: one round trip
		}
		if int64(len(p)) > want {
			want = int64(len(p))
		}
		if int64(cap(r.buf)) < want {
			r.buf = make([]byte, want)
		}
		m, err := r.f.ReadAt(r.buf[:want], r.off)
		r.buf, r.bufOff = r.buf[:m], r.off
		if m == 0 {
			if err == nil || err == io.EOF {
				return 0, io.EOF
			}
			return 0, r.n.pathErr("read", r.name, err)
		}
	}
	m := copy(p, r.buf[r.off-r.bufOff:])
	r.off += int64(m)
	return m, nil
}

func (r *readFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.size
	default:
		return r.off, errors.New("9p: bad whence")
	}
	if offset < 0 {
		return r.off, errors.New("9p: negative offset")
	}
	r.off = offset
	return offset, nil
}

// Close releases the fid without waiting: nothing was written through it,
// so the reply carries nothing a caller needs.
func (r *readFile) Close() error {
	clunkAsync(r.f)
	return nil
}

// clunkAsync releases a read-only fid in the background, taking its round
// trip off the caller's path. The fid returns to the pool only once zerofs
// has answered, so it is never reused early.
func clunkAsync(f p9.File) { go f.Close() }

// MkdirAll makes the directory name and any missing parents.
func (n *NineP) MkdirAll(name string, perm fs.FileMode) error {
	el := split(name)
	cur, err := n.walk(nil)
	if err != nil {
		return n.pathErr("mkdir", name, err)
	}
	defer func() { cur.Close() }()
	for i, e := range el {
		_, next, err := cur.Walk([]string{e})
		if errors.Is(n.pathErr("mkdir", name, err), fs.ErrNotExist) {
			if _, err = cur.Mkdir(e, p9.FileMode(perm.Perm()), p9.NoUID, p9.NoGID); err != nil && !errors.Is(n.pathErr("mkdir", name, err), fs.ErrExist) {
				return n.pathErr("mkdir", path.Join(el[:i+1]...), err)
			}
			_, next, err = cur.Walk([]string{e})
		}
		if err != nil {
			return n.pathErr("mkdir", path.Join(el[:i+1]...), err)
		}
		cur.Close()
		cur = next
	}
	fi, err := n.stat(cur, name)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return &fs.PathError{Op: "mkdir", Path: name, Err: errors.New("not a directory")}
	}
	return nil
}

// Rename renames oldname to newname, replacing newname if it exists.
func (n *NineP) Rename(oldname, newname string) error {
	od, ob, err := n.walkParent("rename", oldname)
	if err != nil {
		return err
	}
	defer od.Close()
	nd, nb, err := n.walkParent("rename", newname)
	if err != nil {
		return err
	}
	defer nd.Close()
	return n.pathErr("rename", oldname, od.RenameAt(ob, nd, nb))
}

// TempFile creates a new file in dir for writing, like os.CreateTemp.
func (n *NineP) TempFile(dir, pattern string) (files.WritableFile, error) {
	prefix, suffix := pattern, ""
	if i := strings.LastIndex(pattern, "*"); i >= 0 {
		prefix, suffix = pattern[:i], pattern[i+1:]
	}
	d, err := n.walk(split(dir))
	if err != nil {
		return nil, n.pathErr("createtemp", dir, err)
	}
	defer d.Close()
	for range 10 {
		var r [8]byte
		rand.Read(r[:])
		base := prefix + hex.EncodeToString(r[:]) + suffix
		name := path.Join(dir, base)
		// Create turns the fid it is called on into the new file's.
		_, f, err := d.Walk(nil)
		if err != nil {
			return nil, n.pathErr("createtemp", dir, err)
		}
		if _, _, _, err = f.Create(base, p9.WriteOnly, 0o600, p9.NoUID, p9.NoGID); err != nil {
			f.Close()
			if err = n.pathErr("createtemp", name, err); errors.Is(err, fs.ErrExist) {
				continue
			}
			return nil, err
		}
		return &writeFile{n: n, f: f, name: name}, nil
	}
	return nil, &fs.PathError{Op: "createtemp", Path: path.Join(dir, pattern), Err: fs.ErrExist}
}

type writeFile struct {
	n    *NineP
	f    p9.File
	name string
	off  int64
}

func (w *writeFile) Write(p []byte) (int, error) {
	m, err := w.f.WriteAt(p, w.off)
	w.off += int64(m)
	if err == nil && m < len(p) {
		err = io.ErrShortWrite
	}
	return m, w.n.pathErr("write", w.name, err)
}

func (w *writeFile) Name() string { return w.name }

// Sync does nothing: see NineP.
func (w *writeFile) Sync() error { return nil }

func (w *writeFile) Close() error { return w.n.pathErr("close", w.name, w.f.Close()) }

// SyncDir does nothing: see NineP.
func (n *NineP) SyncDir(string) error { return nil }

// ReadDir lists dir, in no particular order.
func (n *NineP) ReadDir(dir string) ([]fs.DirEntry, error) {
	d, err := n.walk(split(dir))
	if err != nil {
		return nil, n.pathErr("readdir", dir, err)
	}
	defer d.Close()
	if _, _, err := d.Open(p9.ReadOnly); err != nil {
		return nil, n.pathErr("readdir", dir, err)
	}
	var out []fs.DirEntry
	var off uint64
	for {
		ents, err := d.Readdir(off, ninePMsize/2)
		if err != nil {
			return nil, n.pathErr("readdir", dir, err)
		}
		if len(ents) == 0 {
			return out, nil
		}
		for _, e := range ents {
			off = e.Offset
			if e.Name == "." || e.Name == ".." {
				continue
			}
			typ := direntType(e)
			out = append(out, &dirEntry{n: n, path: path.Join(dir, e.Name), name: e.Name, typ: typ})
		}
	}
}

// direntType is a directory entry's type, from its QID. The entry's own
// type byte is not reliable: zerofs sends a Linux DT_* value there, and
// the p9 package's servers a QID type.
func direntType(e p9.Dirent) fs.FileMode {
	switch {
	case e.QID.Type&p9.TypeDir != 0:
		return fs.ModeDir
	case e.QID.Type&p9.TypeSymlink != 0:
		return fs.ModeSymlink
	}
	return 0
}

type dirEntry struct {
	n    *NineP
	path string
	name string
	typ  fs.FileMode

	once sync.Once
	fi   fs.FileInfo
	err  error
}

func (e *dirEntry) Name() string      { return e.name }
func (e *dirEntry) IsDir() bool       { return e.typ.IsDir() }
func (e *dirEntry) Type() fs.FileMode { return e.typ }

// Info stats the entry on first use: 9P directory entries carry no size.
func (e *dirEntry) Info() (fs.FileInfo, error) {
	e.once.Do(func() { e.fi, e.err = e.n.Lstat(e.path) })
	return e.fi, e.err
}

// ReadFile returns the contents of name.
func (n *NineP) ReadFile(name string) ([]byte, error) {
	f, err := n.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// WriteFile replaces name with data atomically, making its directory if
// needed.
func (n *NineP) WriteFile(name string, data []byte) error {
	dir := path.Dir("/" + name)
	if err := n.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := n.TempFile(dir, ".tmp-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = n.Rename(f.Name(), name)
	}
	if err != nil {
		n.Remove(f.Name())
	}
	return err
}

// ninePConn adapts p9.Client's wire protocol to zerofs's 9P server, which
// differs from the client in two places. It frames the client's outgoing
// messages (the client writes each in a few pieces, under its send lock),
// writes each whole message in one call, and rewrites:
//
//   - Tversion: the client asks for "9P2000.L.Google.N" and expects EAGAIN
//     to step down; zerofs answers "unknown" to anything but "9P2000.L".
//     Asking for plain "9P2000.L" makes the client use only base 9P2000.L
//     messages.
//   - Tfsync: the client sends the original form without the datasync
//     field, which zerofs rejects; the field is added (0, a full fsync).
type ninePConn struct {
	rwc io.ReadWriteCloser
	mu  sync.Mutex
	buf []byte
}

const (
	msgTversion = 100
	msgTfsync   = 50
)

func (c *ninePConn) Read(p []byte) (int, error) { return c.rwc.Read(p) }
func (c *ninePConn) Close() error               { return c.rwc.Close() }

func (c *ninePConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, p...)
	for len(c.buf) >= 7 {
		size := int(binary.LittleEndian.Uint32(c.buf))
		if size < 7 {
			return 0, fmt.Errorf("9p: bad message size %d", size)
		}
		if len(c.buf) < size {
			break
		}
		msg := c.buf[:size]
		switch msg[4] {
		case msgTversion:
			const ver = "9P2000.L"
			m := make([]byte, 13+len(ver))
			copy(m[4:11], msg[4:11]) // type, tag, msize
			binary.LittleEndian.PutUint16(m[11:], uint16(len(ver)))
			copy(m[13:], ver)
			msg = m
		case msgTfsync:
			if size == 11 { // size, type, tag, fid
				msg = append(append(make([]byte, 0, 15), msg...), 0, 0, 0, 0)
			}
		}
		binary.LittleEndian.PutUint32(msg, uint32(len(msg)))
		if _, err := c.rwc.Write(msg); err != nil {
			return 0, err
		}
		c.buf = c.buf[:copy(c.buf, c.buf[size:])]
	}
	return len(p), nil
}
