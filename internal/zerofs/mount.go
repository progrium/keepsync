package zerofs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// MountMode chooses how keepsync reaches the ZeroFS filesystem: through a
// mount at Dir, or (MountNineP) as a 9P client with no mount. Every mode
// speaks 9P to ZeroFS, never NFS: ZeroFS reports NFS writes as stable while
// they are still buffered.
type MountMode string

const (
	// MountFUSE runs ZeroFS's own 9P client, "zerofs mount", which mounts
	// through FUSE as the current user. Linux only; needs /dev/fuse and
	// fusermount3, but not root. It reconnects if zerofs restarts.
	MountFUSE MountMode = "fuse"
	// MountKernel mounts with the stock Linux v9fs client
	// ("mount -t 9p"). Needs root and the 9p and 9pnet_fd modules.
	MountKernel MountMode = "v9fs"
	// MountExternal mounts nothing: the operator has already mounted the
	// 9P export at MountDir by other means. Start checks that MountDir is a
	// mount point, so data is never written to a plain local directory.
	MountExternal MountMode = "external"
	// MountNineP mounts nothing: keepsync is itself the 9P client (see
	// NineP and Supervisor.FS), so it needs no FUSE, root or kernel
	// module, and nothing else on the machine sees the filesystem.
	MountNineP MountMode = "9p"
)

// MountModes lists the valid mount modes, the default first.
var MountModes = []MountMode{MountFUSE, MountNineP, MountKernel, MountExternal}

type mountSpec struct {
	binary string
	socket string
	dir    string
	env    []string
	out    io.Writer
	done   <-chan struct{} // zerofs exited
}

type mounter interface {
	Mount(ctx context.Context, m mountSpec) error
	Unmount(ctx context.Context) error
	// Err reports a mount that has failed since Mount returned.
	Err() error
}

func newMounter(mode MountMode) (mounter, error) {
	switch mode {
	case MountFUSE:
		return &fuseMount{}, nil
	case MountKernel:
		return &kernelMount{}, nil
	case MountExternal:
		return &externalMount{}, nil
	case MountNineP:
		return &ninePMount{}, nil
	}
	return nil, fmt.Errorf("zerofs: unknown mount mode %q", mode)
}

// fuseMount runs "zerofs mount" as a long-lived child.
type fuseMount struct {
	mu      sync.Mutex
	dir     string
	cmd     *exec.Cmd
	exited  chan struct{}
	exitErr error
}

func (f *fuseMount) Mount(ctx context.Context, m mountSpec) error {
	if err := ensureNotMounted(m.dir); err != nil {
		return err
	}
	// Writeback off: a write returns once zerofs has it, so a sync plus a
	// zerofs flush covers everything written before Flush was called.
	cmd := exec.Command(m.binary, "mount", "unix:"+m.socket, m.dir, "--writeback", "false")
	cmd.Env = m.env
	cmd.Stdout = m.out
	cmd.Stderr = m.out
	setProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	f.mu.Lock()
	f.dir, f.cmd, f.exited, f.exitErr = m.dir, cmd, exited, nil
	f.mu.Unlock()
	go func() {
		err := cmd.Wait()
		f.mu.Lock()
		f.exitErr = err
		f.mu.Unlock()
		close(exited)
	}()
	err := waitMounted(ctx, m.dir, exited, m.done)
	if err != nil {
		_ = f.Unmount(context.Background())
	}
	return err
}

func (f *fuseMount) Unmount(ctx context.Context) error {
	f.mu.Lock()
	cmd, exited, dir := f.cmd, f.exited, f.dir
	f.cmd = nil
	f.mu.Unlock()
	if cmd == nil {
		return nil
	}
	select {
	case <-exited:
	default:
		// zerofs mount unmounts and exits cleanly on SIGTERM.
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			<-exited
		}
	}
	// If the client died, the kernel may still hold a dead FUSE mount.
	if mounted, _ := isMountPoint(dir); mounted {
		if err := runQuiet(ctx, "fusermount3", "-u", dir); err != nil {
			if err2 := runQuiet(ctx, "fusermount", "-u", dir); err2 != nil {
				return errors.Join(err, err2)
			}
		}
	}
	return nil
}

func (f *fuseMount) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exited == nil {
		return nil
	}
	select {
	case <-f.exited:
		return fmt.Errorf("zerofs mount exited: %v", f.exitErr)
	default:
		return nil
	}
}

// kernelMount uses the stock Linux v9fs client.
type kernelMount struct{ dir string }

func (k *kernelMount) Mount(ctx context.Context, m mountSpec) error {
	if err := ensureNotMounted(m.dir); err != nil {
		return err
	}
	// cache=none keeps writes synchronous, for the same reason as the FUSE
	// client's writeback=false.
	opts := "trans=unix,version=9p2000.L,access=user,cache=none"
	if err := runQuiet(ctx, "mount", "-t", "9p", "-o", opts, m.socket, m.dir); err != nil {
		return err
	}
	k.dir = m.dir
	return waitMounted(ctx, m.dir, nil, m.done)
}

func (k *kernelMount) Unmount(ctx context.Context) error {
	if k.dir == "" {
		return nil
	}
	dir := k.dir
	k.dir = ""
	if mounted, _ := isMountPoint(dir); !mounted {
		return nil
	}
	return runQuiet(ctx, "umount", dir)
}

// Err is always nil: stock v9fs does not report a dead session until the next
// operation fails, which the sync in Flush surfaces.
func (k *kernelMount) Err() error { return nil }

type externalMount struct{ dir string }

func (e *externalMount) Mount(ctx context.Context, m mountSpec) error {
	mounted, err := isMountPoint(m.dir)
	if err != nil {
		return err
	}
	if !mounted {
		return fmt.Errorf("mount mode %q: %s is not a mount point", MountExternal, m.dir)
	}
	e.dir = m.dir
	return nil
}

func (e *externalMount) Unmount(context.Context) error { return nil }

func (e *externalMount) Err() error {
	if e.dir == "" {
		return nil
	}
	if mounted, err := isMountPoint(e.dir); err != nil || !mounted {
		return fmt.Errorf("%s is no longer a mount point", e.dir)
	}
	return nil
}

func ensureNotMounted(dir string) error {
	mounted, err := isMountPoint(dir)
	if err != nil {
		return err
	}
	if mounted {
		return fmt.Errorf("%s is already a mount point (left over from a crash? unmount it first)", dir)
	}
	return nil
}

func waitMounted(ctx context.Context, dir string, client, server <-chan struct{}) error {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		mounted, err := isMountPoint(dir)
		if err != nil {
			return err
		}
		if mounted {
			return nil
		}
		select {
		case <-client:
			return errors.New("mount client exited before the mount appeared")
		case <-server:
			return errors.New("zerofs exited before the mount appeared")
		case <-ctx.Done():
			return fmt.Errorf("mount did not appear: %w", ctx.Err())
		case <-t.C:
		}
	}
}

// isMountPoint reports whether dir is on a different device than its parent.
func isMountPoint(dir string) (bool, error) {
	st, err := os.Stat(dir)
	if err != nil {
		// A dead FUSE mount answers ENOTCONN; it is still a mount point.
		if errors.Is(err, syscall.ENOTCONN) {
			return true, nil
		}
		return false, err
	}
	pst, err := os.Stat(filepath.Dir(dir))
	if err != nil {
		return false, err
	}
	d1, ok1 := deviceOf(st)
	d2, ok2 := deviceOf(pst)
	if !ok1 || !ok2 {
		return false, errors.New("cannot tell mount points apart on this platform")
	}
	return d1 != d2, nil
}

func runQuiet(ctx context.Context, name string, args ...string) error {
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(buf.String()))
	}
	return nil
}

// ninePMount connects a NineP client instead of mounting. The client is
// made once and redialled in place after a reconnect, so whoever holds it
// (perkeepd's blob store) keeps working.
type ninePMount struct {
	mu sync.Mutex
	fs *NineP
}

func (n *ninePMount) Mount(ctx context.Context, m mountSpec) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fs != nil {
		return n.fs.Redial(ctx, m.socket)
	}
	fs, err := DialNineP(ctx, m.socket)
	if err != nil {
		return err
	}
	n.fs = fs
	return nil
}

// Unmount drops the connection but keeps the client for the next Mount.
func (n *ninePMount) Unmount(context.Context) error {
	if fs := n.client(); fs != nil {
		return fs.Close()
	}
	return nil
}

// release forgets the client, after a final Unmount.
func (n *ninePMount) release() {
	n.mu.Lock()
	n.fs = nil
	n.mu.Unlock()
}

func (n *ninePMount) Err() error {
	if fs := n.client(); fs != nil {
		return fs.Err()
	}
	return nil
}

func (n *ninePMount) client() *NineP {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fs
}
