package zerofs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Config configures a Supervisor.
type Config struct {
	// Binary is the zerofs executable (see FindBinary).
	Binary string
	// DataDir holds zerofs.toml, the cache, the sockets and the default
	// mount point. It is created with owner-only permissions.
	DataDir string
	Storage Storage
	// Password is the ZeroFS encryption password (see GeneratePassword).
	Password string
	// Mount chooses how the filesystem is mounted at Dir. Default MountFUSE.
	Mount MountMode
	// MountDir is where the filesystem is mounted. Default DataDir/mnt.
	MountDir string
	// CacheSizeGB is the local disk cache size. Default DefaultCacheSizeGB.
	CacheSizeGB float64
	// MemoryCacheGB is the in-memory cache size. Default
	// DefaultMemoryCacheGB.
	MemoryCacheGB float64
	// StartTimeout bounds how long Start waits for zerofs and the mount to
	// come up. Default 2m (a first start against a remote bucket can be slow).
	StartTimeout time.Duration
	// StopTimeout bounds how long Stop waits for zerofs to exit after
	// SIGTERM before killing it, if ctx has no earlier deadline. Default 2m.
	StopTimeout time.Duration
	// Output receives zerofs's stdout and stderr. Default DataDir/zerofs.log.
	Output io.Writer
	// Env is appended to the environment zerofs runs with.
	Env    []string
	Logger *slog.Logger
	// Release is the pin the binary is checked against. Default Pinned.
	Release *Release
}

// Cache defaults. ZeroFS's caches are read caches only: writes are buffered
// in RAM (an open 256 MiB segment plus up to four sealing ones) and PUT
// straight to the bucket, so the cache size changes neither write buffering
// nor what Flush has to do. A read that misses goes to the bucket and fills
// the cache. keepsync's replica is written far more than it is read (the
// diskpacked index, and recovery), so the defaults are ZeroFS v2.3.7's
// floors: it gives each half of the disk cache at least 1 GiB and each half
// of the memory cache at least 32 MiB, whatever is configured.
const (
	DefaultCacheSizeGB   = 2
	DefaultMemoryCacheGB = 0.25
)

// Supervisor runs ZeroFS as a child process and mounts it over 9P. It
// implements api.ZeroFS.
type Supervisor struct {
	cfg Config
	log *slog.Logger

	configPath  string
	ninePSocket string
	rpcSocket   string

	mu      sync.Mutex
	running atomic.Bool // mounted and usable; read without mu by wait
	proc    *exec.Cmd
	done    chan struct{} // closed when the zerofs process exits
	exitErr error
	mount   mounter
	logFile *os.File
}

var _ api.ZeroFS = (*Supervisor)(nil)

// New checks cfg and returns a stopped Supervisor.
func New(cfg Config) (*Supervisor, error) {
	if cfg.Binary == "" {
		return nil, errors.New("zerofs: Config.Binary is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("zerofs: Config.DataDir is required")
	}
	if cfg.Storage.URL == "" {
		return nil, errors.New("zerofs: Config.Storage.URL is required")
	}
	if err := ValidatePassword(cfg.Password); err != nil {
		return nil, err
	}
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	cfg.DataDir = dataDir
	if cfg.Mount == "" {
		cfg.Mount = MountFUSE
	}
	if cfg.MountDir == "" {
		cfg.MountDir = filepath.Join(cfg.DataDir, "mnt")
	}
	if cfg.MountDir, err = filepath.Abs(cfg.MountDir); err != nil {
		return nil, err
	}
	if cfg.CacheSizeGB == 0 {
		cfg.CacheSizeGB = DefaultCacheSizeGB
	}
	if cfg.MemoryCacheGB == 0 {
		cfg.MemoryCacheGB = DefaultMemoryCacheGB
	}
	if cfg.StartTimeout == 0 {
		cfg.StartTimeout = 2 * time.Minute
	}
	if cfg.StopTimeout == 0 {
		cfg.StopTimeout = 2 * time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Release == nil {
		cfg.Release = &Pinned
	}
	// ZeroFS expands $VAR in every config value; keepsync writes these
	// verbatim, so a '$' would be misread.
	for name, v := range map[string]string{
		"Storage.URL": cfg.Storage.URL, "Storage.Endpoint": cfg.Storage.Endpoint,
		"Storage.Region": cfg.Storage.Region, "DataDir": cfg.DataDir,
	} {
		if strings.ContainsRune(v, '$') {
			return nil, fmt.Errorf("zerofs: %s must not contain '$'", name)
		}
	}
	m, err := newMounter(cfg.Mount)
	if err != nil {
		return nil, err
	}

	s := &Supervisor{cfg: cfg, log: cfg.Logger.With("component", "zerofs"), mount: m}
	s.configPath = filepath.Join(cfg.DataDir, "zerofs.toml")
	runDir := filepath.Join(cfg.DataDir, "run")
	s.ninePSocket = filepath.Join(runDir, "9p.sock")
	s.rpcSocket = filepath.Join(runDir, "rpc.sock")
	return s, nil
}

// Dir is the mount point the diskpacked replica writes to.
func (s *Supervisor) Dir() string { return s.cfg.MountDir }

// ConfigPath is the generated zerofs.toml.
func (s *Supervisor) ConfigPath() string { return s.configPath }

// Done is closed when the zerofs process exits, expectedly or not.
func (s *Supervisor) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return s.done
}

// Start writes the config, starts zerofs, waits for its 9P and RPC sockets,
// and mounts the filesystem at Dir. It returns once Dir is usable.
func (s *Supervisor) Start(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc != nil {
		return errors.New("zerofs: already running")
	}

	if ok, perr := s.cfg.Release.CheckPin(s.cfg.Binary); perr != nil {
		return fmt.Errorf("zerofs: %w", perr)
	} else if !ok {
		s.log.Warn("zerofs binary does not match the pinned release; the bucket format is tied to the ZeroFS version",
			"binary", s.cfg.Binary, "pinned", s.cfg.Release.Version)
	}

	if err := s.prepareDirs(); err != nil {
		return err
	}
	if err := writeFileAtomic(s.configPath, []byte(renderConfig(configParams{
		Storage:     s.cfg.Storage,
		CacheDir:    filepath.Join(s.cfg.DataDir, "cache"),
		CacheSizeGB: s.cfg.CacheSizeGB,
		MemoryGB:    s.cfg.MemoryCacheGB,
		NinePSocket: s.ninePSocket,
		RPCSocket:   s.rpcSocket,
	})), 0o600); err != nil {
		return err
	}
	// Stale sockets from a crash would make the readiness check pass early.
	for _, p := range []string{s.ninePSocket, s.rpcSocket} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	out := s.cfg.Output
	if out == nil {
		f, err := os.OpenFile(filepath.Join(s.cfg.DataDir, "zerofs.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		s.logFile = f
		out = f
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.StartTimeout)
	defer cancel()

	cmd := exec.Command(s.cfg.Binary, "run", "--config", s.configPath)
	cmd.Env = s.env()
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Dir = s.cfg.DataDir
	setProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		s.closeLog()
		return fmt.Errorf("zerofs: start: %w", err)
	}
	s.proc = cmd
	s.done = make(chan struct{})
	s.exitErr = nil
	go s.wait(cmd, s.done)

	defer func() {
		if err != nil {
			s.killLocked()
		}
	}()

	s.log.Info("zerofs starting", "pid", cmd.Process.Pid, "storage", s.cfg.Storage.URL)
	for _, sock := range []string{s.ninePSocket, s.rpcSocket} {
		if err := s.waitSocket(ctx, sock); err != nil {
			return err
		}
	}
	if err := s.mount.Mount(ctx, mountSpec{
		binary: s.cfg.Binary, socket: s.ninePSocket, dir: s.cfg.MountDir,
		env: cmd.Env, out: out, done: s.done,
	}); err != nil {
		return fmt.Errorf("zerofs: mount %s: %w", s.cfg.MountDir, err)
	}
	s.running.Store(true)
	s.log.Info("zerofs mounted", "dir", s.cfg.MountDir, "mode", s.cfg.Mount)
	return nil
}

// Flush returns nil only once everything written to Dir so far is in the
// bucket: it syncs the mount, then asks zerofs to flush to object storage.
func (s *Supervisor) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running.Load() {
		return errors.New("zerofs: not running")
	}
	return s.flushLocked(ctx)
}

func (s *Supervisor) flushLocked(ctx context.Context) error {
	if err := s.exited(); err != nil {
		return err
	}
	if err := syncDir(s.cfg.MountDir); err != nil {
		return fmt.Errorf("zerofs: sync %s: %w", s.cfg.MountDir, err)
	}
	if err := s.mount.Err(); err != nil {
		return fmt.Errorf("zerofs: mount: %w", err)
	}
	cmd := exec.CommandContext(ctx, s.cfg.Binary, "flush", "--config", s.configPath)
	cmd.Env = s.env()
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("zerofs: flush: %w: %s", err, strings.TrimSpace(buf.String()))
	}
	return nil
}

// Stop flushes, unmounts and stops zerofs. A nil return means everything
// written to Dir is in the bucket. zerofs gets SIGTERM, which makes it drain
// and flush on its own as well; it is killed only if it outlives ctx (or
// StopTimeout).
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc == nil {
		return nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.StopTimeout)
		defer cancel()
	}

	var errs []error
	if s.running.Load() {
		if err := s.flushLocked(ctx); err != nil {
			errs = append(errs, err)
		}
		if err := s.mount.Unmount(ctx); err != nil {
			errs = append(errs, fmt.Errorf("zerofs: unmount %s: %w", s.cfg.MountDir, err))
		}
	}
	s.running.Store(false)

	done := s.done
	select {
	case <-done:
	default:
		_ = s.proc.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-ctx.Done():
			_ = s.proc.Process.Kill()
			<-done
			errs = append(errs, fmt.Errorf("zerofs: killed after stop timeout: %w", ctx.Err()))
		}
	}
	if err := s.exitErr; err != nil && !isSignalExit(err, syscall.SIGTERM) {
		errs = append(errs, fmt.Errorf("zerofs: exited: %w", err))
	}
	s.proc = nil
	s.closeLog()
	return errors.Join(errs...)
}

func (s *Supervisor) prepareDirs() error {
	for _, d := range []string{s.cfg.DataDir, filepath.Join(s.cfg.DataDir, "cache"), filepath.Dir(s.ninePSocket)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
		// The 9P server trusts any client that can reach its socket, so the
		// directory holding it must be private.
		if err := os.Chmod(d, 0o700); err != nil {
			return err
		}
	}
	if len(s.ninePSocket) > maxSocketPath {
		return fmt.Errorf("zerofs: socket path %s is longer than %d bytes; use a shorter DataDir", s.ninePSocket, maxSocketPath)
	}
	return os.MkdirAll(s.cfg.MountDir, 0o700)
}

// maxSocketPath is the portable limit for a unix socket path (sun_path is 104
// bytes on macOS and FreeBSD, 108 on Linux, including the NUL).
const maxSocketPath = 103

func (s *Supervisor) env() []string {
	env := append(os.Environ(),
		envPassword+"="+s.cfg.Password,
		envAccessKey+"="+s.cfg.Storage.AccessKeyID,
		envSecretKey+"="+s.cfg.Storage.SecretAccessKey,
	)
	return append(env, s.cfg.Env...)
}

func (s *Supervisor) wait(cmd *exec.Cmd, done chan struct{}) {
	err := cmd.Wait()
	if s.running.Load() {
		s.log.Error("zerofs exited unexpectedly", "err", err)
	}
	// exitErr is written before done is closed and read only after it is.
	s.exitErr = err
	close(done)
}

// exited reports an error if the zerofs process is gone.
func (s *Supervisor) exited() error {
	select {
	case <-s.done:
		if s.exitErr != nil {
			return fmt.Errorf("zerofs: process exited: %w", s.exitErr)
		}
		return errors.New("zerofs: process exited")
	default:
		return nil
	}
}

func (s *Supervisor) waitSocket(ctx context.Context, path string) error {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		if c, err := net.Dial("unix", path); err == nil {
			c.Close()
			return nil
		}
		select {
		case <-s.done:
			return fmt.Errorf("zerofs: exited during startup (%v); see %s", s.exitErr, s.logHint())
		case <-ctx.Done():
			return fmt.Errorf("zerofs: %s not ready: %w; see %s", path, ctx.Err(), s.logHint())
		case <-t.C:
		}
	}
}

func (s *Supervisor) logHint() string {
	if s.logFile != nil {
		return s.logFile.Name()
	}
	return "zerofs output"
}

func (s *Supervisor) killLocked() {
	if s.mount != nil {
		uctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = s.mount.Unmount(uctx)
		cancel()
	}
	if s.proc != nil {
		_ = s.proc.Process.Kill()
		<-s.done
		s.proc = nil
	}
	s.closeLog()
}

func (s *Supervisor) closeLog() {
	if s.logFile != nil {
		s.logFile.Close()
		s.logFile = nil
	}
}

func isSignalExit(err error, sig syscall.Signal) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == sig
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
