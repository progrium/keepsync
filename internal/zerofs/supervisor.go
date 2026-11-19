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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"perkeep.org/pkg/blobserver/files"
)

// Config configures a Supervisor.
type Config struct {
	// Binary is the zerofs executable (see FindBinary).
	Binary string
	// DataDir holds zerofs.toml, the cache, the sockets, and the default
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
	// ReconnectWindow is how long the supervisor keeps trying to bring
	// zerofs back after it exits or stops answering, before it gives up
	// (Health reports HealthDown, and the server has to be restarted).
	// Default DefaultReconnectWindow.
	ReconnectWindow time.Duration
	// CheckInterval is how often the supervisor checks that zerofs is
	// alive and reachable. Default 2s.
	CheckInterval time.Duration
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

// DefaultReconnectWindow is Config.ReconnectWindow's default.
const DefaultReconnectWindow = 5 * time.Minute

// Health states (Health.State).
const (
	// HealthUp: zerofs is running and reachable.
	HealthUp = "up"
	// HealthReconnecting: zerofs exited or stopped answering, and the
	// supervisor is restarting it and reconnecting, within the window.
	HealthReconnecting = "reconnecting"
	// HealthDown: the window passed without zerofs coming back. The
	// supervisor has stopped trying; restart the server.
	HealthDown = "down"
	// HealthStopped: not started, or stopped.
	HealthStopped = "stopped"
)

// Health is the supervisor's view of zerofs.
type Health struct {
	State string    `json:"state"`
	Since time.Time `json:"since,omitzero"` // when State began
	// Attempts is how many restarts have failed since the outage began.
	Attempts int    `json:"attempts,omitempty"`
	Err      string `json:"err,omitempty"` // the latest failure
	// Window is the reconnect window, for messages.
	Window time.Duration `json:"window,omitempty"`
	// Restarts counts the times the supervisor killed or found dead the
	// zerofs process and started it again. Writes zerofs had not flushed
	// when it died are lost (see Supervisor.Restarts).
	Restarts uint64 `json:"restarts,omitempty"`
}

// Error describes an outage, or returns "" when State is HealthUp.
func (h Health) Error() string {
	switch h.State {
	case HealthReconnecting:
		msg := fmt.Sprintf("zerofs unreachable since %s, reconnecting", h.Since.Format("15:04:05"))
		if h.Attempts > 0 {
			msg += fmt.Sprintf(" (%d failed attempts so far)", h.Attempts)
		}
		return msg + ": " + h.Err
	case HealthDown:
		return fmt.Sprintf("zerofs unreachable since %s; gave up reconnecting after %v; restart `keepsync server run`: %s",
			h.Since.Format("2006-01-02 15:04:05"), h.Window, h.Err)
	case HealthStopped:
		return "zerofs: not running"
	}
	return ""
}

// Supervisor runs ZeroFS as a child process and mounts it over 9P. It
// implements api.ZeroFS.
type Supervisor struct {
	cfg Config
	log *slog.Logger

	configPath  string
	ninePSocket string
	rpcSocket   string

	mu       sync.Mutex
	running  atomic.Bool   // mounted and usable; read without mu by wait
	restarts atomic.Uint64 // process restarts after a death or a kill
	proc     *exec.Cmd
	done     chan struct{} // closed when the zerofs process exits
	exitErr  error
	mount    mounter
	logFile  *os.File
	logStart int64     // zerofs.log's size when the current process started
	out      io.Writer // zerofs's output, kept for restarts
	env      []string  // zerofs's environment, kept for restarts

	stopping bool          // Stop has begun; guarded by mu
	stop     chan struct{} // closed by Stop, ends watch
	gaveUp   chan struct{} // closed when the reconnect window passes

	hmu    sync.Mutex
	health Health
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
	// zerofs runs with DataDir as its working directory, where a relative
	// binary path would resolve wrongly. A bare name is left for PATH.
	if strings.ContainsRune(cfg.Binary, filepath.Separator) {
		if cfg.Binary, err = filepath.Abs(cfg.Binary); err != nil {
			return nil, err
		}
	}
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
	if cfg.ReconnectWindow <= 0 {
		cfg.ReconnectWindow = DefaultReconnectWindow
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 2 * time.Second
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
	socks := SocketPaths(cfg.DataDir)
	s.ninePSocket, s.rpcSocket = socks[0], socks[1]
	return s, nil
}

// SocketPaths returns the unix sockets zerofs listens on for dataDir: 9P,
// then RPC.
func SocketPaths(dataDir string) []string {
	runDir := filepath.Join(dataDir, "run")
	return []string{filepath.Join(runDir, "9p.sock"), filepath.Join(runDir, "rpc.sock")}
}

// CheckSocketPaths reports whether dataDir is short enough for zerofs's
// sockets, so a caller can check before creating anything.
func CheckSocketPaths(dataDir string) error {
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	for _, p := range SocketPaths(dataDir) {
		if len(p) > maxSocketPath {
			return fmt.Errorf("zerofs: socket path %s is %d bytes, over the %d-byte limit for unix sockets; use a shorter --dir", p, len(p), maxSocketPath)
		}
	}
	return nil
}

// Dir is the mount point, or "" in MountNineP mode, where nothing is
// mounted (use FS).
func (s *Supervisor) Dir() string {
	if s.cfg.Mount == MountNineP {
		return ""
	}
	return s.cfg.MountDir
}

// FS is the 9P client in MountNineP mode, while running; otherwise nil.
func (s *Supervisor) FS() *NineP {
	if m, ok := s.mount.(*ninePMount); ok {
		return m.client()
	}
	return nil
}

// VFS is FS as a Perkeep files.VFS, or nil (not a nil *NineP) when FS is.
func (s *Supervisor) VFS() files.VFS {
	if fs := s.FS(); fs != nil {
		return fs
	}
	return nil
}

// ReadFile reads name, a slash-separated path relative to the root of the
// ZeroFS filesystem, through the mount or the 9P client.
func (s *Supervisor) ReadFile(name string) ([]byte, error) {
	if s.cfg.Mount == MountNineP {
		fs := s.FS()
		if fs == nil {
			return nil, errors.New("zerofs: not running")
		}
		return fs.ReadFile(name)
	}
	return os.ReadFile(filepath.Join(s.cfg.MountDir, filepath.FromSlash(name)))
}

// WriteFile replaces name (as in ReadFile) with data atomically. Like any
// write, it is durable in the bucket after the next Flush.
func (s *Supervisor) WriteFile(name string, data []byte) error {
	if s.cfg.Mount == MountNineP {
		fs := s.FS()
		if fs == nil {
			return errors.New("zerofs: not running")
		}
		return fs.WriteFile(name, data)
	}
	p := filepath.Join(s.cfg.MountDir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return writeFileAtomic(p, data, 0o600)
}

// ConfigPath is the generated zerofs.toml.
func (s *Supervisor) ConfigPath() string { return s.configPath }

// Done is closed when the supervisor gives up on zerofs: the reconnect
// window passed without it coming back (see Health). It is not closed by
// a zerofs exit the supervisor recovers from, nor by Stop.
func (s *Supervisor) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gaveUp == nil {
		s.gaveUp = make(chan struct{})
	}
	return s.gaveUp
}

// Health reports whether zerofs is up, being reconnected, or given up on.
func (s *Supervisor) Health() Health {
	s.hmu.Lock()
	defer s.hmu.Unlock()
	h := s.health
	if h.State == "" {
		h.State = HealthStopped
	}
	h.Window = s.cfg.ReconnectWindow
	h.Restarts = s.restarts.Load()
	return h
}

// Restarts counts the times zerofs died, or was killed for not answering,
// and was started again. Everything written since its last successful
// Flush may be gone from Dir after one, so a caller that remembers what
// it wrote compares this before and after a Flush: if it changed, the
// Flush covers only what the new process holds. A reconnect to a process
// that kept running (a dropped 9P connection) loses nothing and does not
// count.
func (s *Supervisor) Restarts() uint64 { return s.restarts.Load() }

func (s *Supervisor) setHealth(h Health) {
	s.hmu.Lock()
	s.health = h
	s.hmu.Unlock()
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
	if err := s.stopStale(s.cfg.StopTimeout); err != nil {
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
	s.out = s.cfg.Output
	if s.out == nil {
		f, err := os.OpenFile(filepath.Join(s.cfg.DataDir, "zerofs.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		s.logFile = f
		s.out = f
	}
	s.env = s.environ()

	ctx, cancel := context.WithTimeout(ctx, s.cfg.StartTimeout)
	defer cancel()
	if err := s.launchLocked(ctx); err != nil {
		s.closeLog()
		return err
	}
	s.stopping = false
	s.stop = make(chan struct{})
	if s.gaveUp == nil {
		s.gaveUp = make(chan struct{})
	}
	s.setHealth(Health{State: HealthUp, Since: time.Now()})
	go s.watch(s.stop)
	return nil
}

// launchLocked starts the zerofs process, waits for its sockets, and mounts
// (or connects). On failure the process is killed.
func (s *Supervisor) launchLocked(ctx context.Context) (err error) {
	// Stale sockets from a crash would make the readiness check pass early.
	for _, p := range []string{s.ninePSocket, s.rpcSocket} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	s.logStart = 0
	if s.logFile != nil {
		if st, err := s.logFile.Stat(); err == nil {
			s.logStart = st.Size()
		}
	}
	cmd := exec.Command(s.cfg.Binary, "run", "--config", s.configPath)
	cmd.Env = s.env
	cmd.Stdout = s.out
	cmd.Stderr = s.out
	cmd.Dir = s.cfg.DataDir
	setProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("zerofs: start: %w", err)
	}
	s.proc = cmd
	if err := os.WriteFile(s.pidFile(), []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o600); err != nil {
		s.log.Warn("zerofs: writing pid file", "err", err)
	}
	s.done = make(chan struct{})
	s.exitErr = nil
	go s.wait(cmd, s.done)

	defer func() {
		if err != nil {
			s.killProcLocked()
		}
	}()

	s.log.Info("zerofs starting", "pid", cmd.Process.Pid, "storage", s.cfg.Storage.URL)
	for _, sock := range []string{s.ninePSocket, s.rpcSocket} {
		if err := s.waitSocket(ctx, sock); err != nil {
			return err
		}
	}
	return s.mountLocked(ctx)
}

func (s *Supervisor) mountLocked(ctx context.Context) error {
	if err := s.mount.Mount(ctx, mountSpec{
		binary: s.cfg.Binary, socket: s.ninePSocket, dir: s.cfg.MountDir,
		env: s.env, out: s.out, done: s.done,
	}); err != nil {
		return fmt.Errorf("zerofs: mount %s: %w", s.cfg.MountDir, err)
	}
	s.running.Store(true)
	if s.cfg.Mount == MountNineP {
		s.log.Info("zerofs connected over 9p", "socket", s.ninePSocket)
	} else {
		s.log.Info("zerofs mounted", "dir", s.cfg.MountDir, "mode", s.cfg.Mount)
	}
	return nil
}

// watch checks zerofs every CheckInterval, and at once when the process
// exits, and reconnects when it is gone or unreachable.
func (s *Supervisor) watch(stop chan struct{}) {
	t := time.NewTicker(s.cfg.CheckInterval)
	defer t.Stop()
	for {
		s.mu.Lock()
		done := s.done
		s.mu.Unlock()
		select {
		case <-stop:
			return
		case <-done:
		case <-t.C:
		}
		err := s.check()
		if err == nil {
			continue
		}
		if !s.reconnect(stop, err) {
			return
		}
	}
}

// check returns why zerofs is unusable, or nil.
func (s *Supervisor) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return nil
	}
	if err := s.exited(); err != nil {
		return err
	}
	return s.mount.Err()
}

// reconnect brings zerofs back: it restarts the process if it is gone and
// mounts (or connects) again, retrying with backoff for ReconnectWindow.
// It returns false if it gave up or Stop was called.
func (s *Supervisor) reconnect(stop chan struct{}, cause error) bool {
	since := time.Now()
	s.setHealth(Health{State: HealthReconnecting, Since: since, Err: cause.Error()})
	s.log.Warn("zerofs lost; reconnecting", "err", cause, "window", s.cfg.ReconnectWindow)
	delay := time.Second
	for attempt := 1; ; attempt++ {
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.StartTimeout)
		err := s.restartLocked(ctx)
		cancel()
		s.mu.Unlock()
		if err == nil {
			s.setHealth(Health{State: HealthUp, Since: time.Now()})
			s.log.Info("zerofs reconnected", "after", time.Since(since).Round(time.Millisecond), "attempts", attempt)
			return true
		}
		if time.Since(since) >= s.cfg.ReconnectWindow {
			s.setHealth(Health{State: HealthDown, Since: since, Attempts: attempt, Err: err.Error()})
			s.log.Error("zerofs still unreachable; gave up reconnecting. Restart `keepsync server run`",
				"since", since, "attempts", attempt, "err", err)
			s.mu.Lock()
			close(s.gaveUp)
			s.mu.Unlock()
			return false
		}
		s.setHealth(Health{State: HealthReconnecting, Since: since, Attempts: attempt, Err: err.Error()})
		s.log.Warn("zerofs reconnect failed", "attempt", attempt, "err", err, "retry_in", delay)
		select {
		case <-stop:
			return false
		case <-time.After(delay):
		}
		delay = min(2*delay, 30*time.Second)
	}
}

// restartLocked mounts (or connects) again: first without restarting
// zerofs, if it is still running, then by restarting it.
func (s *Supervisor) restartLocked(ctx context.Context) error {
	s.running.Store(false)
	if s.proc != nil && s.exited() == nil {
		uctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = s.mount.Unmount(uctx)
		cancel()
		if err := s.mountLocked(ctx); err == nil {
			if err := s.mount.Err(); err == nil {
				return nil
			}
		}
		s.running.Store(false)
		s.log.Warn("zerofs is running but unreachable; restarting it")
	}
	s.restarts.Add(1)
	s.killProcLocked()
	return s.launchLocked(ctx)
}

// Flush returns nil only once everything written to Dir so far is in the
// bucket: it syncs the mount, then asks zerofs to flush to object storage.
func (s *Supervisor) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h := s.Health(); h.State != HealthUp {
		return errors.New(h.Error())
	}
	if !s.running.Load() {
		return errors.New("zerofs: not running")
	}
	return s.flushLocked(ctx)
}

func (s *Supervisor) flushLocked(ctx context.Context) error {
	if err := s.exited(); err != nil {
		return err
	}
	// Over a mount, writes may sit in the kernel until synced. A NineP
	// write is already in zerofs when it returns.
	if s.cfg.Mount != MountNineP {
		if err := syncDir(s.cfg.MountDir); err != nil {
			return fmt.Errorf("zerofs: sync %s: %w", s.cfg.MountDir, err)
		}
	}
	if err := s.mount.Err(); err != nil {
		return fmt.Errorf("zerofs: mount: %w", err)
	}
	cmd := exec.CommandContext(ctx, s.cfg.Binary, "flush", "--config", s.configPath)
	cmd.Env = s.env
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("zerofs: flush: %w: %s", err, strings.TrimSpace(buf.String()))
	}
	return nil
}

// Stop flushes, unmounts, and stops zerofs. A nil return means everything
// written to Dir is in the bucket. zerofs gets SIGTERM, which makes it drain
// and flush on its own as well; it is killed only if it outlives ctx (or
// StopTimeout).
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopping && s.stop != nil {
		s.stopping = true
		close(s.stop)
	}
	before := s.Health()
	defer s.setHealth(Health{State: HealthStopped, Since: time.Now()})
	defer func() {
		if m, ok := s.mount.(*ninePMount); ok {
			m.release()
		}
	}()
	if s.proc == nil {
		s.closeLog()
		if before.State == HealthDown {
			// Whatever was written since zerofs went away may be lost.
			return errors.New(before.Error())
		}
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
	os.Remove(s.pidFile())
	s.closeLog()
	return errors.Join(errs...)
}

// pidFile holds the pid of the running zerofs, so a later run can find it
// if this keepsync is killed before stopping it.
func (s *Supervisor) pidFile() string { return filepath.Join(s.cfg.DataDir, "zerofs.pid") }

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
	if err := CheckSocketPaths(s.cfg.DataDir); err != nil {
		return err
	}
	if s.cfg.Mount == MountNineP {
		return nil
	}
	return os.MkdirAll(s.cfg.MountDir, 0o700)
}

// maxSocketPath is the portable limit for a unix socket path (sun_path is 104
// bytes on macOS and FreeBSD, 108 on Linux, including the NUL).
const maxSocketPath = 103

func (s *Supervisor) environ() []string {
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
			if why := s.startupReason(); why != "" {
				return fmt.Errorf("zerofs: exited during startup (%v): %s; see %s", s.exitErr, why, s.logHint())
			}
			return fmt.Errorf("zerofs: exited during startup (%v); see %s", s.exitErr, s.logHint())
		case <-ctx.Done():
			return fmt.Errorf("zerofs: %s not ready: %w; see %s", path, ctx.Err(), s.logHint())
		case <-t.C:
		}
	}
}

// startupReason picks why zerofs failed from what it logged this start:
// an S3 error code and message if there is one (they end a long debug dump
// otherwise, as for a missing bucket), else the last line that mentions an
// error, else the last line.
func (s *Supervisor) startupReason() string {
	if s.logFile == nil {
		return ""
	}
	f, err := os.Open(s.logFile.Name())
	if err != nil {
		return ""
	}
	defer f.Close()
	const tail = 64 << 10
	start := s.logStart
	if st, err := f.Stat(); err == nil && st.Size()-start > tail {
		start = st.Size() - tail
	}
	b, err := io.ReadAll(io.NewSectionReader(f, start, tail))
	if err != nil && len(b) == 0 {
		return ""
	}
	return logReason(string(b))
}

var (
	s3CodeRE    = regexp.MustCompile(`<Code>([^<]+)</Code>`)
	s3MessageRE = regexp.MustCompile(`<Message>([^<]+)</Message>`)
	ansiRE      = regexp.MustCompile("\x1b\\[[0-9;]*[A-Za-z]")
)

func logReason(log string) string {
	log = ansiRE.ReplaceAllString(log, "")
	if m := s3CodeRE.FindAllStringSubmatch(log, -1); m != nil {
		why := "S3 error " + m[len(m)-1][1]
		if msg := s3MessageRE.FindAllStringSubmatch(log, -1); msg != nil {
			why += ": " + msg[len(msg)-1][1]
		}
		return why
	}
	var last, lastErr string
	for _, line := range strings.Split(log, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		last = line
		if strings.Contains(strings.ToLower(line), "error") {
			lastErr = line
		}
	}
	why := lastErr
	if why == "" {
		why = last
	}
	if len(why) > 300 {
		why = why[:300] + "..."
	}
	return why
}

func (s *Supervisor) logHint() string {
	if s.logFile != nil {
		return s.logFile.Name()
	}
	return "zerofs output"
}

// killProcLocked unmounts and kills the zerofs process, if any.
func (s *Supervisor) killProcLocked() {
	if s.mount != nil {
		uctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = s.mount.Unmount(uctx)
		cancel()
	}
	s.running.Store(false)
	if s.proc != nil {
		select {
		case <-s.done:
		default:
			_ = s.proc.Process.Kill()
			<-s.done
		}
		s.proc = nil
	}
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
