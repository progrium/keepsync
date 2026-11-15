// Package server wires the keepsync server together: the tailcat
// transport, the ZeroFS supervisor, the embedded perkeepd (signer and
// durability watermark), the registry, and the control socket that
// `keepsync server add/remove/nodes` talk to.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/pkserver"
	"github.com/progrium/keepsync/internal/registry"
	"github.com/progrium/keepsync/internal/tcnet"
	"github.com/progrium/keepsync/internal/zerofs"
)

// InitOptions configures Init.
type InitOptions struct {
	Dir     string
	Network config.Network
	Storage config.Storage
	// ZeroFS is an explicit zerofs binary (--zerofs).
	ZeroFS string
	// PasswordFile imports an existing ZeroFS password, to adopt a bucket.
	PasswordFile string
	// Mount and MountDir set the ZeroFS mount mode and mount point.
	Mount    string
	MountDir string
	// NoDownload fails instead of downloading the pinned zerofs.
	NoDownload bool
	Logf       func(format string, args ...any)
}

// InitResult is what `server init` prints.
type InitResult struct {
	Config     *config.Server
	Addr       api.Addr
	Key        api.NodeKey
	ZeroFS     string // the zerofs binary that run will use
	Downloaded bool
	Perkeep    pkserver.Identity
}

// Init creates a server directory: the tailcat key (and so the address),
// the Perkeep identity, the ZeroFS password and the server.toml. It fetches
// the pinned zerofs if none is found. Init connects to DERP once, to learn
// the address it prints.
func Init(ctx context.Context, o InitOptions) (*InitResult, error) {
	logf := o.Logf
	if logf == nil {
		logf = log.Printf
	}
	if _, err := config.LoadServer(o.Dir); err == nil {
		return nil, fmt.Errorf("server: %s is already initialized", o.Dir)
	}
	if o.Storage.URL == "" {
		return nil, errors.New("server: a bucket URL is required (--bucket s3://bucket/prefix)")
	}
	if err := os.MkdirAll(o.Dir, 0o700); err != nil {
		return nil, err
	}
	cfg := &config.Server{
		Dir:     o.Dir,
		Network: o.Network,
		Storage: o.Storage,
		ZeroFS:  config.ZeroFS{Binary: o.ZeroFS, Mount: o.Mount, MountDir: o.MountDir},
	}
	res := &InitResult{Config: cfg}

	// ZeroFS binary: lookup order, else download the pin.
	bin, err := zerofs.FindBinary(o.ZeroFS, cfg.ZeroFSDir())
	if errors.Is(err, zerofs.ErrNotFound) && !o.NoDownload {
		logf("downloading zerofs %s", zerofs.PinnedVersion)
		if err := os.MkdirAll(cfg.ZeroFSDir(), 0o700); err != nil {
			return nil, err
		}
		bin, err = zerofs.Pinned.Install(ctx, nil, cfg.ZeroFSDir())
		res.Downloaded = err == nil
	}
	if err != nil {
		return nil, fmt.Errorf("server: zerofs: %w", err)
	}
	res.ZeroFS = bin
	if ok, err := zerofs.Pinned.CheckPin(bin); err == nil && !ok {
		logf("warning: %s is not the pinned zerofs %s", bin, zerofs.PinnedVersion)
	}

	// ZeroFS password: import or generate.
	if o.PasswordFile != "" {
		cfg.ZeroFS.Password, err = zerofs.ImportPassword(o.PasswordFile)
	} else {
		cfg.ZeroFS.Password, err = zerofs.GeneratePassword()
	}
	if err != nil {
		return nil, fmt.Errorf("server: zerofs password: %w", err)
	}

	// Perkeep identity.
	if res.Perkeep, err = pkserver.Init(cfg.PerkeepDir()); err != nil {
		return nil, fmt.Errorf("server: perkeep identity: %w", err)
	}

	// tailcat key and address. Starting once saves the picked region in
	// the key, so the address stays the same.
	topts, err := cfg.Network.TransportOptions(cfg.KeyPath())
	if err != nil {
		return nil, err
	}
	tr, err := tcnet.New(ctx, topts)
	if err != nil {
		return nil, err
	}
	res.Addr, res.Key = tr.Addr(), tr.Key()
	tr.Close()

	if err := cfg.Save(); err != nil {
		return nil, err
	}
	return res, nil
}

// Server is a running keepsync server.
type Server struct {
	cfg  *config.Server
	logf func(format string, args ...any)

	tr  *tcnet.Transport
	zfs *zerofs.Supervisor
	pk  *pkserver.Server
	reg *registry.Server
	ctl *ctl.Server
	up  atomic.Bool // set once start has finished

	mu        sync.Mutex
	lastFlush time.Time
	flushErr  error
}

// Options configures Run.
type Options struct {
	Logf func(format string, args ...any)
	// Ready, if set, is called once the server is serving.
	Ready func(*Server)
}

// Run starts the server and blocks until ctx is done or ZeroFS exits, then
// stops cleanly: it drains replication, flushes ZeroFS so everything is in
// the bucket, and stops perkeepd and ZeroFS.
func Run(ctx context.Context, cfg *config.Server, o Options) (err error) {
	s := &Server{cfg: cfg, logf: o.Logf}
	if s.logf == nil {
		s.logf = log.Printf
	}
	defer func() {
		if cerr := s.shutdown(); err == nil {
			err = cerr
		}
	}()
	if err := s.start(ctx); err != nil {
		return err
	}
	s.up.Store(true)
	s.logf("server %s up at %s", s.tr.Key(), s.tr.Addr())
	if o.Ready != nil {
		o.Ready(s)
	}

	t := time.NewTicker(cfg.Flush())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.zfs.Done():
			return errors.New("server: zerofs exited")
		case <-t.C:
			s.flush(ctx)
		}
	}
}

func (s *Server) start(ctx context.Context) error {
	cfg := s.cfg
	// Claim the control socket first, so a second `server run` fails
	// before it touches ZeroFS.
	s.ctl = ctl.NewServer()
	s.handlers()
	if err := s.ctl.Listen(cfg.SocketPath()); err != nil {
		s.ctl = nil
		return err
	}

	topts, err := cfg.Network.TransportOptions(cfg.KeyPath())
	if err != nil {
		return err
	}
	if s.tr, err = tcnet.New(ctx, topts); err != nil {
		return err
	}

	bin, err := zerofs.FindBinary(cfg.ZeroFS.Binary, cfg.ZeroFSDir())
	if err != nil {
		return fmt.Errorf("server: zerofs: %w (run `keepsync server init`, or pass --zerofs)", err)
	}
	if ok, err := zerofs.Pinned.CheckPin(bin); err == nil && !ok {
		s.logf("warning: %s is not the pinned zerofs %s", bin, zerofs.PinnedVersion)
	}
	zfs, err := zerofs.New(zerofs.Config{
		Binary:  bin,
		DataDir: cfg.ZeroFSDir(),
		Storage: zerofs.Storage{
			URL:             cfg.Storage.URL,
			Endpoint:        cfg.Storage.Endpoint,
			Region:          cfg.Storage.Region,
			AccessKeyID:     cfg.Storage.AccessKeyID,
			SecretAccessKey: cfg.Storage.SecretAccessKey,
			AllowHTTP:       cfg.Storage.AllowHTTP,
		},
		Password:      cfg.ZeroFS.Password,
		Mount:         zerofs.MountMode(cfg.ZeroFS.Mount),
		MountDir:      cfg.ZeroFS.MountDir,
		CacheSizeGB:   cfg.ZeroFS.CacheSizeGB,
		MemoryCacheGB: cfg.ZeroFS.MemoryCacheGB,
	})
	if err != nil {
		return err
	}
	s.logf("starting zerofs (%s)", bin)
	if err := zfs.Start(ctx); err != nil {
		return fmt.Errorf("server: zerofs: %w", err)
	}
	s.zfs = zfs

	if s.pk, err = pkserver.Start(ctx, pkserver.Config{
		Dir:       cfg.PerkeepDir(),
		ZeroFS:    zfs,
		Transport: s.tr,
		Logf:      s.logf,
	}); err != nil {
		return fmt.Errorf("server: perkeep: %w", err)
	}

	if s.reg, err = registry.NewServer(ctx, registry.Config{
		Store:      &FileStore{Path: cfg.RegistryPath()},
		Signer:     s.pk,
		Durability: s.pk,
		Transport:  s.tr,
	}); err != nil {
		return fmt.Errorf("server: registry: %w", err)
	}
	return s.reg.Serve()
}

func (s *Server) flush(ctx context.Context) {
	err := s.pk.Flush(ctx)
	s.mu.Lock()
	s.flushErr = err
	if err == nil {
		s.lastFlush = time.Now()
	}
	s.mu.Unlock()
	if err != nil {
		s.logf("flush: %v", err)
	}
}

// shutdown stops whatever start brought up, in reverse order. A clean
// stop is Drain, Flush, Close, then ZeroFS Stop (which flushes again).
func (s *Server) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var errs []error
	if s.ctl != nil {
		s.ctl.Close()
	}
	if s.reg != nil {
		s.reg.Close()
	}
	if s.pk != nil {
		s.logf("draining replication and flushing")
		if err := s.pk.Drain(ctx); err != nil {
			errs = append(errs, fmt.Errorf("drain: %w", err))
		}
		if err := s.pk.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush: %w", err))
		}
		if err := s.pk.Close(); err != nil {
			errs = append(errs, fmt.Errorf("perkeep: %w", err))
		}
	}
	if s.zfs != nil {
		if err := s.zfs.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("zerofs stop: %w", err))
		}
	}
	if s.tr != nil {
		s.tr.Close()
	}
	if len(errs) == 0 && s.pk != nil {
		s.logf("stopped cleanly; everything is in the bucket")
	}
	return errors.Join(errs...)
}

// Addr is the server's tailcat address.
func (s *Server) Addr() api.Addr { return s.tr.Addr() }

// Registry is the running registry.
func (s *Server) Registry() *registry.Server { return s.reg }
