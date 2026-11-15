package kit

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/zerofs"
)

// SourceFor returns what a kit of the server configured by cfg holds. addr
// is the server's tailcat address, or "" if not known yet.
func SourceFor(cfg *config.Server, addr api.Addr) (Source, error) {
	bin, err := zerofs.FindBinary(cfg.ZeroFS.Binary, cfg.ZeroFSDir())
	if err != nil {
		return Source{}, err
	}
	return Source{
		ServerAddr:     addr,
		Storage:        storageOf(cfg),
		ZeroFSPassword: cfg.ZeroFS.Password,
		Mode:           cfg.StorageMode(),
		Mount:          cfg.ZeroFS.Mount,
		PerkeepDir:     cfg.PerkeepDir(),
		TailcatKey:     cfg.KeyPath(),
		ConfigFile:     filepath.Join(cfg.Dir, config.ServerFile),
		RegistryFile:   cfg.RegistryPath(),
		ZeroFSBinary:   bin,
	}, nil
}

// Write builds the recovery kit of the server configured by cfg at out
// (conventionally FileName) and returns its passphrase, to be shown once.
// This is what `server init` and `server kit` call.
func Write(ctx context.Context, cfg *config.Server, out string, addr api.Addr) (passphrase string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	src, err := SourceFor(cfg, addr)
	if err != nil {
		return "", err
	}
	return Build(src, out)
}

// LayoutFor returns where Recover puts a server whose directory is dir
// (config.RoleDir(config.BaseDir(), config.ServerRole) by default), in the
// layout package config expects, so `server run` finds it.
func LayoutFor(dir string) Layout {
	c := &config.Server{Dir: dir}
	return Layout{
		PerkeepDir:    c.PerkeepDir(),
		TailcatKey:    c.KeyPath(),
		ZeroFSDataDir: c.ZeroFSDir(),
		ConfigFile:    filepath.Join(dir, config.ServerFile),
		RegistryFile:  c.RegistryPath(),
	}
}

// RecoverDir is Recover into the server directory dir. The recovered
// server.toml keeps the old server's settings; if it names a ZeroFS mount
// directory, that is used.
func RecoverDir(ctx context.Context, k *Kit, dir string, o RecoverOptions) (*RestoreStats, error) {
	if dir == "" {
		return nil, errors.New("kit: no server directory")
	}
	o.Layout = LayoutFor(dir)
	stats, err := Recover(ctx, k, o)
	if err != nil {
		return stats, err
	}
	// The recovered server.toml is the old server's; record the mode it
	// was recovered in, which may differ.
	mode := o.mode(k)
	cfg, err := config.LoadServer(dir)
	switch {
	case errors.Is(err, config.ErrNotInitialized):
		cfg = &config.Server{Dir: dir, Mode: mode} // no config in the kit
	case err != nil:
		return stats, err
	case cfg.StorageMode() != mode:
		cfg.Mode = mode
		if err := cfg.Save(); err != nil {
			return stats, err
		}
	}
	cfg.Mode = mode
	return stats, cfg.WriteModeFile()
}

func storageOf(cfg *config.Server) zerofs.Storage {
	s := cfg.Storage
	return zerofs.Storage{
		URL:             s.URL,
		Endpoint:        s.Endpoint,
		Region:          s.Region,
		AccessKeyID:     s.AccessKeyID,
		SecretAccessKey: s.SecretAccessKey,
		AllowHTTP:       s.AllowHTTP,
	}
}
