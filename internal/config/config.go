// Package config holds keepsync's on-disk configuration: where each role
// keeps its state, and the server.toml and node.toml files that init writes
// and run reads.
//
// Every role has a directory. The server's holds its tailcat key, the
// Perkeep identity and blobs, ZeroFS's state and the registry; the node's
// holds its tailcat key, the Syncthing certificate and index, and the local
// folder paths. Both hold a control socket that the other CLI commands use
// to talk to the running process (package ctl).
//
// Config files are TOML with owner-only permissions, because server.toml
// holds the bucket credentials and the ZeroFS encryption password.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"tailscale.com/tailcfg"

	"github.com/progrium/keepsync/internal/tcnet"
)

// Role directory names under the base directory.
const (
	ServerRole = "server"
	NodeRole   = "node"
)

// File names inside a role directory.
const (
	ServerFile    = "server.toml"
	NodeFile      = "node.toml"
	KeyFile       = "tailcat.key.json"
	ControlSocket = "ctl.sock"
)

// ErrNotInitialized is returned by Load when the role directory has no
// config file yet.
var ErrNotInitialized = errors.New("config: not initialized")

// BaseDir is the default base directory: $KEEPSYNC_DIR, else
// $XDG_DATA_HOME/keepsync, else ~/.local/share/keepsync.
func BaseDir() string {
	if d := os.Getenv("KEEPSYNC_DIR"); d != "" {
		return d
	}
	if d := os.Getenv("XDG_DATA_HOME"); d != "" {
		return filepath.Join(d, "keepsync")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".keepsync"
	}
	return filepath.Join(home, ".local", "share", "keepsync")
}

// RoleDir is the directory for role (ServerRole or NodeRole) under base.
func RoleDir(base, role string) string { return filepath.Join(base, role) }

// Network is how a process reaches tailcat's DERP relays.
type Network struct {
	// DERPMapURL overrides tailcat's default DERP map, for a self-hosted
	// derper. It may be an http(s) URL or a path to a local JSON file.
	DERPMapURL string `toml:"derp_map_url,omitempty,omitzero"`
	// RegionID pins the bootstrap DERP region from the map.
	RegionID int `toml:"derp_region_id,omitempty,omitzero"`
}

// Storage is the S3-compatible bucket ZeroFS persists to.
type Storage struct {
	// URL is the bucket and prefix, such as "s3://my-bucket/keepsync", or
	// "file:///path" for local storage (testing).
	URL             string `toml:"url"`
	Endpoint        string `toml:"endpoint,omitempty,omitzero"`
	Region          string `toml:"region,omitempty,omitzero"`
	AccessKeyID     string `toml:"access_key_id,omitempty,omitzero"`
	SecretAccessKey string `toml:"secret_access_key,omitempty,omitzero"`
	AllowHTTP       bool   `toml:"allow_http,omitempty,omitzero"`
}

// ZeroFS configures the server's ZeroFS supervisor.
type ZeroFS struct {
	// Binary is an explicit zerofs path (the --zerofs flag). Empty means
	// look on PATH, then in the server dir.
	Binary string `toml:"binary,omitempty,omitzero"`
	// Password is the ZeroFS encryption password.
	Password string `toml:"password"`
	// Mount is the mount mode: "fuse" (default), "v9fs" or "external".
	Mount string `toml:"mount,omitempty,omitzero"`
	// MountDir overrides the mount point (default <server dir>/zerofs/mnt).
	MountDir      string  `toml:"mount_dir,omitempty,omitzero"`
	CacheSizeGB   float64 `toml:"cache_size_gb,omitempty,omitzero"`
	MemoryCacheGB float64 `toml:"memory_cache_gb,omitempty,omitzero"`
}

// Duration is a time.Duration that reads and writes as "48h", "30s".
type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) { return []byte(time.Duration(d).String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// ParseDuration is time.ParseDuration plus a "d" (24h) suffix, so "7d" and
// "30d" work as in the docs.
func ParseDuration(s string) (time.Duration, error) {
	if n := len(s); n > 1 && s[n-1] == 'd' {
		var days float64
		if _, err := fmt.Sscanf(s[:n-1], "%g", &days); err == nil {
			return time.Duration(days * float64(24*time.Hour)), nil
		}
	}
	return time.ParseDuration(s)
}

// Server is server.toml.
type Server struct {
	// Dir is the server directory. It is not stored; Load sets it.
	Dir string `toml:"-"`

	Network Network `toml:"network"`
	Storage Storage `toml:"storage"`
	ZeroFS  ZeroFS  `toml:"zerofs"`
	// FlushInterval is how often the server flushes ZeroFS and advances
	// the durability watermark. Default 1m.
	FlushInterval Duration `toml:"flush_interval,omitempty,omitzero"`
}

// DefaultFlushInterval is the default Server.FlushInterval.
const DefaultFlushInterval = time.Minute

// KeyPath is the server's tailcat identity.
func (c *Server) KeyPath() string { return filepath.Join(c.Dir, KeyFile) }

// PerkeepDir holds the Perkeep identity, local blobs and index.
func (c *Server) PerkeepDir() string { return filepath.Join(c.Dir, "perkeep") }

// ZeroFSDir is the ZeroFS supervisor's data dir (config, cache, sockets,
// default mount point, and keepsync's own copy of the zerofs binary).
func (c *Server) ZeroFSDir() string { return filepath.Join(c.Dir, "zerofs") }

// RegistryPath is the file the registry state is saved to.
func (c *Server) RegistryPath() string { return filepath.Join(c.Dir, "registry.json") }

// SocketPath is the server's control socket.
func (c *Server) SocketPath() string { return filepath.Join(c.Dir, ControlSocket) }

// Flush returns FlushInterval or its default.
func (c *Server) Flush() time.Duration {
	if c.FlushInterval <= 0 {
		return DefaultFlushInterval
	}
	return time.Duration(c.FlushInterval)
}

// Folder is a synced folder attached on this node.
type Folder struct {
	Name string `toml:"name"`
	Path string `toml:"path"`
	// ID is the Syncthing folder ID, from the registry.
	ID string `toml:"id,omitempty,omitzero"`
}

// Node is node.toml.
type Node struct {
	// Dir is the node directory. It is not stored; Load sets it.
	Dir string `toml:"-"`

	// Name is this node's name as the server and peers see it.
	Name string `toml:"name"`
	// Server is the server's tailcat address ("tc…").
	Server  string  `toml:"server"`
	Network Network `toml:"network"`
	// KeepInterval is how often the keeper walks kept folders. Default 5m.
	KeepInterval Duration `toml:"keep_interval,omitempty,omitzero"`
	// ConflictHook is the global conflict hook script (milestone 5).
	ConflictHook string   `toml:"conflict_hook,omitempty,omitzero"`
	Folders      []Folder `toml:"folder"`
}

// KeyPath is the node's tailcat identity.
func (c *Node) KeyPath() string { return filepath.Join(c.Dir, KeyFile) }

// SyncthingDir holds the Syncthing certificate, index and generated config.
func (c *Node) SyncthingDir() string { return filepath.Join(c.Dir, "syncthing") }

// MembershipPath caches the last membership seen, so the node keeps syncing
// with its known peers when it starts while the server is unreachable.
func (c *Node) MembershipPath() string { return filepath.Join(c.Dir, "membership.json") }

// SocketPath is the node's control socket.
func (c *Node) SocketPath() string { return filepath.Join(c.Dir, ControlSocket) }

// Folder returns the attached folder by name.
func (c *Node) Folder(name string) (Folder, bool) {
	for _, f := range c.Folders {
		if f.Name == name {
			return f, true
		}
	}
	return Folder{}, false
}

// SetFolder adds or replaces an attached folder.
func (c *Node) SetFolder(f Folder) {
	for i := range c.Folders {
		if c.Folders[i].Name == f.Name {
			c.Folders[i] = f
			return
		}
	}
	c.Folders = append(c.Folders, f)
}

// RemoveFolder detaches a folder, reporting whether it was attached.
func (c *Node) RemoveFolder(name string) bool {
	for i := range c.Folders {
		if c.Folders[i].Name == name {
			c.Folders = append(c.Folders[:i], c.Folders[i+1:]...)
			return true
		}
	}
	return false
}

// LoadServer reads dir/server.toml.
func LoadServer(dir string) (*Server, error) {
	c := &Server{}
	if err := load(filepath.Join(dir, ServerFile), c); err != nil {
		return nil, err
	}
	c.Dir = dir
	return c, nil
}

// Save writes server.toml atomically with owner-only permissions.
func (c *Server) Save() error { return save(filepath.Join(c.Dir, ServerFile), c) }

// LoadNode reads dir/node.toml.
func LoadNode(dir string) (*Node, error) {
	c := &Node{}
	if err := load(filepath.Join(dir, NodeFile), c); err != nil {
		return nil, err
	}
	c.Dir = dir
	return c, nil
}

// Save writes node.toml atomically with owner-only permissions.
func (c *Node) Save() error { return save(filepath.Join(c.Dir, NodeFile), c) }

func load(path string, v any) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: no %s (run init first)", ErrNotInitialized, path)
	}
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(b, v); err != nil {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	return nil
}

func save(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".config-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if err := toml.NewEncoder(f).Encode(v); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// TransportOptions returns the tcnet options for a process whose identity
// is saved at keyFile. A DERPMapURL that is not an http(s) URL is read as a
// local DERP map file, and the region is pinned from it (RegionID, or the
// lowest region in the map).
func (n Network) TransportOptions(keyFile string) (tcnet.Options, error) {
	o := tcnet.Options{KeyFile: keyFile, RegionID: n.RegionID}
	u := n.DERPMapURL
	if u == "" || strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		o.DERPMapURL = u
		return o, nil
	}
	b, err := os.ReadFile(u)
	if err != nil {
		return o, fmt.Errorf("config: derp map: %w", err)
	}
	var dm tailcfg.DERPMap
	if err := json.Unmarshal(b, &dm); err != nil {
		return o, fmt.Errorf("config: derp map %s: %w", u, err)
	}
	id := n.RegionID
	if id == 0 {
		for rid := range dm.Regions {
			if id == 0 || int(rid) < id {
				id = int(rid)
			}
		}
	}
	r := dm.Regions[tailcfg.DERPRegionID(id)]
	if r == nil {
		return o, fmt.Errorf("config: derp map %s has no region %d", u, id)
	}
	o.Region, o.RegionID = r, 0
	return o, nil
}
