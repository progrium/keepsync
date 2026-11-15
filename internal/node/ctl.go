package node

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/keeper"
	"github.com/progrium/keepsync/internal/registry"
)

// Control socket methods. The CLI calls these by name.
const (
	MethodStatus     = "status"
	MethodFolderAdd  = "folder.add"
	MethodFolderJoin = "folder.join"
	MethodFolderLs   = "folder.ls"
	MethodFolderRm   = "folder.rm"
	MethodKeep       = "keep"
	MethodKeepStatus = "keep.status"
)

// FolderParams is the body of MethodFolderAdd, MethodFolderJoin and
// MethodFolderRm (which ignores Path).
type FolderParams struct {
	Name string `json:"name"`
	Path string `json:"path,omitempty"`
}

// RmResult is the result of MethodFolderRm.
type RmResult struct {
	Left    bool   `json:"left"`              // removed from the folder's members
	Warning string `json:"warning,omitempty"` // why the registry was not updated
}

// KeepParams is the body of MethodKeep. Zero Grace and MaxDefer take the
// defaults; zero Expire means never expire.
type KeepParams struct {
	Folder   string        `json:"folder"`
	Grace    time.Duration `json:"grace,omitempty"`
	MaxDefer time.Duration `json:"maxDefer,omitempty"`
	Expire   time.Duration `json:"expire,omitempty"`
}

// KeepResult is the result of MethodKeep.
type KeepResult struct {
	Permanode api.BlobRef    `json:"permanode"`
	Policy    api.KeepPolicy `json:"policy"`
}

// FolderInfo is one folder in MethodFolderLs and MethodStatus.
type FolderInfo struct {
	Name     string          `json:"name"`
	ID       string          `json:"id"`
	Path     string          `json:"path,omitempty"` // empty: not attached here
	Members  []string        `json:"members"`        // node names
	Keep     *api.KeepPolicy `json:"keep,omitempty"`
	Keeper   bool            `json:"keeper,omitempty"` // this node keeps it
	Attached bool            `json:"attached"`
}

// PeerInfo is one peer in MethodStatus.
type PeerInfo struct {
	Name      string       `json:"name"`
	Device    api.DeviceID `json:"device"`
	Connected bool         `json:"connected"`
}

// Status is the result of MethodStatus.
type Status struct {
	Name       string       `json:"name"`
	Key        api.NodeKey  `json:"key"`
	Addr       api.Addr     `json:"addr"`
	Device     api.DeviceID `json:"device"`
	Server     api.Addr     `json:"server"`
	Registered bool         `json:"registered"`
	Watching   bool         `json:"watching"`
	ServerErr  string       `json:"serverErr,omitempty"`
	ServerSeen time.Time    `json:"serverSeen,omitzero"`
	Version    uint64       `json:"version"`
	Peers      []PeerInfo   `json:"peers"`
	Folders    []FolderInfo `json:"folders"`
}

// KeepStatus is the result of MethodKeepStatus.
type KeepStatus struct {
	Folder    string         `json:"folder"`
	Keeper    bool           `json:"keeper"`
	Counts    map[string]int `json:"counts"`
	Status    keeper.Status  `json:"status"`
	LastPass  time.Time      `json:"lastPass,omitzero"`
	LastError string         `json:"lastError,omitempty"`
	Last      keeper.Report  `json:"-"`
}

func (n *Node) handlers() {
	ready := func() error {
		if !n.up.Load() {
			return errors.New("node is still starting")
		}
		return nil
	}
	ctl.HandleFunc(n.ctl, MethodStatus, func(ctx context.Context, _ struct{}) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return n.status(), nil
	})
	ctl.HandleFunc(n.ctl, MethodFolderAdd, func(ctx context.Context, p FolderParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return n.attach(ctx, p, n.reg.AddFolder)
	})
	ctl.HandleFunc(n.ctl, MethodFolderJoin, func(ctx context.Context, p FolderParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return n.attach(ctx, p, n.reg.JoinFolder)
	})
	ctl.HandleFunc(n.ctl, MethodFolderLs, func(ctx context.Context, _ struct{}) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return n.status().Folders, nil
	})
	ctl.HandleFunc(n.ctl, MethodFolderRm, func(ctx context.Context, p FolderParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		if _, err := n.localFolder(p.Name); err != nil {
			return nil, err
		}
		// Leave the folder in the registry so this node stops being a
		// member (and keeper). If the server cannot be told, still stop
		// syncing here and say so; the files stay either way.
		var res RmResult
		if err := n.reg.LeaveFolder(ctx, p.Name); err != nil {
			res.Warning = err.Error()
		} else {
			res.Left = true
		}
		n.cfgMu.Lock()
		ok := n.cfg.RemoveFolder(p.Name)
		var err error
		if ok {
			err = n.cfg.Save()
		}
		n.cfgMu.Unlock()
		if !ok {
			return nil, errors.New("folder " + p.Name + " is not attached here")
		}
		if err != nil {
			return nil, err
		}
		n.reconcile(n.ctx)
		return res, nil
	})
	ctl.HandleFunc(n.ctl, MethodKeep, func(ctx context.Context, p KeepParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		lf, err := n.localFolder(p.Folder)
		if err != nil {
			return nil, err
		}
		pol := keeper.WithDefaults(api.KeepPolicy{Grace: p.Grace, MaxDefer: p.MaxDefer, Expire: p.Expire})
		if err := registry.ValidatePolicy(pol); err != nil {
			return nil, err
		}
		pn, err := keeper.Keep(ctx, n.reg, p.Folder, lf.Path, pol)
		if err != nil {
			return nil, err
		}
		pol.Permanode = pn
		return KeepResult{Permanode: pn, Policy: pol}, nil
	})
	ctl.HandleFunc(n.ctl, MethodKeepStatus, func(ctx context.Context, p FolderParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		if _, err := n.localFolder(p.Name); err != nil {
			return nil, err
		}
		n.mu.Lock()
		k := n.keepers[p.Name]
		n.mu.Unlock()
		if k == nil {
			return nil, errors.New("folder " + p.Name + " is not kept (or the server has not confirmed it yet)")
		}
		st, err := k.k.Status(ctx)
		if err != nil {
			return nil, err
		}
		out := KeepStatus{Folder: p.Name, Keeper: k.k.IsKeeper(), Status: st, Counts: map[string]int{}}
		for s, c := range st.Counts() {
			out.Counts[s.String()] = c
		}
		n.mu.Lock()
		out.LastPass, out.LastError = k.lastAt, k.lastE
		n.mu.Unlock()
		return out, nil
	})
}

// attach creates or joins a folder in the registry and attaches it at path.
func (n *Node) attach(ctx context.Context, p FolderParams, call func(context.Context, string) (api.Folder, error)) (any, error) {
	if p.Name == "" {
		return nil, errors.New("a folder name is required")
	}
	dir, err := absDir(p.Path)
	if err != nil {
		return nil, err
	}
	n.cfgMu.Lock()
	for _, f := range n.cfg.Folders {
		if f.Path == dir && f.Name != p.Name {
			n.cfgMu.Unlock()
			return nil, errors.New(dir + " is already attached as folder " + f.Name)
		}
	}
	n.cfgMu.Unlock()
	f, err := call(ctx, p.Name)
	if errors.Is(err, registry.ErrExists) {
		return nil, errors.New("folder " + p.Name + " already exists: use folder join")
	}
	if errors.Is(err, registry.ErrNotFound) {
		return nil, errors.New("folder " + p.Name + " does not exist: use folder add")
	}
	if err != nil {
		return nil, err
	}
	n.cfgMu.Lock()
	n.cfg.SetFolder(config.Folder{Name: f.Name, Path: dir, ID: f.ID})
	err = n.cfg.Save()
	n.cfgMu.Unlock()
	if err != nil {
		return nil, err
	}
	// The registry publishes the change to the watch as well; reconciling
	// now just makes the CLI's next command see it sooner.
	n.mu.Lock()
	if !slices.ContainsFunc(n.member.Folders, func(x api.Folder) bool { return x.Name == f.Name }) {
		n.member.Folders = append(n.member.Folders, f)
	}
	n.mu.Unlock()
	n.reconcile(n.ctx)
	return FolderInfo{Name: f.Name, ID: f.ID, Path: dir, Attached: true}, nil
}

func (n *Node) status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cfgMu.Lock()
	local := slices.Clone(n.cfg.Folders)
	n.cfgMu.Unlock()
	self := n.tr.Key()
	st := Status{
		Name: n.cfg.Name, Key: self, Addr: n.tr.Addr(), Device: n.sy.DeviceID(), Server: api.Addr(n.cfg.Server),
		Registered: n.registered, Watching: n.watching && n.reachable, ServerErr: n.serverErr, ServerSeen: n.serverSeen,
		Version: n.member.Version,
	}
	names := map[api.NodeKey]string{}
	for _, nd := range n.member.Nodes {
		names[nd.Key] = nd.Name
	}
	peers := map[api.NodeKey]bool{}
	for _, f := range n.member.Folders {
		if !slices.Contains(f.Members, self) {
			continue
		}
		fi := FolderInfo{Name: f.Name, ID: f.ID, Keep: f.Keep}
		for _, lf := range local {
			if lf.Name == f.Name {
				fi.Path, fi.Attached = lf.Path, true
			}
		}
		for _, k := range f.Members {
			name := names[k]
			if name == "" {
				name = string(k)
			}
			fi.Members = append(fi.Members, name)
			if k != self && fi.Attached {
				peers[k] = true
			}
		}
		if f.Keep != nil {
			fi.Keeper = slices.Contains(f.Keep.Keepers, self)
		}
		st.Folders = append(st.Folders, fi)
	}
	for _, nd := range n.member.Nodes {
		if peers[nd.Key] {
			st.Peers = append(st.Peers, PeerInfo{Name: nd.Name, Device: nd.Device, Connected: n.connected[nd.Device]})
		}
	}
	return st
}
