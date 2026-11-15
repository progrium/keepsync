// Package node wires a keepsync node together: the tailcat transport, the
// embedded Syncthing, the registry client that drives peers and folders,
// the keepers for kept folders, and the control socket that `keepsync
// status`, `folder …` and `keep …` talk to.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/keeper"
	"github.com/progrium/keepsync/internal/pkserver"
	"github.com/progrium/keepsync/internal/syncer"
	"github.com/progrium/keepsync/internal/tcnet"
)

// InitOptions configures Init.
type InitOptions struct {
	Dir     string
	Name    string // default: the hostname
	Server  string // "tc…" address, or a DNS name with a TXT record
	Network config.Network
}

// InitResult is what `node init` prints.
type InitResult struct {
	Config *config.Node
	Key    api.NodeKey
	Device api.DeviceID
}

// Init writes the node's config, its tailcat key and its Syncthing device
// certificate. It works offline, except for resolving a DNS server name.
func Init(ctx context.Context, o InitOptions) (*InitResult, error) {
	if _, err := config.LoadNode(o.Dir); err == nil {
		return nil, fmt.Errorf("node: %s is already initialized", o.Dir)
	}
	addr, err := ResolveServer(ctx, o.Server)
	if err != nil {
		return nil, err
	}
	name := o.Name
	if name == "" {
		if name, err = os.Hostname(); err != nil || name == "" {
			name = "node"
		}
		name, _, _ = strings.Cut(name, ".")
	}
	cfg := &config.Node{Dir: o.Dir, Name: name, Server: string(addr), Network: o.Network}
	if err := os.MkdirAll(o.Dir, 0o700); err != nil {
		return nil, err
	}

	pk, err := tcnet.LoadKey(cfg.KeyPath())
	if err != nil {
		pk = tcnet.NewKey()
		if err := tcnet.SaveKey(cfg.KeyPath(), pk); err != nil {
			return nil, err
		}
	}
	// syncer.New only creates the certificate here; nothing runs, so it
	// gets a transport that never connects.
	sy, err := syncer.New(syncer.Config{Dir: cfg.SyncthingDir(), Name: name, Transport: offline{}})
	if err != nil {
		return nil, fmt.Errorf("node: syncthing identity: %w", err)
	}
	if err := cfg.Save(); err != nil {
		return nil, err
	}
	return &InitResult{Config: cfg, Key: api.NodeKey(pk.Private.Public().String()), Device: sy.DeviceID()}, nil
}

// ResolveServer turns `--server` into a tailcat address. A "tc…" value is
// used as is; anything else is a DNS name whose TXT record holds the
// address (either bare or as "keepsync=tc…").
func ResolveServer(ctx context.Context, s string) (api.Addr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("node: --server is required")
	}
	if strings.HasPrefix(s, "tc") && !strings.Contains(s, ".") {
		return api.Addr(s), nil
	}
	txts, err := net.DefaultResolver.LookupTXT(ctx, s)
	if err != nil {
		return "", fmt.Errorf("node: resolve %s: %w", s, err)
	}
	for _, t := range txts {
		t = strings.TrimPrefix(strings.TrimSpace(t), "keepsync=")
		if strings.HasPrefix(t, "tc") {
			return api.Addr(t), nil
		}
	}
	return "", fmt.Errorf("node: %s has no TXT record with a keepsync server address", s)
}

// Node is a running keepsync node.
type Node struct {
	cfg  *config.Node
	logf func(format string, args ...any)

	tr     *tcnet.Transport
	reg    *liveClient
	putter *pkserver.Putter
	sy     *syncer.Syncer
	ctl    *ctl.Server
	up     atomic.Bool

	mu         sync.Mutex
	member     api.Membership
	haveMember bool
	registered bool
	watching   bool
	serverErr  string
	serverSeen time.Time
	connected  map[api.DeviceID]bool
	keepers    map[string]*kept // by folder name
	ctx        context.Context
	cfgMu      sync.Mutex // serializes config edits and saves
	recMu      sync.Mutex // serializes reconcile
}

type kept struct {
	k      *keeper.Keeper
	id     string
	dir    string
	cancel context.CancelFunc
	last   keeper.Report
	lastAt time.Time
	lastE  string
}

// Options configures Run.
type Options struct {
	Logf  func(format string, args ...any)
	Ready func(*Node)
}

// Run starts the node and blocks until ctx is done.
func Run(ctx context.Context, cfg *config.Node, o Options) (err error) {
	n := &Node{cfg: cfg, logf: o.Logf, connected: map[api.DeviceID]bool{}, keepers: map[string]*kept{}}
	if n.logf == nil {
		n.logf = log.Printf
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n.ctx = ctx
	defer n.shutdown()

	n.ctl = ctl.NewServer()
	n.handlers()
	if err := n.ctl.Listen(cfg.SocketPath()); err != nil {
		n.ctl = nil
		return err
	}
	topts, err := cfg.Network.TransportOptions(cfg.KeyPath())
	if err != nil {
		return err
	}
	if n.tr, err = tcnet.New(ctx, topts); err != nil {
		return err
	}
	server := api.Addr(cfg.Server)
	n.reg = newLiveClient(n.tr, server)
	if n.putter, err = pkserver.NewPutter(n.tr, server); err != nil {
		return err
	}
	if n.sy, err = syncer.New(syncer.Config{Dir: cfg.SyncthingDir(), Name: cfg.Name, Transport: n.tr}); err != nil {
		return err
	}
	if err := n.sy.Start(ctx); err != nil {
		return fmt.Errorf("node: syncthing: %w", err)
	}
	go n.events()

	// Sync with the last known peers until the server answers.
	if m, ok := n.loadMembership(); ok {
		n.apply(ctx, m)
	}
	n.up.Store(true)
	n.logf("node %s (%s) up at %s, device %s", cfg.Name, n.tr.Key(), n.tr.Addr(), n.sy.DeviceID())
	if o.Ready != nil {
		o.Ready(n)
	}
	go n.probe(ctx)
	n.watch(ctx)
	return nil
}

// probeInterval and probeTimeout set how quickly a node notices that the
// server is gone (see liveClient).
const (
	probeInterval = 20 * time.Second
	probeTimeout  = 15 * time.Second
)

// probe checks the server is still answering while the membership stream
// is up, and resets the registry client when it is not.
func (n *Node) probe(ctx context.Context) {
	t := time.NewTicker(probeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.mu.Lock()
		watching := n.watching
		n.mu.Unlock()
		if !watching {
			continue
		}
		if err := n.reg.probe(ctx, probeTimeout); err != nil && ctx.Err() == nil {
			n.logf("server not answering (%v); reconnecting", err)
			n.mu.Lock()
			n.watching, n.serverErr = false, err.Error()
			n.mu.Unlock()
		} else if err == nil {
			n.mu.Lock()
			n.serverSeen = time.Now()
			n.mu.Unlock()
		}
	}
}

func (n *Node) shutdown() {
	if n.ctl != nil {
		n.ctl.Close()
	}
	n.mu.Lock()
	for name, k := range n.keepers {
		k.cancel()
		delete(n.keepers, name)
	}
	n.mu.Unlock()
	if n.reg != nil {
		n.reg.Close()
	}
	if n.sy != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		n.sy.Stop(ctx)
		cancel()
	}
	if n.tr != nil {
		n.tr.Close()
	}
}

// watch registers with the server and follows the membership, reconnecting
// with backoff whenever the server is unreachable, until ctx is done.
func (n *Node) watch(ctx context.Context) {
	backoff := time.Second
	fail := func(err error) {
		n.mu.Lock()
		n.serverErr = err.Error()
		n.watching = false
		n.mu.Unlock()
		n.logf("server: %v (retrying in %v)", err, backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
	for ctx.Err() == nil {
		m, err := n.reg.Register(ctx, api.Node{Name: n.cfg.Name, Key: n.tr.Key(), Addr: n.tr.Addr(), Device: n.sy.DeviceID()})
		if err != nil {
			fail(err)
			continue
		}
		n.mu.Lock()
		n.registered, n.serverErr, n.serverSeen = true, "", time.Now()
		n.mu.Unlock()
		n.apply(ctx, m)
		ch, err := n.reg.Watch(ctx)
		if err != nil {
			fail(err)
			continue
		}
		n.mu.Lock()
		n.watching = true
		n.mu.Unlock()
		backoff = time.Second
		for m := range ch {
			n.mu.Lock()
			n.serverSeen = time.Now()
			n.mu.Unlock()
			n.apply(ctx, m)
		}
		if ctx.Err() == nil {
			fail(errors.New("membership stream closed"))
		}
	}
}

func (n *Node) events() {
	for ev := range n.sy.Events() {
		switch ev.Type {
		case api.SyncPeerConnected, api.SyncPeerDisconnected:
			n.mu.Lock()
			n.connected[ev.Device] = ev.Type == api.SyncPeerConnected
			n.mu.Unlock()
			n.logf("peer %s %s", short(ev.Device), map[bool]string{true: "connected", false: "disconnected"}[ev.Type == api.SyncPeerConnected])
		case api.SyncConflict:
			n.logf("conflict in %s: %s", ev.Folder, ev.Path)
		}
	}
}

// apply makes Syncthing and the keepers match the membership and the
// folders attached here. It is idempotent.
func (n *Node) apply(ctx context.Context, m api.Membership) {
	n.mu.Lock()
	if n.haveMember && m.Version < n.member.Version {
		n.mu.Unlock()
		return
	}
	n.member, n.haveMember = m, true
	n.mu.Unlock()
	n.saveMembership(m)
	n.reconcile(ctx)
}

// reconcile applies the current membership and config.
func (n *Node) reconcile(ctx context.Context) {
	n.recMu.Lock()
	defer n.recMu.Unlock()
	n.mu.Lock()
	m := n.member
	n.mu.Unlock()
	n.cfgMu.Lock()
	local := slices.Clone(n.cfg.Folders)
	n.cfgMu.Unlock()

	self := n.tr.Key()
	nodes := map[api.NodeKey]api.Node{}
	for _, nd := range m.Nodes {
		nodes[nd.Key] = nd
	}
	byName := map[string]api.Folder{}
	for _, f := range m.Folders {
		byName[f.Name] = f
	}

	peerSet := map[api.NodeKey]bool{}
	var folders []api.FolderConfig
	wantKeep := map[string]api.Folder{}
	dirs := map[string]config.Folder{}
	for _, lf := range local {
		f, ok := byName[lf.Name]
		if !ok || !slices.Contains(f.Members, self) {
			continue
		}
		fc := api.FolderConfig{ID: f.ID, Path: lf.Path, ReceiveOnly: slices.Contains(f.ReceiveOnly, self)}
		for _, k := range f.Members {
			nd, ok := nodes[k]
			if k == self || !ok || nd.Device == "" {
				continue
			}
			peerSet[k] = true
			fc.Devices = append(fc.Devices, nd.Device)
		}
		folders = append(folders, fc)
		if f.Keep != nil {
			wantKeep[f.Name] = f
			dirs[f.Name] = lf
		}
	}
	var peers []api.Peer
	for k := range peerSet {
		nd := nodes[k]
		peers = append(peers, api.Peer{Name: nd.Name, Key: nd.Key, Addr: nd.Addr, Device: nd.Device})
	}
	slices.SortFunc(peers, func(a, b api.Peer) int { return strings.Compare(string(a.Key), string(b.Key)) })
	if err := n.sy.SetPeers(ctx, peers); err != nil {
		n.logf("set peers: %v", err)
	}
	if err := n.sy.SetFolders(ctx, folders); err != nil {
		n.logf("set folders: %v", err)
	}
	n.reconcileKeepers(wantKeep, dirs)
}

func (n *Node) reconcileKeepers(want map[string]api.Folder, dirs map[string]config.Folder) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for name, k := range n.keepers {
		f, ok := want[name]
		if !ok || f.ID != k.id || dirs[name].Path != k.dir {
			k.cancel()
			delete(n.keepers, name)
		}
	}
	for name, f := range want {
		if k, ok := n.keepers[name]; ok {
			if err := k.k.SetPolicy(*f.Keep); err != nil {
				n.logf("keep %s: %v", name, err)
			}
			continue
		}
		lf := dirs[name]
		kp, err := keeper.New(keeper.Config{
			Folder:   name,
			FolderID: f.ID,
			Dir:      lf.Path,
			Self:     n.tr.Key(),
			Policy:   *f.Keep,
			Registry: n.reg,
			Putter:   n.putter,
			Scanner:  n.sy,
			Logf:     n.logf, // the keeper prefixes its own lines

		})
		if err != nil {
			n.logf("keep %s: %v", name, err)
			continue
		}
		kctx, cancel := context.WithCancel(n.ctx)
		k := &kept{k: kp, id: f.ID, dir: lf.Path, cancel: cancel}
		n.keepers[name] = k
		go n.runKeeper(kctx, name, k)
	}
}

// runKeeper makes a pass right away and then every interval, recording the
// last report for `keep status`.
func (n *Node) runKeeper(ctx context.Context, name string, k *kept) {
	interval := time.Duration(n.cfg.KeepInterval)
	if interval <= 0 {
		interval = keeper.DefaultInterval
	}
	for {
		rep, err := k.k.Pass(ctx)
		if ctx.Err() != nil {
			return
		}
		n.mu.Lock()
		k.last, k.lastAt, k.lastE = rep, time.Now(), ""
		if err != nil {
			k.lastE = err.Error()
		}
		n.mu.Unlock()
		if err != nil {
			n.logf("keep %s: %v", name, err)
		} else if len(rep.Kept)+len(rep.Expired)+len(rep.Errors) > 0 {
			n.logf("keep %s: kept %d, expired %d, errors %d", name, len(rep.Kept), len(rep.Expired), len(rep.Errors))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (n *Node) loadMembership() (api.Membership, bool) {
	var m api.Membership
	b, err := os.ReadFile(n.cfg.MembershipPath())
	if err != nil || json.Unmarshal(b, &m) != nil {
		return m, false
	}
	return m, true
}

func (n *Node) saveMembership(m api.Membership) {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return
	}
	p := n.cfg.MembershipPath()
	tmp := p + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		os.Rename(tmp, p)
	}
}

// localFolder returns the attached folder, or an error naming the fix.
func (n *Node) localFolder(name string) (config.Folder, error) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	f, ok := n.cfg.Folder(name)
	if !ok {
		return f, fmt.Errorf("folder %q is not attached here (folder add or folder join first)", name)
	}
	return f, nil
}

func absDir(p string) (string, error) {
	if p == "" {
		return "", errors.New("a path is required")
	}
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, p[2:])
	}
	p, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return p, os.MkdirAll(p, 0o755)
}

func short(d api.DeviceID) string {
	if len(d) > 7 {
		return string(d[:7])
	}
	return string(d)
}

// offline is an api.Transport that never connects, for creating the
// Syncthing identity at init time without starting tailcat.
type offline struct{}

func (offline) Addr() api.Addr                             { return "" }
func (offline) Key() api.NodeKey                           { return "" }
func (offline) Handle(uint16, func(net.Conn, api.NodeKey)) {}
func (offline) Allow(...api.NodeKey)                       {}
func (offline) Disallow(...api.NodeKey)                    {}
func (offline) Close() error                               { return nil }
func (offline) Dial(context.Context, api.Addr, uint16) (net.Conn, error) {
	return nil, errors.New("offline")
}
