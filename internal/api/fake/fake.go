// Package fake provides in-memory implementations of the api interfaces for
// unit tests. They follow the documented semantics closely enough that a
// component tested against them should work against the real thing.
package fake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Clock is a manually advanced clock.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock(t time.Time) *Clock { return &Clock{now: t} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Network connects fake Transports in memory.
type Network struct {
	mu    sync.Mutex
	nodes map[api.Addr]*Transport
}

func NewNetwork() *Network { return &Network{nodes: map[api.Addr]*Transport{}} }

// Transport returns a new endpoint named name on the network.
func (n *Network) Transport(name string) *Transport {
	t := &Transport{
		net:      n,
		addr:     api.Addr("tc-fake-" + name),
		key:      api.NodeKey("nodekey:" + name),
		handlers: map[uint16]func(net.Conn, api.NodeKey){},
		allowed:  map[api.NodeKey]bool{},
	}
	n.mu.Lock()
	n.nodes[t.addr] = t
	n.mu.Unlock()
	return t
}

// Transport is an in-memory api.Transport. Connections are net.Pipe pairs.
type Transport struct {
	net      *Network
	addr     api.Addr
	key      api.NodeKey
	mu       sync.Mutex
	handlers map[uint16]func(net.Conn, api.NodeKey)
	allowed  map[api.NodeKey]bool
	closed   bool
}

var _ api.Transport = (*Transport)(nil)

func (t *Transport) Addr() api.Addr   { return t.addr }
func (t *Transport) Key() api.NodeKey { return t.key }

func (t *Transport) Handle(port uint16, h func(net.Conn, api.NodeKey)) {
	t.mu.Lock()
	t.handlers[port] = h
	t.mu.Unlock()
}

func (t *Transport) Allow(keys ...api.NodeKey) {
	t.mu.Lock()
	for _, k := range keys {
		t.allowed[k] = true
	}
	t.mu.Unlock()
}

func (t *Transport) Disallow(keys ...api.NodeKey) {
	t.mu.Lock()
	for _, k := range keys {
		delete(t.allowed, k)
	}
	t.mu.Unlock()
}

// ErrRefused is returned when the remote does not allow the caller's key,
// has no handler on the port, or is closed.
var ErrRefused = errors.New("fake: connection refused")

func (t *Transport) Dial(ctx context.Context, to api.Addr, port uint16) (net.Conn, error) {
	t.net.mu.Lock()
	remote := t.net.nodes[to]
	t.net.mu.Unlock()
	if remote == nil {
		return nil, ErrRefused
	}
	remote.mu.Lock()
	h := remote.handlers[port]
	ok := remote.allowed[t.key] && !remote.closed && h != nil
	remote.mu.Unlock()
	if !ok {
		return nil, ErrRefused
	}
	local, far := net.Pipe()
	go h(far, t.key)
	return local, nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

// Perkeep is an in-memory Signer, Durability, and BlobPutter sharing one
// store. Blobs become durable only when MarkDurable is called, standing in
// for a ZeroFS flush.
type Perkeep struct {
	mu      sync.Mutex
	blobs   map[api.BlobRef]bool // ref -> durable
	trees   map[api.BlobRef]map[string]api.PathEntry
	history map[api.BlobRef][]historyEntry
	nextPN  int
	Claims  int // number of claims written, for assertions
}

func NewPerkeep() *Perkeep {
	return &Perkeep{blobs: map[api.BlobRef]bool{}, trees: map[api.BlobRef]map[string]api.PathEntry{}}
}

var (
	_ api.Signer     = (*Perkeep)(nil)
	_ api.Durability = (*Perkeep)(nil)
	_ api.BlobPutter = (*Perkeep)(nil)
)

func (p *Perkeep) PutFile(ctx context.Context, path string) (api.BlobRef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum224(b)
	ref := api.BlobRef("sha224-" + hex.EncodeToString(sum[:]))
	p.mu.Lock()
	if _, ok := p.blobs[ref]; !ok {
		p.blobs[ref] = false
	}
	p.mu.Unlock()
	return ref, nil
}

func (p *Perkeep) NewPermanode(ctx context.Context, title string) (api.BlobRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextPN++
	ref := api.BlobRef(fmt.Sprintf("sha224-permanode%04d", p.nextPN))
	p.trees[ref] = map[string]api.PathEntry{}
	p.blobs[ref] = false
	return ref, nil
}

func (p *Perkeep) ClaimPath(ctx context.Context, c api.PathClaim) (api.ClaimResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tree, ok := p.trees[c.Permanode]
	if !ok {
		return 0, fmt.Errorf("fake: unknown permanode %s", c.Permanode)
	}
	if cur, ok := tree[c.Path]; ok {
		if cur.File == c.File {
			if cur.Gone == api.GoneNone {
				return api.ClaimUnchanged, nil
			}
			// Back in sync with the same content: clear the mark, and
			// take the newer version if this claim supersedes.
			p.Claims++
			now := time.Now()
			if api.Supersedes(c, cur) {
				cur.ModTime, cur.Version = c.ModTime, c.Version
				if c.Mode != 0 {
					cur.Mode = c.Mode
				}
			}
			cur.Gone, cur.GoneAt = api.GoneNone, time.Time{}
			tree[c.Path] = cur
			p.record(c.Permanode, c.Path, cur, now)
			return api.ClaimWritten, nil
		}
		if !api.Supersedes(c, cur) {
			return api.ClaimRejectedOlder, nil
		}
	}
	p.Claims++
	claim := api.BlobRef(fmt.Sprintf("sha224-claim%04d", p.Claims))
	p.blobs[claim] = false
	now := time.Now()
	tree[c.Path] = api.PathEntry{File: c.File, ModTime: c.ModTime, Claims: []api.BlobRef{claim}, Version: c.Version, Mode: c.Mode, KeptAt: now}
	p.record(c.Permanode, c.Path, tree[c.Path], now)
	return api.ClaimWritten, nil
}

func (p *Perkeep) record(pn api.BlobRef, path string, e api.PathEntry, at time.Time) {
	if p.history == nil {
		p.history = map[api.BlobRef][]historyEntry{}
	}
	p.history[pn] = append(p.history[pn], historyEntry{at: at, path: path, entry: e})
}

func (p *Perkeep) MarkGone(ctx context.Context, g api.GoneClaim) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tree, ok := p.trees[g.Permanode]
	if !ok {
		return fmt.Errorf("fake: unknown permanode %s", g.Permanode)
	}
	cur, ok := tree[g.Path]
	if !ok || cur.File != g.File || cur.Gone == g.Reason {
		return nil
	}
	p.Claims++
	now := time.Now()
	cur.Gone, cur.GoneAt = g.Reason, now
	tree[g.Path] = cur
	p.record(g.Permanode, g.Path, cur, now)
	return nil
}

func (p *Perkeep) History(ctx context.Context, permanode api.BlobRef, path string) ([]api.PathEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.trees[permanode]; !ok {
		return nil, fmt.Errorf("fake: unknown permanode %s", permanode)
	}
	var out []api.PathEntry
	for _, h := range p.history[permanode] {
		if h.path != path || h.entry.Gone != api.GoneNone {
			continue
		}
		if n := len(out); n > 0 && out[n-1].File == h.entry.File {
			continue
		}
		out = append(out, h.entry)
	}
	return out, nil
}

func (p *Perkeep) Paths(ctx context.Context, permanode api.BlobRef) (map[string]api.PathEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tree, ok := p.trees[permanode]
	if !ok {
		return nil, fmt.Errorf("fake: unknown permanode %s", permanode)
	}
	out := make(map[string]api.PathEntry, len(tree))
	for k, v := range tree {
		out[k] = v
	}
	return out, nil
}

type historyEntry struct {
	at    time.Time
	path  string
	entry api.PathEntry
}

func (p *Perkeep) PathsAt(ctx context.Context, permanode api.BlobRef, at time.Time) (map[string]api.PathEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.trees[permanode]; !ok {
		return nil, fmt.Errorf("fake: unknown permanode %s", permanode)
	}
	out := map[string]api.PathEntry{}
	for _, h := range p.history[permanode] {
		if !h.at.After(at) {
			out[h.path] = h.entry
		}
	}
	return out, nil
}

func (p *Perkeep) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]bool, len(refs))
	for i, r := range refs {
		out[i] = p.blobs[r]
	}
	return out, nil
}

// MarkDurable marks every blob stored so far as durable.
func (p *Perkeep) MarkDurable() {
	p.mu.Lock()
	for r := range p.blobs {
		p.blobs[r] = true
	}
	p.mu.Unlock()
}

// Registry is an in-memory registry. Each node talks to it through Client.
type Registry struct {
	mu       sync.Mutex
	pk       *Perkeep
	m        api.Membership
	watchers []chan api.Membership
}

func NewRegistry(pk *Perkeep) *Registry { return &Registry{pk: pk} }

// Client returns an api.RegistryClient acting as the node with key self.
func (r *Registry) Client(self api.NodeKey) api.RegistryClient {
	return &registryClient{r: r, self: self}
}

// Membership returns a copy of the current membership.
func (r *Registry) Membership() api.Membership {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyMembership(r.m)
}

func (r *Registry) publishLocked() {
	r.m.Version++
	m := copyMembership(r.m)
	for _, w := range r.watchers {
		select {
		case w <- m:
		default:
		}
	}
}

func copyMembership(m api.Membership) api.Membership {
	out := api.Membership{Epoch: m.Epoch, Version: m.Version}
	out.Nodes = append(out.Nodes, m.Nodes...)
	for _, f := range m.Folders {
		f.Members = append([]api.NodeKey(nil), f.Members...)
		f.ReceiveOnly = append([]api.NodeKey(nil), f.ReceiveOnly...)
		if f.Keep != nil {
			k := *f.Keep
			k.Keepers = append([]api.NodeKey(nil), k.Keepers...)
			f.Keep = &k
		}
		out.Folders = append(out.Folders, f)
	}
	return out
}

type registryClient struct {
	r    *Registry
	self api.NodeKey
}

var _ api.RegistryClient = (*registryClient)(nil)

func (c *registryClient) Register(ctx context.Context, n api.Node) (api.Membership, error) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	n.Key = c.self
	replaced := false
	for i := range c.r.m.Nodes {
		if c.r.m.Nodes[i].Key == n.Key {
			c.r.m.Nodes[i] = n
			replaced = true
		}
	}
	if !replaced {
		c.r.m.Nodes = append(c.r.m.Nodes, n)
	}
	c.r.publishLocked()
	return copyMembership(c.r.m), nil
}

func (c *registryClient) Watch(ctx context.Context) (<-chan api.Membership, error) {
	ch := make(chan api.Membership, 16)
	c.r.mu.Lock()
	ch <- copyMembership(c.r.m)
	c.r.watchers = append(c.r.watchers, ch)
	c.r.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.r.mu.Lock()
		for i, w := range c.r.watchers {
			if w == ch {
				c.r.watchers = append(c.r.watchers[:i], c.r.watchers[i+1:]...)
				break
			}
		}
		c.r.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (c *registryClient) folderLocked(name string) *api.Folder {
	for i := range c.r.m.Folders {
		if c.r.m.Folders[i].Name == name {
			return &c.r.m.Folders[i]
		}
	}
	return nil
}

func (c *registryClient) AddFolder(ctx context.Context, name string) (api.Folder, error) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	if c.folderLocked(name) != nil {
		return api.Folder{}, fmt.Errorf("fake: folder %q exists", name)
	}
	c.r.m.Folders = append(c.r.m.Folders, api.Folder{Name: name, ID: "fake-" + name, Members: []api.NodeKey{c.self}})
	c.r.publishLocked()
	return copyMembership(c.r.m).Folders[len(c.r.m.Folders)-1], nil
}

func (c *registryClient) JoinFolder(ctx context.Context, name string) (api.Folder, error) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	f := c.folderLocked(name)
	if f == nil {
		return api.Folder{}, fmt.Errorf("fake: no folder %q", name)
	}
	for _, m := range f.Members {
		if m == c.self {
			return *f, nil
		}
	}
	f.Members = append(f.Members, c.self)
	c.r.publishLocked()
	return *c.folderLocked(name), nil
}

func (c *registryClient) Unkeep(ctx context.Context, name string) error {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	f := c.folderLocked(name)
	if f == nil {
		return fmt.Errorf("fake: no folder %q", name)
	}
	if !contains(f.Members, c.self) {
		return fmt.Errorf("fake: %s not in %q", c.self, name)
	}
	if f.Keep == nil {
		return nil
	}
	f.Archive = f.Keep.Permanode
	f.Keep = nil
	c.r.publishLocked()
	return nil
}

func (c *registryClient) LeaveFolder(ctx context.Context, name string) error {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	f := c.folderLocked(name)
	if f == nil {
		return fmt.Errorf("fake: no folder %q", name)
	}
	f.Members = without(f.Members, c.self)
	f.ReceiveOnly = without(f.ReceiveOnly, c.self)
	if f.Keep != nil {
		f.Keep.Keepers = without(f.Keep.Keepers, c.self)
		for _, m := range f.Members {
			if len(f.Keep.Keepers) >= 2 {
				break
			}
			if !contains(f.Keep.Keepers, m) {
				f.Keep.Keepers = append(f.Keep.Keepers, m)
			}
		}
	}
	c.r.publishLocked()
	return nil
}

func without(ks []api.NodeKey, k api.NodeKey) []api.NodeKey {
	out := ks[:0:0]
	for _, x := range ks {
		if x != k {
			out = append(out, x)
		}
	}
	return out
}

func contains(ks []api.NodeKey, k api.NodeKey) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

func (c *registryClient) Keep(ctx context.Context, folder string, p api.KeepPolicy) (api.BlobRef, error) {
	c.r.mu.Lock()
	f := c.folderLocked(folder)
	if f == nil {
		c.r.mu.Unlock()
		return "", fmt.Errorf("fake: no folder %q", folder)
	}
	if f.Archive != "" {
		p.Permanode = f.Archive
	}
	if f.Keep != nil {
		p.Permanode = f.Keep.Permanode
	}
	c.r.mu.Unlock()
	if p.Permanode == "" {
		pn, err := c.r.pk.NewPermanode(ctx, folder)
		if err != nil {
			return "", err
		}
		p.Permanode = pn
	}
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	f = c.folderLocked(folder)
	if len(p.Keepers) == 0 {
		n := len(f.Members)
		if n > 2 {
			n = 2
		}
		p.Keepers = append([]api.NodeKey(nil), f.Members[:n]...)
	}
	f.Keep = &p
	f.Archive = p.Permanode
	c.r.publishLocked()
	return p.Permanode, nil
}

func (c *registryClient) ClaimPath(ctx context.Context, cl api.PathClaim) (api.ClaimResult, error) {
	return c.r.pk.ClaimPath(ctx, cl)
}

func (c *registryClient) Paths(ctx context.Context, pn api.BlobRef) (map[string]api.PathEntry, error) {
	return c.r.pk.Paths(ctx, pn)
}

func (c *registryClient) PathsAt(ctx context.Context, pn api.BlobRef, at time.Time) (map[string]api.PathEntry, error) {
	return c.r.pk.PathsAt(ctx, pn, at)
}

func (c *registryClient) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	return c.r.pk.Durable(ctx, refs)
}

func (c *registryClient) MarkGone(ctx context.Context, g api.GoneClaim) error {
	return c.r.pk.MarkGone(ctx, g)
}

func (c *registryClient) History(ctx context.Context, pn api.BlobRef, path string) ([]api.PathEntry, error) {
	return c.r.pk.History(ctx, pn, path)
}

// ZeroFS is a fake api.ZeroFS backed by a local directory. OnFlush, if set,
// runs on every successful Flush (wire it to Perkeep.MarkDurable).
type ZeroFS struct {
	DirPath string
	OnFlush func()
	mu      sync.Mutex
	running bool
	Flushes int
}

var _ api.ZeroFS = (*ZeroFS)(nil)

func (z *ZeroFS) Start(ctx context.Context) error {
	if err := os.MkdirAll(z.DirPath, 0o700); err != nil {
		return err
	}
	z.mu.Lock()
	z.running = true
	z.mu.Unlock()
	return nil
}

func (z *ZeroFS) Dir() string { return z.DirPath }

func (z *ZeroFS) Flush(ctx context.Context) error {
	z.mu.Lock()
	running := z.running
	if running {
		z.Flushes++
	}
	z.mu.Unlock()
	if !running {
		return errors.New("fake: zerofs not running")
	}
	if z.OnFlush != nil {
		z.OnFlush()
	}
	return nil
}

func (z *ZeroFS) Stop(ctx context.Context) error {
	z.mu.Lock()
	z.running = false
	z.mu.Unlock()
	return nil
}

// Syncer is a fake api.Syncer that records configuration and lets tests
// inject events with Emit.
type Syncer struct {
	ID      api.DeviceID
	mu      sync.Mutex
	Peers   []api.Peer
	Folders []api.FolderConfig
	Scans   []string // "folderID/subpath"
	events  chan api.SyncEvent
	modBy   map[string]api.DeviceID
	states  map[string]api.FileState
}

func NewSyncer(id api.DeviceID) *Syncer {
	return &Syncer{ID: id, events: make(chan api.SyncEvent, 64)}
}

var _ api.Syncer = (*Syncer)(nil)

func (s *Syncer) Start(ctx context.Context) error { return nil }
func (s *Syncer) DeviceID() api.DeviceID          { return s.ID }

func (s *Syncer) SetPeers(ctx context.Context, peers []api.Peer) error {
	s.mu.Lock()
	s.Peers = append([]api.Peer(nil), peers...)
	s.mu.Unlock()
	return nil
}

func (s *Syncer) SetFolders(ctx context.Context, folders []api.FolderConfig) error {
	s.mu.Lock()
	s.Folders = append([]api.FolderConfig(nil), folders...)
	s.mu.Unlock()
	return nil
}

func (s *Syncer) Scan(ctx context.Context, folderID, subpath string) error {
	s.mu.Lock()
	s.Scans = append(s.Scans, folderID+"/"+subpath)
	s.mu.Unlock()
	return nil
}

func (s *Syncer) Events() <-chan api.SyncEvent { return s.events }

// SetFileState sets what FileState reports for folderID/path. Paths never
// set are reported as known and current, with no version.
func (s *Syncer) SetFileState(folderID, path string, st api.FileState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = map[string]api.FileState{}
	}
	s.states[folderID+"/"+path] = st
}

func (s *Syncer) FileState(folderID, path string) (api.FileState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.states[folderID+"/"+path]; ok {
		return st, nil
	}
	return api.FileState{Known: true, Current: true}, nil
}

// SetModifiedBy sets what ModifiedBy reports for folderID/path.
func (s *Syncer) SetModifiedBy(folderID, path string, d api.DeviceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.modBy == nil {
		s.modBy = map[string]api.DeviceID{}
	}
	s.modBy[folderID+"/"+path] = d
}

func (s *Syncer) ModifiedBy(folderID, path string) (api.DeviceID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.modBy[folderID+"/"+path]
	return d, ok
}

// Emit delivers an event to Events.
func (s *Syncer) Emit(ev api.SyncEvent) { s.events <- ev }

func (s *Syncer) Stop(ctx context.Context) error { return nil }
