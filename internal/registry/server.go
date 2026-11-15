package registry

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Config configures a Server.
type Config struct {
	Store      Store          // required
	Signer     api.Signer     // required
	Durability api.Durability // required
	// Transport is the server's tailcat endpoint. The server keeps its
	// allow list equal to the approved nodes, and serves on
	// api.PortRegistry once Serve is called.
	Transport api.Transport
	Clock     api.Clock // default api.SystemClock
	// KeeperTimeout is how long a keeper may be disconnected before the
	// server hands its folders to another connected member. Default
	// DefaultKeeperTimeout.
	KeeperTimeout time.Duration
	// PingInterval is how often the server pings each connection.
	// Default DefaultPingInterval.
	PingInterval time.Duration
	// IdleTimeout is how long a connection may stay silent (no request
	// and no pong) before the server drops it. Default DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Server is the registry: the membership list, folders, keep policies and
// keeper assignments, plus the gateway through which nodes reach the
// server's Signer and Durability.
type Server struct {
	cfg Config

	mu      sync.Mutex
	state   State
	conns   map[api.NodeKey]int       // open connections per node
	offline map[api.NodeKey]time.Time // when a node's last connection closed
	subs    map[*subscriber]struct{}
	open    map[net.Conn]api.NodeKey
	timers  map[*time.Timer]struct{}
	closed  bool
}

type subscriber struct {
	ch chan api.Membership // buffer 1, always holds the newest
}

// NewServer loads state from cfg.Store and allows every approved node on
// cfg.Transport.
func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Store == nil || cfg.Signer == nil || cfg.Durability == nil {
		return nil, errors.New("registry: Store, Signer and Durability are required")
	}
	if cfg.Clock == nil {
		cfg.Clock = api.SystemClock{}
	}
	if cfg.KeeperTimeout <= 0 {
		cfg.KeeperTimeout = DefaultKeeperTimeout
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = DefaultPingInterval
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	st, err := cfg.Store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry: load state: %w", err)
	}
	s := &Server{
		cfg:     cfg,
		state:   st,
		conns:   map[api.NodeKey]int{},
		offline: map[api.NodeKey]time.Time{},
		subs:    map[*subscriber]struct{}{},
		open:    map[net.Conn]api.NodeKey{},
		timers:  map[*time.Timer]struct{}{},
	}
	now := cfg.Clock.Now()
	for _, n := range st.Nodes {
		// Nobody is connected yet; count the timeout from startup.
		s.offline[n.Key] = now
		if cfg.Transport != nil {
			cfg.Transport.Allow(n.Key)
		}
	}
	return s, nil
}

// Serve registers the registry handler on api.PortRegistry.
func (s *Server) Serve() error {
	if s.cfg.Transport == nil {
		return errors.New("registry: no Transport configured")
	}
	s.cfg.Transport.Handle(api.PortRegistry, s.HandleConn)
	return nil
}

// Close drops every connection and stops background timers. It does not
// close the Transport.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.open {
		c.Close()
	}
	for t := range s.timers {
		t.Stop()
	}
	s.timers = map[*time.Timer]struct{}{}
	s.mu.Unlock()
	return nil
}

// Membership returns the currently published membership.
func (s *Server) Membership() api.Membership {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.membership()
}

// NodeInfo is one approved node, for `server nodes`.
type NodeInfo struct {
	NodeRecord
	Online bool
}

// Nodes lists every approved node, registered or not.
func (s *Server) Nodes() []NodeInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]NodeInfo, 0, len(s.state.Nodes))
	for _, n := range s.state.Nodes {
		out = append(out, NodeInfo{NodeRecord: n, Online: s.conns[n.Key] > 0})
	}
	return out
}

// Add approves a node key (`server add <nodekey> [--name]`). Adding a key
// that is already approved only renames it, if name is set.
func (s *Server) Add(ctx context.Context, key api.NodeKey, name string) error {
	if key == "" {
		return fmt.Errorf("%w: empty node key", ErrBadRequest)
	}
	err := s.mutate(ctx, func(st *State) error {
		if n := st.node(key); n != nil {
			if name == "" || (n.Name == name && n.NamedByAdmin) {
				return errNoChange
			}
			n.Name, n.NamedByAdmin = name, true
			return nil
		}
		st.Nodes = append(st.Nodes, NodeRecord{Node: api.Node{Name: name, Key: key}, NamedByAdmin: name != ""})
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if _, ok := s.offline[key]; !ok && s.conns[key] == 0 {
		s.offline[key] = s.cfg.Clock.Now()
	}
	s.mu.Unlock()
	if s.cfg.Transport != nil {
		s.cfg.Transport.Allow(key)
	}
	return nil
}

// Remove removes a node by name or key (`server remove <name>`): it leaves
// the allow list and every folder, its keeper assignments move to other
// members, and its open connections are closed.
func (s *Server) Remove(ctx context.Context, nameOrKey string) error {
	var key api.NodeKey
	err := s.mutate(ctx, func(st *State) error {
		i, err := st.findNode(nameOrKey)
		if err != nil {
			return err
		}
		key = st.Nodes[i].Key
		st.Nodes = append(st.Nodes[:i], st.Nodes[i+1:]...)
		for j := range st.Folders {
			f := &st.Folders[j]
			f.Members = without(f.Members, key)
			f.ReceiveOnly = without(f.ReceiveOnly, key)
			if f.Keep != nil {
				f.Keep.Keepers = without(f.Keep.Keepers, key)
			}
		}
		s.assignKeepersLocked(st)
		return nil
	})
	if err != nil {
		return err
	}
	if s.cfg.Transport != nil {
		s.cfg.Transport.Disallow(key)
	}
	s.mu.Lock()
	delete(s.offline, key)
	for c, k := range s.open {
		if k == key {
			c.Close()
		}
	}
	s.mu.Unlock()
	return nil
}

// Perkeep gateway scopes (see docs/design.md, "Perkeep gateway"). A
// node's scope decides what pkserver lets it do over api.PortPerkeep.
const (
	// ScopeBlobs allows uploading and fetching blobs, which is what the
	// keeper needs. Every node has it by default.
	ScopeBlobs = "blobs"
	// ScopeRead adds search, the web UI, pk-get and pk-mount: everything
	// that reads the archive. It still cannot sign or delete.
	ScopeRead = "read"
)

// Grant sets the Perkeep gateway scope of a node, by name or key (`server
// grant <name> <scope>`). There is no write scope yet.
func (s *Server) Grant(ctx context.Context, nameOrKey, scope string) error {
	switch scope {
	case ScopeBlobs, ScopeRead:
	case "write":
		return fmt.Errorf("%w: the write scope is not supported yet", ErrBadRequest)
	default:
		return fmt.Errorf("%w: unknown scope %q (want %s or %s)", ErrBadRequest, scope, ScopeBlobs, ScopeRead)
	}
	if scope == ScopeBlobs {
		scope = "" // the default
	}
	return s.mutate(ctx, func(st *State) error {
		i, err := st.findNode(nameOrKey)
		if err != nil {
			return err
		}
		if st.Nodes[i].Scope == scope {
			return errNoChange
		}
		st.Nodes[i].Scope = scope
		return nil
	})
}

// Scope returns the Perkeep gateway scope of an approved node, or "" if
// key is not approved. pkserver calls it for every request a node makes.
func (s *Server) Scope(key api.NodeKey) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.state.node(key)
	if n == nil {
		return ""
	}
	if n.Scope == "" {
		return ScopeBlobs
	}
	return n.Scope
}

// SetReceiveOnly marks a member of a folder receive-only, or clears it.
func (s *Server) SetReceiveOnly(ctx context.Context, folder string, key api.NodeKey, on bool) error {
	return s.mutate(ctx, func(st *State) error {
		f := st.folder(folder)
		if f == nil {
			return fmt.Errorf("%w: folder %q", ErrNotFound, folder)
		}
		if !contains(f.Members, key) {
			return fmt.Errorf("%w: %s in %q", ErrNotMember, key, folder)
		}
		if contains(f.ReceiveOnly, key) == on {
			return errNoChange
		}
		if on {
			f.ReceiveOnly = append(f.ReceiveOnly, key)
		} else {
			f.ReceiveOnly = without(f.ReceiveOnly, key)
		}
		s.assignKeepersLocked(st)
		return nil
	})
}

// Reconcile reassigns keepers now instead of waiting for a timer. It runs
// on its own whenever a node connects, and KeeperTimeout after a node's
// last connection closes.
func (s *Server) Reconcile(ctx context.Context) error {
	return s.mutate(ctx, func(st *State) error {
		if !s.assignKeepersLocked(st) {
			return errNoChange
		}
		return nil
	})
}

// errNoChange from a mutate func means: nothing to save or publish.
var errNoChange = errors.New("no change")

// mutate applies fn to a copy of the state and, if fn changed it, saves the
// copy, swaps it in and publishes it. fn runs with s.mu held.
func (s *Server) mutate(ctx context.Context, fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	next := s.state.clone()
	if err := fn(&next); err != nil {
		if errors.Is(err, errNoChange) {
			return nil
		}
		return err
	}
	next.Version = s.state.Version + 1
	if err := s.cfg.Store.Save(ctx, next); err != nil {
		return fmt.Errorf("registry: save state: %w", err)
	}
	s.state = next
	m := next.membership()
	for sub := range s.subs {
		select {
		case <-sub.ch:
		default:
		}
		sub.ch <- m
	}
	return nil
}

// assignKeepersLocked brings every kept folder up to two keepers, chosen
// from its members, and replaces keepers that have been disconnected for
// KeeperTimeout when a connected member can take over. Existing keepers keep
// their order, so Keepers[0] (the conflict hook runner) stays put while it
// is healthy. Send-receive members are preferred over receive-only ones.
// It reports whether anything changed.
func (s *Server) assignKeepersLocked(st *State) bool {
	now := s.cfg.Clock.Now()
	online := func(k api.NodeKey) bool { return s.conns[k] > 0 }
	stale := func(k api.NodeKey) bool {
		if online(k) {
			return false
		}
		t, ok := s.offline[k]
		return !ok || now.Sub(t) >= s.cfg.KeeperTimeout
	}
	changed := false
	for i := range st.Folders {
		f := &st.Folders[i]
		if f.Keep == nil {
			continue
		}
		// Candidates: connected members not already keeping, send-receive first.
		var cands []api.NodeKey
		for _, pass := range []bool{false, true} {
			for _, m := range f.Members {
				if online(m) && contains(f.ReceiveOnly, m) == pass && !contains(f.Keep.Keepers, m) {
					cands = append(cands, m)
				}
			}
		}
		// Healthy keepers stay, in order; stale ones are replaced by
		// candidates and only kept while nobody else can take over.
		var keepers, staleKeepers []api.NodeKey
		for _, k := range f.Keep.Keepers {
			switch {
			case !contains(f.Members, k) || contains(keepers, k) || contains(staleKeepers, k):
			case stale(k):
				staleKeepers = append(staleKeepers, k)
			default:
				keepers = append(keepers, k)
			}
		}
		for len(keepers) < 2 && len(cands) > 0 {
			keepers = append(keepers, cands[0])
			cands = cands[1:]
		}
		for len(keepers) < 2 && len(staleKeepers) > 0 {
			keepers = append(keepers, staleKeepers[0])
			staleKeepers = staleKeepers[1:]
		}
		if !equalKeys(keepers, f.Keep.Keepers) {
			changed = true
		}
		f.Keep.Keepers = keepers
	}
	return changed
}

// HandleConn serves one registry connection from peer. Serve registers it
// on the Transport; it is exported for tests and custom listeners.
func (s *Server) HandleConn(conn net.Conn, peer api.NodeKey) {
	s.mu.Lock()
	if s.closed || s.state.node(peer) == nil {
		s.mu.Unlock()
		conn.Close()
		return
	}
	s.open[conn] = peer
	s.conns[peer]++
	delete(s.offline, peer)
	s.mu.Unlock()
	s.Reconcile(context.Background())

	defer s.disconnect(conn, peer)

	var wmu sync.Mutex
	enc := json.NewEncoder(conn)
	send := func(r response) error {
		wmu.Lock()
		defer wmu.Unlock()
		return enc.Encode(r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	done := make(chan struct{})
	defer close(done)
	go watchdog(conn, s.cfg.IdleTimeout, &last, done)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.pinger(ctx, conn, send)
	}()

	dec := json.NewDecoder(conn)
	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			conn.Close()
			return
		}
		last.Store(time.Now().UnixNano())
		if req.Method == mPong {
			continue
		}
		if req.Method == mWatch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.streamWatch(ctx, conn, req.ID, send)
			}()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.call(ctx, peer, req)
			r := response{ID: req.ID, Error: toWireError(err)}
			if err == nil {
				r.Result, err = json.Marshal(res)
				if err != nil {
					r.Error = toWireError(err)
				}
			}
			if send(r) != nil {
				conn.Close()
			}
		}()
	}
}

// pinger sends a ping every PingInterval. A ping stuck on a dead peer
// blocks only until the watchdog closes the connection.
func (s *Server) pinger(ctx context.Context, conn net.Conn, send func(response) error) {
	tick := time.NewTicker(s.cfg.PingInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if send(response{Ping: true}) != nil {
				conn.Close()
				return
			}
		}
	}
}

func (s *Server) disconnect(conn net.Conn, peer api.NodeKey) {
	s.mu.Lock()
	delete(s.open, conn)
	s.conns[peer]--
	last := s.conns[peer] <= 0
	if last {
		delete(s.conns, peer)
		if s.state.node(peer) != nil {
			s.offline[peer] = s.cfg.Clock.Now()
		}
	}
	if last && !s.closed {
		var t *time.Timer
		t = time.AfterFunc(s.cfg.KeeperTimeout, func() {
			s.mu.Lock()
			delete(s.timers, t)
			s.mu.Unlock()
			s.Reconcile(context.Background())
		})
		s.timers[t] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *Server) streamWatch(ctx context.Context, conn net.Conn, id uint64, send func(response) error) {
	sub := &subscriber{ch: make(chan api.Membership, 1)}
	s.mu.Lock()
	sub.ch <- s.state.membership()
	s.subs[sub] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-sub.ch:
			b, _ := json.Marshal(m)
			if send(response{ID: id, Result: b}) != nil {
				conn.Close()
				return
			}
		}
	}
}

func (s *Server) call(ctx context.Context, peer api.NodeKey, req request) (any, error) {
	decode := func(v any) error {
		if err := json.Unmarshal(req.Params, v); err != nil {
			return fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		return nil
	}
	switch req.Method {
	case mRegister:
		var n api.Node
		if err := decode(&n); err != nil {
			return nil, err
		}
		return s.register(ctx, peer, n)
	case mAddFolder:
		var p folderParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return s.addFolder(ctx, peer, p.Name)
	case mJoinFolder:
		var p folderParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return s.joinFolder(ctx, peer, p.Name)
	case mLeave:
		var p folderParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return nil, s.leaveFolder(ctx, peer, p.Name)
	case mKeep:
		var p keepParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return s.keep(ctx, peer, p.Folder, p.Policy)
	case mClaimPath:
		var c api.PathClaim
		if err := decode(&c); err != nil {
			return nil, err
		}
		return s.claimPath(ctx, peer, c)
	case mPaths:
		var p permanodeParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return s.paths(ctx, peer, p.Permanode)
	case mPathsAt:
		var p pathsAtParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		if err := s.authorizePermanode(peer, p.Permanode); err != nil {
			return nil, err
		}
		return s.cfg.Signer.PathsAt(ctx, p.Permanode, p.At)
	case mDurable:
		var p refsParams
		if err := decode(&p); err != nil {
			return nil, err
		}
		return s.cfg.Durability.Durable(ctx, p.Refs)
	default:
		return nil, fmt.Errorf("%w: unknown method %q", ErrBadRequest, req.Method)
	}
}

func (s *Server) register(ctx context.Context, peer api.NodeKey, n api.Node) (api.Membership, error) {
	err := s.mutate(ctx, func(st *State) error {
		rec := st.node(peer)
		if rec == nil {
			return fmt.Errorf("%w: %s", ErrUnknownNode, peer)
		}
		next := *rec
		next.Key, next.Addr, next.Device, next.Registered = peer, n.Addr, n.Device, true
		if !rec.NamedByAdmin && n.Name != "" {
			next.Name = n.Name
		}
		if next == *rec {
			return errNoChange
		}
		*rec = next
		return nil
	})
	if err != nil {
		return api.Membership{}, err
	}
	return s.Membership(), nil
}

func (s *Server) addFolder(ctx context.Context, peer api.NodeKey, name string) (api.Folder, error) {
	if strings.TrimSpace(name) == "" {
		return api.Folder{}, fmt.Errorf("%w: empty folder name", ErrBadRequest)
	}
	id, err := newFolderID()
	if err != nil {
		return api.Folder{}, err
	}
	var out api.Folder
	err = s.mutate(ctx, func(st *State) error {
		if st.node(peer) == nil {
			return fmt.Errorf("%w: %s", ErrUnknownNode, peer)
		}
		if st.folder(name) != nil {
			return fmt.Errorf("%w: folder %q", ErrExists, name)
		}
		f := api.Folder{Name: name, ID: id, Members: []api.NodeKey{peer}}
		st.Folders = append(st.Folders, f)
		out = cloneFolder(f)
		return nil
	})
	return out, err
}

func (s *Server) joinFolder(ctx context.Context, peer api.NodeKey, name string) (api.Folder, error) {
	var out api.Folder
	err := s.mutate(ctx, func(st *State) error {
		if st.node(peer) == nil {
			return fmt.Errorf("%w: %s", ErrUnknownNode, peer)
		}
		f := st.folder(name)
		if f == nil {
			return fmt.Errorf("%w: folder %q", ErrNotFound, name)
		}
		if contains(f.Members, peer) {
			out = cloneFolder(*f)
			return errNoChange
		}
		f.Members = append(f.Members, peer)
		s.assignKeepersLocked(st)
		out = cloneFolder(*f)
		return nil
	})
	return out, err
}

// leaveFolder removes peer from a folder's members, receive-only list and
// keepers, and refills the keepers from the remaining connected members.
// Leaving a folder the node is not in is a no-op. The folder itself, and
// its kept archive, stay even if its last member leaves.
func (s *Server) leaveFolder(ctx context.Context, peer api.NodeKey, name string) error {
	return s.mutate(ctx, func(st *State) error {
		f := st.folder(name)
		if f == nil {
			return fmt.Errorf("%w: folder %q", ErrNotFound, name)
		}
		if !contains(f.Members, peer) {
			return errNoChange
		}
		f.Members = without(f.Members, peer)
		f.ReceiveOnly = without(f.ReceiveOnly, peer)
		if f.Keep != nil {
			f.Keep.Keepers = without(f.Keep.Keepers, peer)
		}
		s.assignKeepersLocked(st)
		return nil
	})
}

// keep marks a folder kept. The first call creates the folder's permanode
// (or adopts p.Permanode, for a folder that already has a .keepsync file);
// later calls update the timings and keep the permanode. Keepers are always
// assigned by the server, so p.Keepers is ignored.
func (s *Server) keep(ctx context.Context, peer api.NodeKey, folder string, p api.KeepPolicy) (api.BlobRef, error) {
	if err := ValidatePolicy(p); err != nil {
		return "", err
	}
	check := func(st *State) (*api.Folder, error) {
		f := st.folder(folder)
		if f == nil {
			return nil, fmt.Errorf("%w: folder %q", ErrNotFound, folder)
		}
		if !contains(f.Members, peer) {
			return nil, fmt.Errorf("%w: %s in %q", ErrNotMember, peer, folder)
		}
		return f, nil
	}

	s.mu.Lock()
	f, err := check(&s.state)
	var pn api.BlobRef
	if err == nil && f.Keep != nil {
		pn = f.Keep.Permanode
	}
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	if pn == "" {
		pn = p.Permanode
	}
	if pn == "" {
		// Signing may be slow, so it runs without the lock. If two nodes
		// race here, the loser's permanode is left unused.
		pn, err = s.cfg.Signer.NewPermanode(ctx, folder)
		if err != nil {
			return "", fmt.Errorf("registry: new permanode: %w", err)
		}
	}

	var out api.BlobRef
	err = s.mutate(ctx, func(st *State) error {
		f, err := check(st)
		if err != nil {
			return err
		}
		next := api.KeepPolicy{Permanode: pn, Grace: p.Grace, MaxDefer: p.MaxDefer, Expire: p.Expire}
		if f.Keep != nil {
			next.Permanode = f.Keep.Permanode
			next.Keepers = f.Keep.Keepers
		}
		out = next.Permanode
		if other := st.keptFolder(next.Permanode); other != nil && other.Name != f.Name {
			return fmt.Errorf("%w: permanode %s already keeps folder %q", ErrExists, next.Permanode, other.Name)
		}
		if f.Keep != nil && f.Keep.Grace == next.Grace && f.Keep.MaxDefer == next.MaxDefer && f.Keep.Expire == next.Expire {
			return errNoChange
		}
		f.Keep = &next
		s.assignKeepersLocked(st)
		return nil
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

// authorizePermanode checks that pn keeps a folder peer is a member of.
func (s *Server) authorizePermanode(peer api.NodeKey, pn api.BlobRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.state.keptFolder(pn)
	if f == nil {
		return fmt.Errorf("%w: no kept folder for permanode %s", ErrNotFound, pn)
	}
	if !contains(f.Members, peer) {
		return fmt.Errorf("%w: %s in %q", ErrNotMember, peer, f.Name)
	}
	return nil
}

func (s *Server) claimPath(ctx context.Context, peer api.NodeKey, c api.PathClaim) (api.ClaimResult, error) {
	if c.Path == "" || c.File == "" {
		return 0, fmt.Errorf("%w: claim needs a path and a file", ErrBadRequest)
	}
	if err := s.authorizePermanode(peer, c.Permanode); err != nil {
		return 0, err
	}
	return s.cfg.Signer.ClaimPath(ctx, c)
}

func (s *Server) paths(ctx context.Context, peer api.NodeKey, pn api.BlobRef) (map[string]api.PathEntry, error) {
	if err := s.authorizePermanode(peer, pn); err != nil {
		return nil, err
	}
	return s.cfg.Signer.Paths(ctx, pn)
}

// newFolderID makes a Syncthing-style folder ID ("abcde-fghij").
func newFolderID() (string, error) {
	var b [7]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	s := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))[:10]
	return s[:5] + "-" + s[5:], nil
}

func equalKeys(a, b []api.NodeKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
