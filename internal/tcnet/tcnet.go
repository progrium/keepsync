// Package tcnet implements api.Transport on tailcat.
//
// Each keepsync process runs one tailcat.Server, which accepts connections
// and dispatches them by virtual port, plus one tailcat.Client per remote
// address it dials. See docs/design.md, "Networking over tailcat".
//
// # Identity and dial keys
//
// A process is identified by its server key (Key, "nodekey:…"), and that is
// what other nodes put on their allow lists. Clients do not dial with that
// key. A tailcat.Client holds its own DERP connection, and DERP routes by
// node key, so several clients (or a client and the server) sharing one key
// fight over it: in a test with three peers, concurrent dials over DERP took
// up to 7 s with a shared key against 1 s with distinct keys.
//
// So the key a process dials a peer with is derived per (dialer, target)
// pair from an X25519 exchange between their server keys. The target can
// derive the same public key from its own private key and the dialer's
// public key, so Allow(identity) admits exactly the dial key that identity
// uses towards this process, and handlers see the dialer's identity, not the
// per-pair key. Nobody else can compute a pair's dial key.
package tcnet

import (
	"context"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/tailcat"
	"go4.org/mem"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"

	"github.com/progrium/keepsync/internal/api"
)

// DefaultDialTimeout bounds Dial when the caller's context has no deadline.
// A remote that does not allow our key never answers, so without a deadline
// Dial would wait forever.
const DefaultDialTimeout = 30 * time.Second

// Options configures a Transport.
type Options struct {
	// Key is the process identity. If nil, KeyFile is used; if both are
	// unset, an ephemeral key is generated.
	Key *tailcat.PrivateKey

	// KeyFile is where the identity is saved, as tailcat's
	// *.private.json format with owner-only permissions. It is created on
	// first use, and the DERP region picked at that first start is saved
	// into it, so the address stays the same across restarts.
	KeyFile string

	// Region, if set, pins the bootstrap DERP region (for example the
	// user's own derper), overriding the key's saved region.
	Region *tailcfg.DERPRegion

	// RegionID, if non-zero and Region is nil, picks a region from the
	// DERP map by ID, overriding the key's saved region.
	RegionID int

	// DERPMapURL overrides tailcat.DefaultDERPMapURL.
	DERPMapURL string

	// DialTimeout overrides DefaultDialTimeout.
	DialTimeout time.Duration

	// Logf receives tailcat's debug logging. If nil, it is discarded.
	Logf logger.Logf
}

// Transport is an api.Transport on tailcat.
type Transport struct {
	srv         *tailcat.Server
	priv        key.NodePrivate
	pub         key.NodePublic
	addr        api.Addr
	logf        logger.Logf
	derpMapURL  string
	dialTimeout time.Duration

	mu       sync.Mutex
	handlers map[uint16]func(net.Conn, api.NodeKey)
	allowed  map[key.NodePublic]key.NodePublic // incoming dial key -> identity
	idents   map[key.NodePublic]key.NodePublic // identity -> incoming dial key
	clients  map[api.Addr]*tailcat.Client
	closed   bool

	// Each peer can have two tunnels: one it dialed into our server (in)
	// and one we dialed into its server (out). tunnels tracks each one's
	// open connections, bytes moved and, for out, the last disco ping.
	tunnels map[tunnelKey]*tunnel
	stop    chan struct{}
}

type tunnelKey struct {
	id  key.NodePublic // the peer's identity
	out bool
}

// tunnel is one direction's tunnel to a peer. Fields other than the
// counters are guarded by Transport.mu.
type tunnel struct {
	peerTraffic
	live int      // open connections
	addr api.Addr // for out: the address we dialed
	// For out: the last disco ping's answer, nil before the first.
	path    *api.Path
	pathAt  time.Time
	pinging bool
}

var (
	_ api.Transport    = (*Transport)(nil)
	_ api.PathReporter = (*Transport)(nil)
)

// ErrClosed is returned by Dial after Close.
var ErrClosed = errors.New("tcnet: transport closed")

// New starts a Transport: it loads or creates the key, connects to the
// DERP relay, and begins accepting allowed clients.
func New(ctx context.Context, o Options) (*Transport, error) {
	logf := o.Logf
	if logf == nil {
		logf = logger.Discard
	}
	pk := o.Key
	saveRegion := false // KeyFile has no region yet: save the one picked
	if pk == nil && o.KeyFile != "" {
		var err error
		pk, err = LoadKey(o.KeyFile)
		if errors.Is(err, errNoKey) {
			pk = NewKey()
			err = SaveKey(o.KeyFile, pk)
		}
		if err != nil {
			return nil, err
		}
		saveRegion = len(pk.Public.Region) == 0 && pk.Public.RegionID <= 0
	}
	if pk == nil {
		pk = NewKey()
	}
	t := &Transport{
		priv:        pk.Private,
		pub:         pk.Private.Public(),
		logf:        logf,
		derpMapURL:  o.DERPMapURL,
		dialTimeout: o.DialTimeout,
		handlers:    map[uint16]func(net.Conn, api.NodeKey){},
		allowed:     map[key.NodePublic]key.NodePublic{},
		idents:      map[key.NodePublic]key.NodePublic{},
		clients:     map[api.Addr]*tailcat.Client{},
		tunnels:     map[tunnelKey]*tunnel{},
		stop:        make(chan struct{}),
	}
	if t.dialTimeout == 0 {
		t.dialTimeout = DefaultDialTimeout
	}
	s := &tailcat.Server{
		Key:          pk.Private,
		PresharedKey: pk.Public.PresharedKey,
		Logf:         logf,
		DERPMapURL:   o.DERPMapURL,
		AllowClient:  t.allowClient,
		OnTCP:        t.onTCP,
	}
	switch {
	case o.Region != nil:
		s.Region = o.Region
	case o.RegionID != 0:
		s.RegionID = tailcfg.DERPRegionID(o.RegionID)
	case len(pk.Public.Region) > 0:
		s.Region = pk.Public.Region[0]
	case pk.Public.RegionID > 0:
		s.RegionID = pk.Public.RegionID
	}
	if err := s.StartContext(ctx); err != nil {
		return nil, fmt.Errorf("tcnet: start: %w", err)
	}
	t.srv = s
	t.addr = api.Addr(s.TailcatAddr())
	go t.sample()
	go t.nudge()
	if saveRegion && o.Region == nil && o.RegionID == 0 {
		// Remember the region picked by latency so the address is
		// stable. The address embeds the full region either way.
		if ci, err := tailcat.ParseAddr(s.TailcatAddr()); err == nil && len(ci.Region) > 0 {
			pk.Public.Region = ci.Region
			pk.Public.RegionID = 0
			if err := SaveKey(o.KeyFile, pk); err != nil {
				s.Close()
				return nil, err
			}
		}
	}
	return t, nil
}

func (t *Transport) Addr() api.Addr   { return t.addr }
func (t *Transport) Key() api.NodeKey { return api.NodeKey(t.pub.String()) }

func (t *Transport) Handle(port uint16, h func(conn net.Conn, peer api.NodeKey)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if h == nil {
		delete(t.handlers, port)
		return
	}
	t.handlers[port] = h
}

// Allow admits the given identities. Invalid keys are ignored.
func (t *Transport) Allow(keys ...api.NodeKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range keys {
		id, err := ParseKey(k)
		if err != nil || id == t.pub {
			continue
		}
		if _, ok := t.idents[id]; ok {
			continue
		}
		dk, err := dialKey(t.priv, id, id, t.pub)
		if err != nil {
			t.logf("tcnet: allow %v: %v", k, err)
			continue
		}
		dp := dk.Public()
		t.allowed[dp] = id
		t.idents[id] = dp
	}
}

// Disallow removes identities from the allow list and drops their current
// connections.
func (t *Transport) Disallow(keys ...api.NodeKey) {
	var drop []key.NodePublic
	t.mu.Lock()
	for _, k := range keys {
		id, err := ParseKey(k)
		if err != nil {
			continue
		}
		if dp, ok := t.idents[id]; ok {
			delete(t.idents, id)
			delete(t.allowed, dp)
			drop = append(drop, dp)
		}
	}
	t.mu.Unlock()
	for _, dp := range drop {
		t.srv.DisconnectClient(dp)
	}
}

func (t *Transport) allowClient(k key.NodePublic) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.allowed[k]
	return ok && !t.closed
}

// onTCP is tailcat's per-port hook. The handler is looked up per
// connection, so Handle works before or after Start.
func (t *Transport) onTCP(port uint16) func(net.Conn) {
	t.mu.Lock()
	_, ok := t.handlers[port]
	t.mu.Unlock()
	if !ok {
		return nil
	}
	return func(c net.Conn) {
		dk, ok := t.srv.PeerKey(c.RemoteAddr())
		t.mu.Lock()
		id, allowed := t.allowed[dk]
		h := t.handlers[port]
		t.mu.Unlock()
		if !ok || !allowed || h == nil {
			c.Close()
			return
		}
		h(t.track(c, tunnelKey{id: id}, ""), api.NodeKey(id.String()))
	}
}

// Dial opens a TCP connection to a virtual port on the process at to. The
// remote must have allowed this transport's Key. A remote that does not
// answer (because it does not allow us, is down, or does not exist) makes
// Dial wait for ctx, or DialTimeout if ctx has no deadline.
func (t *Transport) Dial(ctx context.Context, to api.Addr, port uint16) (net.Conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.dialTimeout)
		defer cancel()
	}
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	conn, err := c.DialTCPPort(ctx, port)
	if err != nil {
		// The remote may have restarted or dropped us. Start the next
		// dial on a fresh client.
		t.dropClient(to, c)
		return nil, fmt.Errorf("tcnet: dial %v port %d: %w", short(to), port, err)
	}
	// client parsed to already, so this cannot fail.
	ci, _ := tailcat.ParseAddr(tailcat.Addr(to))
	return t.track(conn, tunnelKey{id: ci.ServerPublic.NodePublic, out: true}, to), nil
}

func (t *Transport) client(to api.Addr) (*tailcat.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if c := t.clients[to]; c != nil {
		return c, nil
	}
	ci, err := tailcat.ParseAddr(tailcat.Addr(to))
	if err != nil {
		return nil, fmt.Errorf("tcnet: bad address: %w", err)
	}
	dk, err := dialKey(t.priv, ci.ServerPublic.NodePublic, t.pub, ci.ServerPublic.NodePublic)
	if err != nil {
		return nil, err
	}
	c := &tailcat.Client{Server: tailcat.Addr(to), Key: dk, Logf: t.logf, DERPMapURL: t.derpMapURL}
	t.clients[to] = c
	return c, nil
}

func (t *Transport) dropClient(to api.Addr, c *tailcat.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	} else {
		c = nil
	}
	t.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// RoundTripper returns an http.RoundTripper that sends every request to the
// given virtual port at to, whatever host the request URL names. Use it with
// URLs like http://perkeep/.
func (t *Transport) RoundTripper(to api.Addr, port uint16) http.RoundTripper {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return t.Dial(ctx, to, port)
		},
		ForceAttemptHTTP2:   false,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
	}
}

// Close stops the server and every client. Connections already handed out
// are closed with them.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.stop)
	clients := t.clients
	t.clients = map[api.Addr]*tailcat.Client{}
	t.mu.Unlock()
	for _, c := range clients {
		c.Close()
	}
	return t.srv.Close()
}

// pathPingTimeout bounds a disco ping on an outbound tunnel.
const pathPingTimeout = 2 * time.Second

// Disco pings on outbound tunnels. A tunnel that is not known to be direct
// is pinged every nudgeEvery, which is how a tunnel gets upgraded from DERP
// to direct: tailcat has no control plane to hand out endpoints, so path
// discovery only runs when something pings. Tunnels asks again when its
// answer is older than pathFresh.
const (
	nudgeEvery = 15 * time.Second
	pathFresh  = 20 * time.Second
)

// Tunnels reports each open tunnel to the peer with identity peerKey: how
// its traffic travels, whether it is idle, and the bytes it moved.
//
// For the tunnel the peer dialed in, the path comes from tailcat's own
// status. tailcat only reports a direct address while it trusts it, which
// lapses about 6.5 s after the last pong, so an idle tunnel can look
// relayed when it is not: for an idle tunnel without a current direct
// address the path is left unknown. For the tunnel we dialed, the path is
// the last disco ping's answer, refreshed if it is stale. A pong is a
// live measurement, so it holds for idle tunnels too.
func (t *Transport) Tunnels(ctx context.Context, peerKey api.NodeKey, _ api.Addr) []api.Tunnel {
	id, err := ParseKey(peerKey)
	if err != nil {
		return nil
	}
	now := time.Now()
	var out []api.Tunnel
	t.mu.Lock()
	in := t.tunnels[tunnelKey{id: id}]
	dk, allowed := t.idents[id]
	var inT *api.Tunnel
	if in != nil && in.live > 0 && allowed {
		inT = &api.Tunnel{Conns: in.live, Traffic: in.report(now)}
	}
	o := t.tunnels[tunnelKey{id: id, out: true}]
	var outT *api.Tunnel
	stale := false
	if o != nil && o.live > 0 {
		outT = &api.Tunnel{Out: true, Conns: o.live, Traffic: o.report(now)}
		stale = o.path == nil || now.Sub(o.pathAt) > pathFresh
	}
	t.mu.Unlock()

	if inT != nil {
		inT.Idle = inT.Traffic.InRate == 0 && inT.Traffic.OutRate == 0
		if ps, ok := t.srv.Status().Peer[dk]; ok {
			switch {
			case ps.CurAddr != "":
				inT.Path = &api.Path{Direct: true, Endpoint: ps.CurAddr}
			case !inT.Idle && ps.Relay != "":
				inT.Path = &api.Path{Relay: ps.Relay}
			}
		}
		out = append(out, *inT)
	}
	if outT != nil {
		outT.Idle = outT.Traffic.InRate == 0 && outT.Traffic.OutRate == 0
		if stale {
			t.ping(ctx, o)
		}
		t.mu.Lock()
		if o.path != nil {
			p := *o.path
			outT.Path = &p
		}
		t.mu.Unlock()
		out = append(out, *outT)
	}
	return out
}

// ping disco pings the outbound tunnel tn and records the answer. Only one
// ping per tunnel runs at a time; a caller that finds one running waits for
// nothing and uses the previous answer.
func (t *Transport) ping(ctx context.Context, tn *tunnel) {
	t.mu.Lock()
	c := t.clients[tn.addr]
	if c == nil || tn.pinging {
		t.mu.Unlock()
		return
	}
	tn.pinging = true
	t.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, pathPingTimeout)
	defer cancel()
	r, err := c.DiscoPing(ctx)
	var p *api.Path
	switch {
	case err != nil:
	case r.Endpoint != "":
		p = &api.Path{Direct: true, Endpoint: r.Endpoint}
	case r.DERPRegionCode != "":
		p = &api.Path{Relay: r.DERPRegionCode}
	case r.DERPRegionID != 0:
		p = &api.Path{Relay: fmt.Sprint(r.DERPRegionID)}
	}
	t.mu.Lock()
	tn.pinging = false
	if p != nil {
		tn.path, tn.pathAt = p, time.Now()
	}
	t.mu.Unlock()
}

// nudge pings every open outbound tunnel that is not known to be direct,
// every nudgeEvery, so tailcat keeps trying to upgrade it from DERP.
func (t *Transport) nudge() {
	tick := time.NewTicker(nudgeEvery)
	defer tick.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
		}
		var due []*tunnel
		t.mu.Lock()
		for k, tn := range t.tunnels {
			if k.out && tn.live > 0 && (tn.path == nil || !tn.path.Direct) {
				due = append(due, tn)
			}
		}
		t.mu.Unlock()
		for _, tn := range due {
			go t.ping(ctx, tn)
		}
	}
}

// track counts c as open on tunnel k until it is closed, and counts the
// bytes it moves. addr is the address dialed, for an outbound tunnel.
func (t *Transport) track(c net.Conn, k tunnelKey, addr api.Addr) net.Conn {
	t.mu.Lock()
	tn := t.tunnels[k]
	if tn == nil {
		tn = &tunnel{}
		t.tunnels[k] = tn
	}
	if addr != "" && tn.addr != addr {
		tn.addr, tn.path = addr, nil
	}
	tn.live++
	t.mu.Unlock()
	return &trackedConn{Conn: c, tr: &tn.peerTraffic, done: func() {
		t.mu.Lock()
		tn.live--
		t.mu.Unlock()
	}}
}

type trackedConn struct {
	net.Conn
	tr   *peerTraffic
	once sync.Once
	done func()
}

func (c *trackedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.tr.in.Add(int64(n))
	return n, err
}

func (c *trackedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.tr.out.Add(int64(n))
	return n, err
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.done)
	return err
}

// Traffic rates are averaged over rateWindow, from a sample of every
// tunnel's byte counters taken each rateTick.
const (
	rateTick   = time.Second
	rateWindow = 10 * time.Second
)

// peerTraffic is the bytes moved on one tunnel. The counters are atomic;
// samples is guarded by Transport.mu.
type peerTraffic struct {
	in, out atomic.Int64
	samples []trafficSample // oldest first, spanning at most rateWindow
}

type trafficSample struct {
	at      time.Time
	in, out int64
}

func (t *Transport) sample() {
	tick := time.NewTicker(rateTick)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case now := <-tick.C:
			t.mu.Lock()
			for _, tn := range t.tunnels {
				tn.record(now)
			}
			t.mu.Unlock()
		}
	}
}

func (tr *peerTraffic) record(now time.Time) {
	tr.samples = append(tr.samples, trafficSample{now, tr.in.Load(), tr.out.Load()})
	drop := 0
	for drop < len(tr.samples)-1 && now.Sub(tr.samples[drop].at) > rateWindow {
		drop++
	}
	tr.samples = tr.samples[drop:]
}

// report is the traffic as of now. t.mu must be held.
func (tr *peerTraffic) report(now time.Time) api.Traffic {
	in, out := tr.in.Load(), tr.out.Load()
	r := api.Traffic{InTotal: in, OutTotal: out}
	if len(tr.samples) > 0 {
		old := tr.samples[0]
		if secs := now.Sub(old.at).Seconds(); secs >= rateTick.Seconds()/2 {
			r.InRate = float64(in-old.in) / secs
			r.OutRate = float64(out-old.out) / secs
		}
	}
	return r
}

var _ api.TrafficReporter = (*Transport)(nil)

// Traffic reports the bytes moved with the peer over both tunnels, with
// rates over the last rateWindow.
func (t *Transport) Traffic(peer api.NodeKey) (api.Traffic, bool) {
	id, err := ParseKey(peer)
	if err != nil {
		return api.Traffic{}, false
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	var sum api.Traffic
	found := false
	for _, out := range []bool{false, true} {
		if tn := t.tunnels[tunnelKey{id: id, out: out}]; tn != nil {
			r := tn.report(now)
			sum.InRate += r.InRate
			sum.OutRate += r.OutRate
			sum.InTotal += r.InTotal
			sum.OutTotal += r.OutTotal
			found = true
		}
	}
	return sum, found
}

// ParseKey parses a "nodekey:<hex>" api.NodeKey.
func ParseKey(k api.NodeKey) (key.NodePublic, error) {
	var p key.NodePublic
	if err := p.UnmarshalText([]byte(k)); err != nil {
		return key.NodePublic{}, fmt.Errorf("tcnet: bad node key %q: %w", k, err)
	}
	return p, nil
}

// dialKey derives the private key that dialer uses to dial target, from one
// side's private key and the other side's public key. Both ends get the
// same result: X25519(dialer, target) == X25519(target, dialer). The two
// public keys are mixed in, in dialer-then-target order, so A dialing B and
// B dialing A use different keys and do not share a DERP slot.
func dialKey(ourPriv key.NodePrivate, theirPub, dialer, target key.NodePublic) (key.NodePrivate, error) {
	raw := ourPriv.Raw32()
	priv, err := ecdh.X25519().NewPrivateKey(raw[:])
	if err != nil {
		return key.NodePrivate{}, err
	}
	praw := theirPub.Raw32()
	pub, err := ecdh.X25519().NewPublicKey(praw[:])
	if err != nil {
		return key.NodePrivate{}, err
	}
	shared, err := priv.ECDH(pub)
	if err != nil {
		return key.NodePrivate{}, fmt.Errorf("tcnet: key exchange: %w", err)
	}
	d, t := dialer.Raw32(), target.Raw32()
	info := append(append([]byte("keepsync tcnet dial v1"), d[:]...), t[:]...)
	out, err := hkdf.Key(sha256.New, shared, nil, string(info), 32)
	if err != nil {
		return key.NodePrivate{}, err
	}
	return key.NodePrivateFromRaw32(mem.B(out)), nil
}

func short(a api.Addr) string {
	if len(a) > 16 {
		return string(a[:16]) + "…"
	}
	return string(a)
}
