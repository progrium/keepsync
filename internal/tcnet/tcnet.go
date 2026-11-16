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

	// Open connections per tunnel, so Path knows which tunnel to ask.
	inLive  map[key.NodePublic]int // by identity: they dialed our server
	outLive map[api.Addr]int       // by address: we dialed their server
}

var (
	_ api.Transport    = (*Transport)(nil)
	_ api.PathReporter = (*Transport)(nil)
)

// ErrClosed is returned by Dial after Close.
var ErrClosed = errors.New("tcnet: transport closed")

// New starts a Transport: it loads or creates the key, connects to the
// DERP relay and begins accepting allowed clients.
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
		inLive:      map[key.NodePublic]int{},
		outLive:     map[api.Addr]int{},
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
		h(t.track(c, func(d int) { t.inLive[id] += d }), api.NodeKey(id.String()))
	}
}

// Dial opens a TCP connection to a virtual port on the process at to. The
// remote must have allowed this transport's Key. A remote that does not
// answer (because it does not allow us, is down or does not exist) makes
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
	return t.track(conn, func(d int) { t.outLive[to] += d }), nil
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
	clients := t.clients
	t.clients = map[api.Addr]*tailcat.Client{}
	t.mu.Unlock()
	for _, c := range clients {
		c.Close()
	}
	return t.srv.Close()
}

// pathPingTimeout bounds the disco ping Path sends on an outbound tunnel.
const pathPingTimeout = 2 * time.Second

// Path reports how traffic to the peer travels. It prefers the tunnel the
// peer dialed into our server, whose state tailcat already tracks; failing
// that it disco pings the peer over the tunnel we dialed, which also nudges
// that tunnel towards a direct path. Both tunnels run between the same two
// hosts, so they normally agree.
func (t *Transport) Path(ctx context.Context, peer api.NodeKey, to api.Addr) (api.Path, bool) {
	id, err := ParseKey(peer)
	if err != nil {
		return api.Path{}, false
	}
	t.mu.Lock()
	dk, allowed := t.idents[id]
	in := allowed && t.inLive[id] > 0
	c := t.clients[to]
	out := c != nil && t.outLive[to] > 0
	t.mu.Unlock()
	if in {
		if ps, ok := t.srv.Status().Peer[dk]; ok && (ps.CurAddr != "" || ps.Relay != "") {
			if ps.CurAddr != "" {
				return api.Path{Direct: true, Endpoint: ps.CurAddr}, true
			}
			return api.Path{Relay: ps.Relay}, true
		}
	}
	if !out {
		return api.Path{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, pathPingTimeout)
	defer cancel()
	r, err := c.DiscoPing(ctx)
	switch {
	case err != nil:
		return api.Path{}, false
	case r.Endpoint != "":
		return api.Path{Direct: true, Endpoint: r.Endpoint}, true
	case r.DERPRegionCode != "":
		return api.Path{Relay: r.DERPRegionCode}, true
	case r.DERPRegionID != 0:
		return api.Path{Relay: fmt.Sprint(r.DERPRegionID)}, true
	}
	return api.Path{}, false
}

// track counts c as open on a tunnel until it is closed. add runs under
// t.mu with +1 now and -1 on the first Close.
func (t *Transport) track(c net.Conn, add func(int)) net.Conn {
	t.mu.Lock()
	add(1)
	t.mu.Unlock()
	return &trackedConn{Conn: c, done: func() {
		t.mu.Lock()
		add(-1)
		t.mu.Unlock()
	}}
}

type trackedConn struct {
	net.Conn
	once sync.Once
	done func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.done)
	return err
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
