package syncer

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// The localhost bridge (design: "Syncthing transport", first version).
//
// Inbound: the Transport hands us a conn on api.PortSyncthing; we dial
// Syncthing's loopback listener and splice the two.
//
// Outbound: each peer gets a loopback listener whose address is the peer's
// only Syncthing address. When Syncthing dials it, we Transport.Dial the
// peer's tailcat address on api.PortSyncthing and splice.
//
// BEP runs TLS end to end over the splice, so Syncthing still verifies the
// device ID; the Transport separately authenticates the node key.
//
// One tunnel per pair. Left alone, both nodes dial each other, so a pair
// has two tunnels that hole-punch separately: one can be direct while the
// other crawls over a rate-limited DERP relay, and Syncthing spreads its
// connections over both. So only the node with the lower key dials. The
// other refuses Syncthing's dials and waits to be dialed, unless it has had
// no inbound connection from the peer for dialFallback, in case the
// dialer cannot reach it.

// dialFallback is how long the node that does not dial waits without an
// inbound connection before dialing the peer itself.
var dialFallback = 30 * time.Second // a var for tests

// inbound is the inbound connections from one peer.
type inbound struct {
	active    int
	idleSince time.Time // when active last dropped to 0, or first asked
}

// shouldDial reports whether this node may dial peer now.
func (s *Syncer) shouldDial(peer api.NodeKey) bool {
	if s.cfg.Transport.Key() < peer {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	in := s.inboundLocked(peer)
	return in.active == 0 && time.Since(in.idleSince) >= dialFallback
}

func (s *Syncer) inboundLocked(peer api.NodeKey) *inbound {
	in := s.inbound[peer]
	if in == nil {
		in = &inbound{idleSince: time.Now()}
		s.inbound[peer] = in
	}
	return in
}

func (s *Syncer) handleInbound(conn net.Conn, peer api.NodeKey) {
	s.mu.Lock()
	known := s.allowed[peer]
	addr := s.listenAddr
	running := s.running
	s.mu.Unlock()
	if !known || !running {
		conn.Close()
		return
	}
	s.mu.Lock()
	s.inboundLocked(peer).active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		in := s.inboundLocked(peer)
		if in.active--; in.active == 0 {
			in.idleSince = time.Now()
		}
		s.mu.Unlock()
	}()
	var d net.Dialer
	local, err := d.DialContext(s.ctx, "tcp", addr)
	if err != nil {
		slog.Debug("syncer: inbound bridge dial", "err", err)
		conn.Close()
		return
	}
	splice(conn, local)
}

// outBridge is the loopback listener standing in for one peer.
type outBridge struct {
	ln net.Listener
	s  *Syncer

	mu     sync.Mutex
	target api.Addr
	key    api.NodeKey
	ctx    context.Context
	cancel context.CancelFunc
}

func newOutBridge(s *Syncer, target api.Addr, key api.NodeKey) (*outBridge, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &outBridge{ln: ln, s: s, target: target, key: key, ctx: ctx, cancel: cancel}
	go b.serve()
	return b, nil
}

func (b *outBridge) addr() string { return b.ln.Addr().String() }

func (b *outBridge) setTarget(a api.Addr, key api.NodeKey) {
	b.mu.Lock()
	b.target, b.key = a, key
	b.mu.Unlock()
}

func (b *outBridge) serve() {
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}
		go b.forward(c)
	}
}

func (b *outBridge) forward(local net.Conn) {
	b.mu.Lock()
	target, key := b.target, b.key
	b.mu.Unlock()
	if !b.s.shouldDial(key) {
		// The peer dials us; see "One tunnel per pair".
		local.Close()
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	remote, err := b.s.cfg.Transport.Dial(ctx, target, api.PortSyncthing)
	cancel()
	if err != nil {
		slog.Debug("syncer: outbound bridge dial", "to", target, "err", err)
		local.Close()
		return
	}
	splice(local, remote)
}

func (b *outBridge) close() {
	b.cancel()
	b.ln.Close()
}

// syncBridgesLocked makes the bridge set match s.peers.
func (s *Syncer) syncBridgesLocked() error {
	for id, b := range s.bridges {
		if _, ok := s.peers[id]; !ok {
			b.close()
			delete(s.bridges, id)
		}
	}
	for id, p := range s.peers {
		if p.Addr == "" {
			// Restored from config.xml; not dialable until SetPeers.
			if b, ok := s.bridges[id]; ok {
				b.close()
				delete(s.bridges, id)
			}
			continue
		}
		if b, ok := s.bridges[id]; ok {
			b.setTarget(p.Addr, p.Key)
			continue
		}
		b, err := newOutBridge(s, p.Addr, p.Key)
		if err != nil {
			return err
		}
		s.bridges[id] = b
	}
	return nil
}

// splice copies both ways until either side is done, then closes both.
func splice(a, b net.Conn) {
	var once sync.Once
	closeBoth := func() { a.Close(); b.Close() }
	go func() {
		io.Copy(a, b)
		once.Do(closeBoth)
	}()
	io.Copy(b, a)
	once.Do(closeBoth)
}
