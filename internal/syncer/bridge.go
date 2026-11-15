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
	tr api.Transport

	mu     sync.Mutex
	target api.Addr
	ctx    context.Context
	cancel context.CancelFunc
}

func newOutBridge(tr api.Transport, target api.Addr) (*outBridge, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &outBridge{ln: ln, tr: tr, target: target, ctx: ctx, cancel: cancel}
	go b.serve()
	return b, nil
}

func (b *outBridge) addr() string { return b.ln.Addr().String() }

func (b *outBridge) setTarget(a api.Addr) {
	b.mu.Lock()
	b.target = a
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
	target := b.target
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	remote, err := b.tr.Dial(ctx, target, api.PortSyncthing)
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
		if b, ok := s.bridges[id]; ok {
			b.setTarget(p.Addr)
			continue
		}
		b, err := newOutBridge(s.cfg.Transport, p.Addr)
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
