package pkserver

import (
	"context"
	"net"
	"sync"

	"perkeep.org/pkg/auth"
)

// grantConn is a connection carrying the operations its requests are
// granted, which the http.Server puts in each request's context.
type grantConn struct {
	net.Conn
	grant auth.Operation
}

func grantFor(c net.Conn) auth.Operation {
	if g, ok := c.(*grantConn); ok {
		return g.grant
	}
	return 0
}

// pipeListener is a net.Listener fed by push, for connections that arrive
// over tailcat or from inside the process.
type pipeListener struct {
	ch     chan net.Conn
	done   chan struct{}
	closer sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{ch: make(chan net.Conn), done: make(chan struct{})}
}

func (l *pipeListener) push(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.done:
		c.Close()
	}
}

func (l *pipeListener) dial(ctx context.Context, grant auth.Operation) (net.Conn, error) {
	local, far := net.Pipe()
	select {
	case l.ch <- &grantConn{Conn: far, grant: grant}:
		return local, nil
	case <-l.done:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closer.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pkserver" }
