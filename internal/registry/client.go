package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Client is an api.RegistryClient that talks to a registry Server over an
// api.Transport. Calls share one connection, dialled on first use and
// redialled on the next call after it drops. Each Watch holds its own
// connection and reconnects on its own.
type Client struct {
	t      api.Transport
	server api.Addr

	// RetryMin and RetryMax bound Watch's reconnect backoff.
	RetryMin, RetryMax time.Duration

	mu      sync.Mutex
	conn    *rpcConn
	nextID  uint64
	closed  bool
	closing chan struct{}
}

var _ api.RegistryClient = (*Client)(nil)

// NewClient returns a client for the registry at server.
func NewClient(t api.Transport, server api.Addr) *Client {
	return &Client{
		t:        t,
		server:   server,
		RetryMin: 100 * time.Millisecond,
		RetryMax: 30 * time.Second,
		closing:  make(chan struct{}),
	}
}

// Close drops the client's connections and ends every Watch.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closing)
	if c.conn != nil {
		c.conn.close(ErrClosed)
		c.conn = nil
	}
	return nil
}

type rpcConn struct {
	conn    net.Conn
	wmu     sync.Mutex
	enc     *json.Encoder
	mu      sync.Mutex
	pending map[uint64]chan response
	err     error
}

func (r *rpcConn) close(err error) {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	for id, ch := range r.pending {
		close(ch)
		delete(r.pending, id)
	}
	r.mu.Unlock()
	r.conn.Close()
}

func (r *rpcConn) readLoop() {
	dec := json.NewDecoder(r.conn)
	for {
		var res response
		if err := dec.Decode(&res); err != nil {
			r.close(fmt.Errorf("registry: connection lost: %w", err))
			return
		}
		r.mu.Lock()
		ch := r.pending[res.ID]
		delete(r.pending, res.ID)
		r.mu.Unlock()
		if ch != nil {
			ch <- res
		}
	}
}

// getConn returns the shared connection, dialling if needed. reused
// reports whether the connection was already open before this call.
func (c *Client) getConn(ctx context.Context) (rc *rpcConn, reused bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false, ErrClosed
	}
	if c.conn != nil {
		c.conn.mu.Lock()
		dead := c.conn.err != nil
		c.conn.mu.Unlock()
		if !dead {
			return c.conn, true, nil
		}
		c.conn = nil
	}
	nc, err := c.t.Dial(ctx, c.server, api.PortRegistry)
	if err != nil {
		return nil, false, fmt.Errorf("registry: dial %s: %w", c.server, err)
	}
	rc = &rpcConn{conn: nc, enc: json.NewEncoder(nc), pending: map[uint64]chan response{}}
	go rc.readLoop()
	c.conn = rc
	return rc, false, nil
}

// connError marks a failure of the connection rather than of the call.
type connError struct{ err error }

func (e *connError) Error() string { return e.err.Error() }
func (e *connError) Unwrap() error { return e.err }

func (c *Client) id() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID
}

// call sends one request and waits for its response. If a connection that
// was already open turns out to be dead (the server restarted, say), the
// call is retried once on a fresh connection. Every registry call is safe
// to repeat, except that a repeated AddFolder can report ErrExists and a
// repeated ClaimPath can report ClaimUnchanged for its own first attempt.
func (c *Client) call(ctx context.Context, method string, params, result any) error {
	req := request{Method: method}
	if params != nil {
		var err error
		if req.Params, err = json.Marshal(params); err != nil {
			return err
		}
	}
	for {
		rc, reused, err := c.getConn(ctx)
		if err != nil {
			return err
		}
		err = c.callOn(ctx, rc, req, result)
		var ce *connError
		if reused && errors.As(err, &ce) && ctx.Err() == nil {
			continue
		}
		return err
	}
}

func (c *Client) callOn(ctx context.Context, rc *rpcConn, req request, result any) error {
	req.ID = c.id()
	ch := make(chan response, 1)
	rc.mu.Lock()
	if rc.err != nil {
		err := rc.err
		rc.mu.Unlock()
		return &connError{err}
	}
	rc.pending[req.ID] = ch
	rc.mu.Unlock()

	errc := make(chan error, 1)
	go func() {
		rc.wmu.Lock()
		defer rc.wmu.Unlock()
		errc <- rc.enc.Encode(req)
	}()
	select {
	case err := <-errc:
		if err != nil {
			rc.close(fmt.Errorf("registry: send: %w", err))
			return &connError{err}
		}
	case <-ctx.Done():
		// The write may be stuck on a dead peer; drop the connection so
		// the next call redials.
		rc.close(ctx.Err())
		return ctx.Err()
	}

	select {
	case res, ok := <-ch:
		if !ok {
			rc.mu.Lock()
			err := rc.err
			rc.mu.Unlock()
			if errors.Is(err, ErrClosed) {
				return err
			}
			return &connError{err}
		}
		if res.Error != nil {
			return res.Error.err()
		}
		if result != nil {
			return json.Unmarshal(res.Result, result)
		}
		return nil
	case <-ctx.Done():
		rc.mu.Lock()
		delete(rc.pending, req.ID)
		rc.mu.Unlock()
		return ctx.Err()
	}
}

func (c *Client) Register(ctx context.Context, n api.Node) (api.Membership, error) {
	var m api.Membership
	err := c.call(ctx, mRegister, n, &m)
	return m, err
}

func (c *Client) AddFolder(ctx context.Context, name string) (api.Folder, error) {
	var f api.Folder
	err := c.call(ctx, mAddFolder, folderParams{Name: name}, &f)
	return f, err
}

func (c *Client) JoinFolder(ctx context.Context, name string) (api.Folder, error) {
	var f api.Folder
	err := c.call(ctx, mJoinFolder, folderParams{Name: name}, &f)
	return f, err
}

func (c *Client) Keep(ctx context.Context, folder string, p api.KeepPolicy) (api.BlobRef, error) {
	var ref api.BlobRef
	err := c.call(ctx, mKeep, keepParams{Folder: folder, Policy: p}, &ref)
	return ref, err
}

func (c *Client) ClaimPath(ctx context.Context, cl api.PathClaim) (api.ClaimResult, error) {
	var r api.ClaimResult
	err := c.call(ctx, mClaimPath, cl, &r)
	return r, err
}

func (c *Client) Paths(ctx context.Context, pn api.BlobRef) (map[string]api.PathEntry, error) {
	var out map[string]api.PathEntry
	err := c.call(ctx, mPaths, permanodeParams{Permanode: pn}, &out)
	if out == nil && err == nil {
		out = map[string]api.PathEntry{}
	}
	return out, err
}

func (c *Client) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	var out []bool
	err := c.call(ctx, mDurable, refsParams{Refs: refs}, &out)
	if err == nil && len(out) != len(refs) {
		return nil, fmt.Errorf("registry: durable: got %d answers for %d refs", len(out), len(refs))
	}
	return out, err
}

// Watch delivers the current membership and every later change until ctx is
// done or the client is closed, then closes the channel. If the connection
// drops, Watch reconnects with backoff and delivers the membership again, so
// a reader always ends up with the newest one. A slow reader sees only the
// newest membership, never a backlog. The first connection is made before
// Watch returns, so an unreachable server is reported as an error.
func (c *Client) Watch(ctx context.Context) (<-chan api.Membership, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	conn, err := c.dialWatch(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan api.Membership)
	go c.watchLoop(ctx, conn, out)
	return out, nil
}

func (c *Client) dialWatch(ctx context.Context) (net.Conn, error) {
	nc, err := c.t.Dial(ctx, c.server, api.PortRegistry)
	if err != nil {
		return nil, fmt.Errorf("registry: dial %s: %w", c.server, err)
	}
	if err := json.NewEncoder(nc).Encode(request{ID: 1, Method: mWatch}); err != nil {
		nc.Close()
		return nil, err
	}
	return nc, nil
}

func (c *Client) watchLoop(ctx context.Context, conn net.Conn, out chan<- api.Membership) {
	defer close(out)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
		case <-c.closing:
			cancel()
		}
	}()

	latest := make(chan api.Membership, 1) // newest unread membership
	go func() {
		backoff := c.RetryMin
		for {
			if conn != nil {
				cur := conn
				stop := context.AfterFunc(ctx, func() { cur.Close() })
				err := readWatch(cur, latest)
				stop()
				cur.Close()
				if err == nil {
					backoff = c.RetryMin
				}
				conn = nil
			}
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > c.RetryMax {
				backoff = c.RetryMax
			}
			nc, err := c.dialWatch(ctx)
			if err == nil {
				conn = nc
			}
		}
	}()

	var pending *api.Membership
	for {
		var send chan<- api.Membership
		var m api.Membership
		if pending != nil {
			send, m = out, *pending
		}
		select {
		case <-ctx.Done():
			return
		case n := <-latest:
			if pending == nil || n.Version >= pending.Version {
				pending = &n
			}
		case send <- m:
			pending = nil
		}
	}
}

// readWatch forwards memberships from conn into latest, replacing any
// unread one. It returns nil if at least one membership arrived.
func readWatch(conn net.Conn, latest chan api.Membership) error {
	dec := json.NewDecoder(conn)
	var got bool
	for {
		var res response
		if err := dec.Decode(&res); err != nil {
			if got {
				return nil
			}
			return err
		}
		if res.Error != nil {
			return res.Error.err()
		}
		var m api.Membership
		if err := json.Unmarshal(res.Result, &m); err != nil {
			return err
		}
		got = true
		select {
		case <-latest:
		default:
		}
		latest <- m
	}
}

// LeaveFolder is a placeholder until the registry implements leave
// (workstream B).
func (c *Client) LeaveFolder(ctx context.Context, name string) error {
	return errors.New("registry: LeaveFolder not implemented yet")
}
