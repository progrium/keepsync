package node

import (
	"context"
	"sync"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/registry"
)

// liveClient is an api.RegistryClient whose underlying registry.Client is
// replaced when the server stops answering.
//
// A tailcat connection to a server that went away is not reset: reads just
// block. registry.Client has no keepalive, so its shared connection and a
// Watch stream would hang forever and the node would never re-register when
// the server comes back. probe detects that with a cheap call under a
// timeout and swaps in a fresh client, which ends the old Watch.
type liveClient struct {
	t      api.Transport
	server api.Addr

	mu  sync.Mutex
	cur *registry.Client
}

var _ api.RegistryClient = (*liveClient)(nil)

func newLiveClient(t api.Transport, server api.Addr) *liveClient {
	return &liveClient{t: t, server: server, cur: registry.NewClient(t, server)}
}

func (l *liveClient) c() *registry.Client {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cur
}

// reset closes the current client (ending its Watch streams) and starts a
// new one.
func (l *liveClient) reset() {
	l.mu.Lock()
	old := l.cur
	l.cur = registry.NewClient(l.t, l.server)
	l.mu.Unlock()
	old.Close()
}

// Close closes the current client.
func (l *liveClient) Close() error { return l.c().Close() }

// probe asks the server a trivial question. A failure resets the client.
func (l *liveClient) probe(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c := l.c()
	_, err := c.Durable(ctx, nil)
	if err != nil {
		l.mu.Lock()
		same := l.cur == c
		l.mu.Unlock()
		if same {
			l.reset()
		}
	}
	return err
}

func (l *liveClient) Register(ctx context.Context, n api.Node) (api.Membership, error) {
	return l.c().Register(ctx, n)
}
func (l *liveClient) Watch(ctx context.Context) (<-chan api.Membership, error) {
	return l.c().Watch(ctx)
}
func (l *liveClient) AddFolder(ctx context.Context, name string) (api.Folder, error) {
	return l.c().AddFolder(ctx, name)
}
func (l *liveClient) JoinFolder(ctx context.Context, name string) (api.Folder, error) {
	return l.c().JoinFolder(ctx, name)
}
func (l *liveClient) Keep(ctx context.Context, folder string, p api.KeepPolicy) (api.BlobRef, error) {
	return l.c().Keep(ctx, folder, p)
}
func (l *liveClient) ClaimPath(ctx context.Context, c api.PathClaim) (api.ClaimResult, error) {
	return l.c().ClaimPath(ctx, c)
}
func (l *liveClient) Paths(ctx context.Context, pn api.BlobRef) (map[string]api.PathEntry, error) {
	return l.c().Paths(ctx, pn)
}
func (l *liveClient) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	return l.c().Durable(ctx, refs)
}
