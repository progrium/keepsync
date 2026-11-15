package tcnet

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"tailscale.com/tailcfg"
	"tailscale.com/tstest/integration"
	"tailscale.com/types/key"

	"github.com/progrium/keepsync/internal/api"
)

// derp starts a local DERP and STUN server and returns its map.
func derp(t *testing.T) *tailcfg.DERPMap {
	t.Helper()
	return integration.RunDERPAndSTUN(t, func(string, ...any) {}, "127.0.0.1")
}

func newT(t *testing.T, dm *tailcfg.DERPMap, o Options) *Transport {
	t.Helper()
	if o.Region == nil && o.DERPMapURL == "" {
		o.Region = dm.Regions[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tr, err := New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

// whoami answers each line with the caller's identity.
func whoami(c net.Conn, peer api.NodeKey) {
	defer c.Close()
	sc := bufio.NewScanner(c)
	for sc.Scan() {
		fmt.Fprintf(c, "%s %s\n", sc.Text(), peer)
	}
}

func ask(t *testing.T, from *Transport, to api.Addr, port uint16, timeout time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := from.Dial(ctx, to, port)
	if err != nil {
		return "", err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(timeout))
	if _, err := io.WriteString(c, "hi\n"); err != nil {
		return "", err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return "", err
	}
	return line[:len(line)-1], nil
}

func TestDialKeyAgrees(t *testing.T) {
	a, b := key.NewNode(), key.NewNode()
	ab1, err := dialKey(a, b.Public(), a.Public(), b.Public())
	if err != nil {
		t.Fatal(err)
	}
	ab2, _ := dialKey(b, a.Public(), a.Public(), b.Public())
	ba, _ := dialKey(b, a.Public(), b.Public(), a.Public())
	if !ab1.Equal(ab2) {
		t.Error("dialer and target derive different keys")
	}
	if ab1.Equal(ba) {
		t.Error("A→B and B→A share a dial key")
	}
	if ab1.Public() == a.Public() || ab1.Public() == b.Public() {
		t.Error("dial key equals an identity key")
	}
}

func TestDialBothWays(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	a, b := newT(t, dm, Options{}), newT(t, dm, Options{})
	for _, tr := range []*Transport{a, b} {
		tr.Handle(api.PortRegistry, whoami)
	}
	a.Allow(b.Key())
	b.Allow(a.Key())

	got, err := ask(t, a, b.Addr(), api.PortRegistry, 20*time.Second)
	if want := "hi " + string(a.Key()); err != nil || got != want {
		t.Errorf("a→b = %q, %v; want %q", got, err, want)
	}
	got, err = ask(t, b, a.Addr(), api.PortRegistry, 20*time.Second)
	if want := "hi " + string(b.Key()); err != nil || got != want {
		t.Errorf("b→a = %q, %v; want %q", got, err, want)
	}
	// A port with no handler is refused promptly.
	if _, err := ask(t, a, b.Addr(), 9, 5*time.Second); err == nil {
		t.Error("dial to unhandled port succeeded")
	}
}

func TestRefusedAndDisallow(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	srv, ok, stranger := newT(t, dm, Options{}), newT(t, dm, Options{}), newT(t, dm, Options{})
	srv.Handle(api.PortRegistry, whoami)
	srv.Allow(ok.Key())

	if _, err := ask(t, stranger, srv.Addr(), api.PortRegistry, 3*time.Second); err == nil {
		t.Fatal("key not on the allow list connected")
	}
	if _, err := ask(t, ok, srv.Addr(), api.PortRegistry, 20*time.Second); err != nil {
		t.Fatalf("allowed key: %v", err)
	}

	srv.Disallow(ok.Key())
	if _, err := ask(t, ok, srv.Addr(), api.PortRegistry, 3*time.Second); err == nil {
		t.Fatal("disallowed key still connects")
	}
	srv.Allow(ok.Key())
	if _, err := ask(t, ok, srv.Addr(), api.PortRegistry, 20*time.Second); err != nil {
		t.Fatalf("re-allowed key: %v", err)
	}
}

func TestDialTimeoutDefault(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	srv := newT(t, dm, Options{})
	c := newT(t, dm, Options{DialTimeout: time.Second})
	srv.Handle(1, whoami)
	start := time.Now()
	_, err := c.Dial(context.Background(), srv.Addr(), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Dial = %v; want deadline exceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Dial took %v", d)
	}
}

func TestManyPeers(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	hub := newT(t, dm, Options{})
	hub.Handle(api.PortSyncthing, whoami)
	var peers []*Transport
	for i := 0; i < 4; i++ {
		p := newT(t, dm, Options{})
		p.Handle(api.PortSyncthing, whoami)
		p.Allow(hub.Key())
		hub.Allow(p.Key())
		peers = append(peers, p)
	}
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if got, err := ask(t, hub, p.Addr(), api.PortSyncthing, 20*time.Second); err != nil || got != "hi "+string(hub.Key()) {
				t.Errorf("hub→peer = %q, %v", got, err)
			}
		}()
		go func() {
			defer wg.Done()
			if got, err := ask(t, p, hub.Addr(), api.PortSyncthing, 20*time.Second); err != nil || got != "hi "+string(p.Key()) {
				t.Errorf("peer→hub = %q, %v", got, err)
			}
		}()
	}
	wg.Wait()
}

func TestRoundTripper(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	srv, cli := newT(t, dm, Options{}), newT(t, dm, Options{})
	srv.Allow(cli.Key())
	hs := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "path=%s", r.URL.Path)
	})}
	ln := newConnListener()
	srv.Handle(api.PortPerkeep, func(c net.Conn, _ api.NodeKey) { ln.ch <- c })
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close() })

	hc := &http.Client{Transport: cli.RoundTripper(srv.Addr(), api.PortPerkeep), Timeout: 20 * time.Second}
	for i := 0; i < 3; i++ {
		res, err := hc.Get("http://perkeep/camli/x")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if string(b) != "path=/camli/x" {
			t.Errorf("body = %q", b)
		}
	}
}

func TestKeyFile(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	// Serve the local DERP map so the region is picked, not pinned.
	mapJSON, _ := json.Marshal(dm)
	ms := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(mapJSON) }))
	t.Cleanup(ms.Close)

	path := filepath.Join(t.TempDir(), "keys", "server.private.json")
	first := newT(t, dm, Options{KeyFile: path, DERPMapURL: ms.URL})
	addr, k := first.Addr(), first.Key()
	first.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("key file mode %v", fi.Mode().Perm())
	}
	saved, err := LoadKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Public.Region) == 0 {
		t.Error("picked region not saved")
	}

	second := newT(t, dm, Options{KeyFile: path, DERPMapURL: ms.URL})
	if second.Addr() != addr || second.Key() != k {
		t.Errorf("restart changed identity:\n%v %v\n%v %v", addr, k, second.Addr(), second.Key())
	}
}

// connListener is a net.Listener fed from a Handle callback.
type connListener struct {
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func newConnListener() *connListener {
	return &connListener{ch: make(chan net.Conn), done: make(chan struct{})}
}

func (l *connListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *connListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *connListener) Addr() net.Addr { return &net.TCPAddr{} }
