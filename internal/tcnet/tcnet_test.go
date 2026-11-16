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

func TestPath(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	a, b := newT(t, dm, Options{}), newT(t, dm, Options{})
	held := make(chan struct{})
	b.Handle(api.PortSyncthing, func(c net.Conn, _ api.NodeKey) {
		defer c.Close()
		c.Write([]byte{1})
		<-held
	})
	a.Allow(b.Key())
	b.Allow(a.Key())

	// No connection yet: neither side can tell.
	if p, ok := a.Path(context.Background(), b.Key(), b.Addr()); ok {
		t.Errorf("a.Path before dial = %+v, want none", p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := a.Dial(ctx, b.Addr(), api.PortSyncthing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}

	valid := func(p api.Path) bool {
		if p.Direct {
			return p.Endpoint != "" && p.Relay == ""
		}
		return p.Relay != ""
	}
	check := func(name string, from, to *Transport) {
		t.Helper()
		var p api.Path
		var ok bool
		for range 20 {
			if p, ok = from.Path(ctx, to.Key(), to.Addr()); ok {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if !ok || !valid(p) {
			t.Errorf("%s = %+v, %v; want a direct endpoint or a relay", name, p, ok)
		} else {
			t.Logf("%s = %+v", name, p)
		}
	}
	check("a.Path (outbound)", a, b)
	check("b.Path (inbound)", b, a)

	// Once the connection closes on both ends, the path is unknown again.
	c.Close()
	close(held)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, okA := a.Path(context.Background(), b.Key(), b.Addr())
		_, okB := b.Path(context.Background(), a.Key(), a.Addr())
		if !okA && !okB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Path after close: a %v, b %v; want both unknown", okA, okB)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestTraffic(t *testing.T) {
	t.Parallel()
	dm := derp(t)
	a, b := newT(t, dm, Options{}), newT(t, dm, Options{})
	const n = 256 << 10
	got := make(chan int64, 1)
	b.Handle(api.PortSyncthing, func(c net.Conn, _ api.NodeKey) {
		defer c.Close()
		m, _ := io.Copy(io.Discard, io.LimitReader(c, n))
		c.Write([]byte("ok"))
		got <- m
	})
	a.Allow(b.Key())
	b.Allow(a.Key())

	if _, ok := a.Traffic(b.Key()); ok {
		t.Error("traffic before any connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := a.Dial(ctx, b.Addr(), api.PortSyncthing)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	if m := <-got; m != n {
		t.Fatalf("b read %d bytes, want %d", m, n)
	}
	// Totals are exact; rates need one sample to measure against.
	time.Sleep(rateTick + rateTick/2)
	ta, ok := a.Traffic(b.Key())
	if !ok || ta.OutTotal != n || ta.InTotal != 2 {
		t.Errorf("a's traffic with b = %+v, %v; want %d out, 2 in", ta, ok, n)
	}
	tb, ok := b.Traffic(a.Key())
	if !ok || tb.InTotal != n || tb.OutTotal != 2 {
		t.Errorf("b's traffic with a = %+v, %v; want %d in, 2 out", tb, ok, n)
	}
	// Everything moved before the first sample, so the rate over the
	// window may be zero now, but never negative.
	if ta.OutRate < 0 || tb.InRate < 0 {
		t.Errorf("negative rates: %+v %+v", ta, tb)
	}
}

func TestTrafficRate(t *testing.T) {
	var tr peerTraffic
	t0 := time.Now()
	tr.record(t0)
	tr.in.Add(5000)
	tr.out.Add(1000)
	r := tr.report(t0.Add(2 * time.Second))
	if r.InRate != 2500 || r.OutRate != 500 || r.InTotal != 5000 || r.OutTotal != 1000 {
		t.Errorf("report = %+v, want 2500/500 B/s", r)
	}
	// Samples older than the window are dropped, keeping the newest.
	for i := 1; i <= 15; i++ {
		tr.record(t0.Add(time.Duration(i) * time.Second))
	}
	if span := tr.samples[len(tr.samples)-1].at.Sub(tr.samples[0].at); span > rateWindow {
		t.Errorf("samples span %v, want at most %v", span, rateWindow)
	}
	// Nothing moved in the window: zero rate.
	if r := tr.report(t0.Add(16 * time.Second)); r.InRate != 0 || r.OutRate != 0 {
		t.Errorf("idle report = %+v, want zero rates", r)
	}
}
