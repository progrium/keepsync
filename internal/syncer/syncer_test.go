package syncer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

type node struct {
	t  *testing.T
	s  *Syncer
	tr *fake.Transport
	// via, if set, is the Transport the Syncer uses in place of tr.
	via    api.Transport
	dir    string // synced folder
	stDir  string // Syncer.Config.Dir
	name   string
	mu     sync.Mutex
	events []api.SyncEvent
}

func newNode(t *testing.T, n *fake.Network, name string) *node {
	t.Helper()
	return newNodeOn(t, n.Transport(name), nil, name)
}

func newNodeOn(t *testing.T, tr *fake.Transport, via api.Transport, name string) *node {
	t.Helper()
	nd := &node{t: t, tr: tr, via: via, dir: t.TempDir(), stDir: t.TempDir(), name: name}
	nd.start()
	return nd
}

// start runs a Syncer on the node's directories, as node run does.
func (n *node) start() {
	t := n.t
	t.Helper()
	var tr api.Transport = n.tr
	if n.via != nil {
		tr = n.via
	}
	s, err := New(Config{
		Dir:               n.stDir,
		Name:              n.name,
		Transport:         tr,
		DisableWatcher:    true,
		RescanInterval:    time.Hour,
		ReconnectInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.s = s
	n.events = nil
	n.mu.Unlock()
	go func() {
		for ev := range s.Events() {
			n.mu.Lock()
			n.events = append(n.events, ev)
			n.mu.Unlock()
		}
	}()
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.stop(s) })
}

func (n *node) stop(s *Syncer) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		n.t.Error(err)
	}
}

func (n *node) peer(name string) api.Peer {
	return api.Peer{Name: name, Key: n.tr.Key(), Addr: n.tr.Addr(), Device: n.s.DeviceID()}
}

func (n *node) hasEvent(f func(api.SyncEvent) bool) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ev := range n.events {
		if f(ev) {
			return true
		}
	}
	return false
}

func (n *node) write(rel, content string) {
	n.t.Helper()
	p := filepath.Join(n.dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		n.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		n.t.Fatal(err)
	}
}

func (n *node) scan(folder string) {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.s.Scan(ctx, folder, ""); err != nil {
		n.t.Fatal(err)
	}
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fileIs(path, want string) func() bool {
	return func() bool {
		b, err := os.ReadFile(path)
		return err == nil && string(b) == want
	}
}

func pair(t *testing.T) (a, b *node) {
	t.Helper()
	net := fake.NewNetwork()
	return connect(t, newNode(t, net, "a"), newNode(t, net, "b"))
}

// connect makes a and b peers sharing folder docs, and waits until they
// are connected.
func connect(t *testing.T, a, b *node) (*node, *node) {
	t.Helper()
	ctx := context.Background()
	if err := a.s.SetPeers(ctx, []api.Peer{b.peer("b")}); err != nil {
		t.Fatal(err)
	}
	if err := b.s.SetPeers(ctx, []api.Peer{a.peer("a")}); err != nil {
		t.Fatal(err)
	}
	devs := []api.DeviceID{a.s.DeviceID(), b.s.DeviceID()}
	if err := a.s.SetFolders(ctx, []api.FolderConfig{{ID: "docs", Path: a.dir, Devices: devs}}); err != nil {
		t.Fatal(err)
	}
	if err := b.s.SetFolders(ctx, []api.FolderConfig{{ID: "docs", Path: b.dir, Devices: devs}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "peers connected", func() bool {
		return a.hasEvent(func(ev api.SyncEvent) bool {
			return ev.Type == api.SyncPeerConnected && ev.Device == b.s.DeviceID()
		}) && b.hasEvent(func(ev api.SyncEvent) bool {
			return ev.Type == api.SyncPeerConnected && ev.Device == a.s.DeviceID()
		})
	})
	return a, b
}

func TestTwoNodesSync(t *testing.T) {
	a, b := pair(t)

	a.write("notes/hello.txt", "hello from a")
	a.scan("docs")
	eventually(t, "file on b", fileIs(filepath.Join(b.dir, "notes/hello.txt"), "hello from a"))
	eventually(t, "item event on b", func() bool {
		return b.hasEvent(func(ev api.SyncEvent) bool {
			return ev.Type == api.SyncItemChanged && ev.Folder == "docs" &&
				ev.Path == "notes/hello.txt" && ev.Device == a.s.DeviceID()
		})
	})
	if !a.hasEvent(func(ev api.SyncEvent) bool {
		return ev.Type == api.SyncLocalChange && ev.Path == "notes/hello.txt" && ev.Device == a.s.DeviceID()
	}) {
		t.Error("no local change event on a")
	}

	// And back the other way.
	b.write("notes/hello.txt", "edited on b")
	b.scan("docs")
	eventually(t, "edit on a", fileIs(filepath.Join(a.dir, "notes/hello.txt"), "edited on b"))
}

func TestConflict(t *testing.T) {
	a, b := pair(t)
	ctx := context.Background()

	a.write("doc.txt", "base")
	a.scan("docs")
	eventually(t, "base on b", fileIs(filepath.Join(b.dir, "doc.txt"), "base"))

	// Partition, edit both sides, heal.
	if err := a.s.SetPeers(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.s.SetPeers(ctx, nil); err != nil {
		t.Fatal(err)
	}
	a.write("doc.txt", "version a")
	b.write("doc.txt", "version b!")
	a.scan("docs")
	b.scan("docs")
	if err := a.s.SetPeers(ctx, []api.Peer{b.peer("b")}); err != nil {
		t.Fatal(err)
	}
	if err := b.s.SetPeers(ctx, []api.Peer{a.peer("a")}); err != nil {
		t.Fatal(err)
	}
	devs := []api.DeviceID{a.s.DeviceID(), b.s.DeviceID()}
	for _, n := range []*node{a, b} {
		if err := n.s.SetFolders(ctx, []api.FolderConfig{{ID: "docs", Path: n.dir, Devices: devs}}); err != nil {
			t.Fatal(err)
		}
	}

	conflictOn := func(n *node) bool {
		ents, _ := os.ReadDir(n.dir)
		for _, e := range ents {
			if IsConflict(e.Name()) {
				return true
			}
		}
		return false
	}
	eventually(t, "conflict copy", func() bool {
		return conflictOn(a) || conflictOn(b)
	})
	eventually(t, "conflict event", func() bool {
		a.scan("docs")
		b.scan("docs")
		for _, n := range []*node{a, b} {
			if n.hasEvent(func(ev api.SyncEvent) bool {
				orig, _ := ConflictOriginal(ev.Path)
				return ev.Type == api.SyncConflict && ev.Folder == "docs" && orig == "doc.txt" && ev.Device != ""
			}) {
				return true
			}
		}
		return false
	})
}

func TestRefusesUnknownPeer(t *testing.T) {
	net := fake.NewNetwork()
	a := newNode(t, net, "a")
	stranger := net.Transport("stranger")
	a.tr.Allow(stranger.Key()) // allowed on the transport, but not a peer
	conn, err := stranger.Dial(context.Background(), a.tr.Addr(), api.PortSyncthing)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the bridge to close a non-peer connection")
	}
}

func TestDeviceIDStable(t *testing.T) {
	dir := t.TempDir()
	tr := fake.NewNetwork().Transport("x")
	s1, err := New(Config{Dir: dir, Transport: tr})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := New(Config{Dir: dir, Transport: tr})
	if err != nil {
		t.Fatal(err)
	}
	if s1.DeviceID() != s2.DeviceID() || !strings.Contains(string(s1.DeviceID()), "-") {
		t.Fatalf("device IDs %q, %q", s1.DeviceID(), s2.DeviceID())
	}
}

func TestConflictNames(t *testing.T) {
	cases := []struct {
		path, short, orig string
		ok                bool
	}{
		{"a/b.sync-conflict-20260101-120000-ABCDEFG.txt", "ABCDEFG", "a/b.txt", true},
		{"noext.sync-conflict-20260101-120000-ZZZZ999", "ZZZZ999", "noext", true},
		{"a/b.txt", "", "", false},
		{"sync-conflict.txt", "", "", false},
	}
	for _, c := range cases {
		short, ok := ConflictDevice(c.path)
		orig, _ := ConflictOriginal(c.path)
		if ok != c.ok || short != c.short || (ok && orig != c.orig) {
			t.Errorf("%s: got %q %q %v", c.path, short, orig, ok)
		}
	}
}

// A restarted node must keep its index, or its files come back as new
// versions and a delete elsewhere turns into a conflict copy (which would
// resurrect every expired file).
func TestRestartKeepsIndex(t *testing.T) {
	a, b := pair(t)
	ctx := context.Background()
	devs := []api.DeviceID{a.s.DeviceID(), b.s.DeviceID()}

	for i := range 5 {
		a.write(fmt.Sprintf("old/%d.txt", i), fmt.Sprintf("file %d", i))
	}
	a.scan("docs")
	for i := range 5 {
		eventually(t, "file on b", fileIs(filepath.Join(b.dir, fmt.Sprintf("old/%d.txt", i)), fmt.Sprintf("file %d", i)))
	}

	// Restart b the way node run does: Start first, then peers and folders
	// once the registry answers.
	id := b.s.DeviceID()
	b.stop(b.s)
	time.Sleep(500 * time.Millisecond)
	b.start()
	if b.s.DeviceID() != id {
		t.Fatalf("device ID changed across restart: %s -> %s", id, b.s.DeviceID())
	}
	time.Sleep(500 * time.Millisecond)
	if err := b.s.SetPeers(ctx, []api.Peer{a.peer("a")}); err != nil {
		t.Fatal(err)
	}
	if err := b.s.SetFolders(ctx, []api.FolderConfig{{ID: "docs", Path: b.dir, Devices: devs}}); err != nil {
		t.Fatal(err)
	}
	b.scan("docs")
	eventually(t, "reconnect", func() bool {
		return b.hasEvent(func(ev api.SyncEvent) bool {
			return ev.Type == api.SyncPeerConnected && ev.Device == a.s.DeviceID()
		})
	})

	// Expiry on a: delete everything.
	if err := os.RemoveAll(filepath.Join(a.dir, "old")); err != nil {
		t.Fatal(err)
	}
	a.scan("docs")
	eventually(t, "deletes on b", func() bool {
		for i := range 5 {
			if _, err := os.Stat(filepath.Join(b.dir, fmt.Sprintf("old/%d.txt", i))); err == nil {
				return false
			}
		}
		return true
	})
	// Give any resurrection a chance to happen, then check both sides.
	time.Sleep(2 * time.Second)
	a.scan("docs")
	b.scan("docs")
	time.Sleep(2 * time.Second)
	for _, n := range []*node{a, b} {
		filepath.WalkDir(n.dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && (IsConflict(p) || strings.Contains(p, "old/")) {
				t.Errorf("%s: unexpected %s after delete", n.name, p)
			}
			return nil
		})
	}
}

func TestFolderSync(t *testing.T) {
	a, b := pair(t)
	a.write("one.txt", "1111")
	a.write("sub/two.txt", "22")
	a.scan("docs")
	eventually(t, "file on b", fileIs(filepath.Join(b.dir, "sub/two.txt"), "22"))

	for _, n := range []*node{a, b} {
		other := b
		if n == b {
			other = a
		}
		var fs api.FolderSync
		eventually(t, n.name+" sees both up to date", func() bool {
			var err error
			fs, err = n.s.FolderSync("docs")
			if err != nil || !fs.Local.UpToDate() || len(fs.Peers) != 1 {
				return false
			}
			p := fs.Peers[0]
			return p.Known && p.UpToDate()
		})
		if fs.Local.GlobalBytes != 6 || fs.Local.Pct != 100 {
			t.Errorf("%s local = %+v, want 6 bytes, 100%%", n.name, fs.Local)
		}
		if fs.Peers[0].Device != other.s.DeviceID() {
			t.Errorf("%s peer = %v, want %v", n.name, fs.Peers[0].Device, other.s.DeviceID())
		}
		if fs.State == "" || fs.State == "error" {
			t.Errorf("%s state = %q (%s)", n.name, fs.State, fs.Error)
		}
		files, total, err := n.s.NeedFiles("docs", 10)
		if err != nil || total != 0 || len(files) != 0 {
			t.Errorf("%s NeedFiles = %v, %d, %v; want none", n.name, files, total, err)
		}
	}
	if _, err := a.s.FolderSync("nope"); err == nil {
		t.Error("FolderSync of an unknown folder succeeded")
	}
}

// inboundFrom is how many Syncthing connections n has open from peer.
func (n *node) inboundFrom(peer *node) int {
	n.s.mu.Lock()
	defer n.s.mu.Unlock()
	if in := n.s.inbound[peer.tr.Key()]; in != nil {
		return in.active
	}
	return 0
}

// lowHigh orders a and b by node key: low is the one that dials.
func lowHigh(a, b *node) (low, high *node) {
	if a.tr.Key() < b.tr.Key() {
		return a, b
	}
	return b, a
}

func TestOneTunnelPerPair(t *testing.T) {
	a, b := pair(t)
	low, high := lowHigh(a, b)
	eventually(t, "low dialed high", func() bool { return high.inboundFrom(low) > 0 })
	// The high node never dials while low's connections are up.
	for range 20 {
		if n := low.inboundFrom(high); n != 0 {
			t.Fatalf("high dialed low: %d inbound connections on low", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
	a.write("x.txt", "x")
	a.scan("docs")
	eventually(t, "file on b", fileIs(filepath.Join(b.dir, "x.txt"), "x"))
}

// noDial is a Transport whose Dial always fails, like a node that cannot
// reach its peer.
type noDial struct{ api.Transport }

func (noDial) Dial(context.Context, api.Addr, uint16) (net.Conn, error) {
	return nil, errors.New("unreachable")
}

func TestDialFallback(t *testing.T) {
	old := dialFallback
	dialFallback = 2 * time.Second
	t.Cleanup(func() { dialFallback = old })

	nw := fake.NewNetwork()
	ta, tb := nw.Transport("a"), nw.Transport("b")
	// Whichever has the lower key is the dialer; make its dials fail.
	var viaA, viaB api.Transport
	if ta.Key() < tb.Key() {
		viaA = noDial{ta}
	} else {
		viaB = noDial{tb}
	}
	a, b := connect(t, newNodeOn(t, ta, viaA, "a"), newNodeOn(t, tb, viaB, "b"))
	low, high := lowHigh(a, b)
	if n := low.inboundFrom(high); n == 0 {
		t.Errorf("connected, but not by high dialing low")
	}
	a.write("y.txt", "y")
	a.scan("docs")
	eventually(t, "file on b", fileIs(filepath.Join(b.dir, "y.txt"), "y"))
}

// TestFolderRemoveAndRejoin checks that SetFolders really removes a folder
// (Syncthing's own SetFolders only adds and replaces, so folder rm used to
// leave it syncing, and joining again at another path then restarted it
// there on the old index: "folder marker missing"), and that adding it back,
// at the same path or another, starts it cleanly with a fresh marker.
func TestFolderRemoveAndRejoin(t *testing.T) {
	a, b := pair(t)
	a.write("keep.txt", "k")
	a.scan("docs")
	eventually(t, "file on b", fileIs(filepath.Join(b.dir, "keep.txt"), "k"))

	ctx := context.Background()
	devs := []api.DeviceID{a.s.DeviceID(), b.s.DeviceID()}
	marker := func(dir string) bool {
		_, err := os.Stat(filepath.Join(dir, ".stfolder"))
		return err == nil
	}
	runsAt := func(dir string) {
		t.Helper()
		if err := b.s.SetFolders(ctx, []api.FolderConfig{{ID: "docs", Path: dir, Devices: devs}}); err != nil {
			t.Fatal(err)
		}
		eventually(t, "docs running at "+dir, func() bool {
			fs, err := b.s.FolderSync("docs")
			return err == nil && fs.State != "error" && !strings.HasPrefix(fs.State, "scan") && marker(dir)
		})
		eventually(t, "keep.txt at "+dir, fileIs(filepath.Join(dir, "keep.txt"), "k"))
	}
	remove := func(dir string) {
		t.Helper()
		if err := b.s.SetFolders(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := b.s.FolderSync("docs"); err == nil {
			t.Fatal("folder still configured after SetFolders(nil)")
		}
		eventually(t, "marker removed", func() bool { return !marker(dir) })
	}

	remove(b.dir)
	runsAt(b.dir) // rejoin at the same path
	remove(b.dir)
	moved := filepath.Join(t.TempDir(), "moved")
	runsAt(moved) // rejoin elsewhere: pulls the folder again
	if !fileIs(filepath.Join(b.dir, "keep.txt"), "k")() {
		t.Error("the old directory's files changed")
	}
}
