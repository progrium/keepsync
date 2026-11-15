package syncer

import (
	"context"
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
	t      *testing.T
	s      *Syncer
	tr     *fake.Transport
	dir    string
	mu     sync.Mutex
	events []api.SyncEvent
}

func newNode(t *testing.T, n *fake.Network, name string) *node {
	t.Helper()
	tr := n.Transport(name)
	s, err := New(Config{
		Dir:               t.TempDir(),
		Name:              name,
		Transport:         tr,
		DisableWatcher:    true,
		RescanInterval:    time.Hour,
		ReconnectInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	nd := &node{t: t, s: s, tr: tr, dir: t.TempDir()}
	go func() {
		for ev := range s.Events() {
			nd.mu.Lock()
			nd.events = append(nd.events, ev)
			nd.mu.Unlock()
		}
	}()
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return nd
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
	a = newNode(t, net, "a")
	b = newNode(t, net, "b")
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
