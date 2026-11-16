package syncer

import (
	"context"
	"fmt"
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
	dir    string // synced folder
	stDir  string // Syncer.Config.Dir
	name   string
	mu     sync.Mutex
	events []api.SyncEvent
}

func newNode(t *testing.T, n *fake.Network, name string) *node {
	t.Helper()
	nd := &node{t: t, tr: n.Transport(name), dir: t.TempDir(), stDir: t.TempDir(), name: name}
	nd.start()
	return nd
}

// start runs a Syncer on the node's directories, as node run does.
func (n *node) start() {
	t := n.t
	t.Helper()
	s, err := New(Config{
		Dir:               n.stDir,
		Name:              n.name,
		Transport:         n.tr,
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
