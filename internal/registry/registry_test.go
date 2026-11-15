package registry

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

const (
	keeperTimeout = 50 * time.Millisecond
	pingInterval  = 10 * time.Millisecond
	idleTimeout   = 100 * time.Millisecond
)

type env struct {
	t     *testing.T
	net   *fake.Network
	srvT  *fake.Transport
	pk    *fake.Perkeep
	store *MemStore
	srv   *Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, net: fake.NewNetwork(), pk: fake.NewPerkeep(), store: &MemStore{}}
	e.srvT = e.net.Transport("server")
	e.start()
	return e
}

func (e *env) start() {
	e.t.Helper()
	srv, err := NewServer(context.Background(), Config{
		Store: e.store, Signer: e.pk, Durability: e.pk,
		Transport: e.srvT, KeeperTimeout: keeperTimeout,
		PingInterval: pingInterval, IdleTimeout: idleTimeout,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := srv.Serve(); err != nil {
		e.t.Fatal(err)
	}
	e.srv = srv
	e.t.Cleanup(func() { srv.Close() })
}

type node struct {
	t *fake.Transport
	c *Client
}

// node approves a new node on the server and returns a registered client.
func (e *env) node(name string) *node {
	e.t.Helper()
	tr := e.net.Transport(name)
	if err := e.srv.Add(context.Background(), tr.Key(), name); err != nil {
		e.t.Fatal(err)
	}
	n := &node{t: tr, c: NewClient(tr, e.srvT.Addr())}
	n.c.RetryMin, n.c.RetryMax = 5*time.Millisecond, 20*time.Millisecond
	n.c.IdleTimeout = idleTimeout
	e.t.Cleanup(func() { n.c.Close() })
	if _, err := n.c.Register(context.Background(), api.Node{Name: name, Addr: tr.Addr(), Device: api.DeviceID("dev-" + name)}); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// eventually polls cond until it holds or the test times out.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func folderOf(m api.Membership, name string) *api.Folder {
	for i := range m.Folders {
		if m.Folders[i].Name == name {
			return &m.Folders[i]
		}
	}
	return nil
}

func keepersOf(e *env, folder string) []api.NodeKey {
	f := folderOf(e.srv.Membership(), folder)
	if f == nil || f.Keep == nil {
		return nil
	}
	return f.Keep.Keepers
}

func policy() api.KeepPolicy {
	return api.KeepPolicy{Grace: 48 * time.Hour, MaxDefer: 7 * 24 * time.Hour, Expire: 30 * 24 * time.Hour}
}

func TestRegisterAndAllowList(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)

	stranger := e.net.Transport("stranger")
	if _, err := NewClient(stranger, e.srvT.Addr()).Register(ctx, api.Node{Name: "x"}); err == nil {
		t.Fatal("unapproved node registered")
	}

	a := e.node("a")
	m, err := a.c.Register(ctx, api.Node{Name: "ignored", Key: "nodekey:spoofed", Addr: a.t.Addr(), Device: "dev-a2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Nodes) != 1 {
		t.Fatalf("nodes = %+v", m.Nodes)
	}
	got := m.Nodes[0]
	if got.Key != a.t.Key() || got.Name != "a" || got.Device != "dev-a2" || got.Addr != a.t.Addr() {
		t.Fatalf("node = %+v (key must come from transport, admin name must win)", got)
	}

	// Approved but unregistered nodes are listed for the admin but not published.
	pending := e.net.Transport("pending")
	e.srv.Add(ctx, pending.Key(), "")
	if n := len(e.srv.Membership().Nodes); n != 1 {
		t.Fatalf("published %d nodes, want 1", n)
	}
	if n := len(e.srv.Nodes()); n != 2 {
		t.Fatalf("admin sees %d nodes, want 2", n)
	}
	// Without an admin name, the node's own name is used.
	if _, err := NewClient(pending, e.srvT.Addr()).Register(ctx, api.Node{Name: "laptop", Addr: pending.Addr()}); err != nil {
		t.Fatal(err)
	}
	if n := e.srv.Membership().Nodes[1]; n.Name != "laptop" {
		t.Fatalf("name = %q", n.Name)
	}
}

func TestWatch(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a := e.node("a")

	ch, err := a.c.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := <-ch
	if len(first.Nodes) != 1 {
		t.Fatalf("first = %+v", first)
	}
	e.node("b")
	for m := range ch {
		if len(m.Nodes) == 2 {
			if m.Version <= first.Version {
				t.Fatalf("version did not advance: %d -> %d", first.Version, m.Version)
			}
			break
		}
	}

	// The watch survives a server restart and sees state loaded from the store.
	before := e.srv.Membership().Version
	e.srv.Close()
	e.start()
	if _, err := a.c.AddFolder(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	for m := range ch {
		if folderOf(m, "docs") != nil {
			if m.Version <= before || len(m.Nodes) != 2 {
				t.Fatalf("after restart: %+v", m)
			}
			break
		}
	}

	wctx, cancel := context.WithCancel(ctx)
	ch2, err := a.c.Watch(wctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range ch2 {
	}
}

func TestFolders(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b := e.node("a"), e.node("b")

	f, err := a.c.AddFolder(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	if f.ID == "" || len(f.Members) != 1 || f.Members[0] != a.t.Key() {
		t.Fatalf("folder = %+v", f)
	}
	if _, err := b.c.AddFolder(ctx, "docs"); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate add: %v", err)
	}
	if _, err := b.c.JoinFolder(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("join missing: %v", err)
	}
	for i := 0; i < 2; i++ {
		j, err := b.c.JoinFolder(ctx, "docs")
		if err != nil {
			t.Fatal(err)
		}
		if j.ID != f.ID || len(j.Members) != 2 {
			t.Fatalf("join %d = %+v", i, j)
		}
	}
	if _, err := a.c.AddFolder(ctx, " "); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("blank name: %v", err)
	}
}

func TestKeep(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")

	bad := []api.KeepPolicy{
		{Grace: 2 * time.Hour, MaxDefer: time.Hour},
		{Grace: time.Hour, MaxDefer: 2 * time.Hour, Expire: 2 * time.Hour},
		{Grace: -1, MaxDefer: time.Hour},
	}
	for _, p := range bad {
		if _, err := a.c.Keep(ctx, "docs", p); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("policy %+v: %v", p, err)
		}
	}
	if _, err := c.c.Keep(ctx, "docs", policy()); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member keep: %v", err)
	}

	pn, err := a.c.Keep(ctx, "docs", policy())
	if err != nil {
		t.Fatal(err)
	}
	f := folderOf(e.srv.Membership(), "docs")
	if f.Keep == nil || f.Keep.Permanode != pn || len(f.Keep.Keepers) != 2 {
		t.Fatalf("keep = %+v", f.Keep)
	}

	// Again, with new timings and a requested permanode and keepers: the
	// permanode and keepers stay, the timings change.
	p := api.KeepPolicy{Grace: time.Hour, MaxDefer: 2 * time.Hour, Permanode: "sha224-other", Keepers: []api.NodeKey{c.t.Key()}}
	pn2, err := b.c.Keep(ctx, "docs", p)
	if err != nil {
		t.Fatal(err)
	}
	f = folderOf(e.srv.Membership(), "docs")
	if pn2 != pn || f.Keep.Grace != time.Hour || f.Keep.Expire != 0 || contains(f.Keep.Keepers, c.t.Key()) {
		t.Fatalf("rekeep = %s %+v", pn2, f.Keep)
	}

	// A folder can adopt an existing permanode (from its .keepsync file),
	// but not one that already keeps another folder.
	c.c.AddFolder(ctx, "photos")
	if _, err := c.c.Keep(ctx, "photos", api.KeepPolicy{Grace: 1, MaxDefer: 2, Permanode: pn}); !errors.Is(err, ErrExists) {
		t.Fatalf("shared permanode: %v", err)
	}
	got, err := c.c.Keep(ctx, "photos", api.KeepPolicy{Grace: 1, MaxDefer: 2, Permanode: "sha224-adopted"})
	if err != nil || got != "sha224-adopted" {
		t.Fatalf("adopt = %s, %v", got, err)
	}
}

func TestClaimsAndDurability(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b := e.node("a"), e.node("b")
	a.c.AddFolder(ctx, "docs")
	pn, err := a.c.Keep(ctx, "docs", policy())
	if err != nil {
		t.Fatal(err)
	}

	t0 := time.Unix(1_700_000_000, 0).UTC()
	claim := api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-1", ModTime: t0}
	steps := []struct {
		c    api.PathClaim
		want api.ClaimResult
	}{
		{claim, api.ClaimWritten},
		{claim, api.ClaimUnchanged},
		{api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-0", ModTime: t0.Add(-time.Hour)}, api.ClaimRejectedOlder},
		{api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-2", ModTime: t0.Add(time.Hour)}, api.ClaimWritten},
	}
	for i, s := range steps {
		got, err := a.c.ClaimPath(ctx, s.c)
		if err != nil || got != s.want {
			t.Fatalf("step %d: %v, %v; want %v", i, got, err, s.want)
		}
	}
	paths, err := a.c.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	if e := paths["a.txt"]; e.File != "sha224-2" || !e.ModTime.Equal(t0.Add(time.Hour)) {
		t.Fatalf("paths = %+v", paths)
	}
	// Every PathEntry field, Claim included, survives the wire.
	direct, err := e.pk.Paths(ctx, pn)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := paths["a.txt"], direct["a.txt"]; got.Claim == "" || got.Claim != want.Claim || got.File != want.File || !got.ModTime.Equal(want.ModTime) {
		t.Fatalf("over the wire %+v, direct %+v", got, want)
	}
	if e.pk.Claims != 2 {
		t.Fatalf("claims written = %d", e.pk.Claims)
	}

	if _, err := b.c.ClaimPath(ctx, claim); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member claim: %v", err)
	}
	if _, err := b.c.Paths(ctx, pn); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member paths: %v", err)
	}
	if _, err := a.c.ClaimPath(ctx, api.PathClaim{Permanode: "sha224-nope", Path: "x", File: "sha224-1"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown permanode: %v", err)
	}

	refs := []api.BlobRef{pn, "sha224-missing"}
	d, err := a.c.Durable(ctx, refs)
	if err != nil || d[0] || d[1] {
		t.Fatalf("durable before flush = %v, %v", d, err)
	}
	e.pk.MarkDurable()
	d, err = b.c.Durable(ctx, refs)
	if err != nil || !d[0] || d[1] {
		t.Fatalf("durable after flush = %v, %v", d, err)
	}
}

func TestKeeperReassignment(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")
	c.c.JoinFolder(ctx, "docs")
	if _, err := a.c.Keep(ctx, "docs", policy()); err != nil {
		t.Fatal(err)
	}
	k := keepersOf(e, "docs")
	if len(k) != 2 || k[0] != a.t.Key() || k[1] != b.t.Key() {
		t.Fatalf("keepers = %v", k)
	}

	// Watchers see the reassignment.
	ch, err := c.c.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// a drops off. Its keeping moves to c only after the timeout; b, the
	// healthy keeper, moves up to run the conflict hook.
	dropped := time.Now()
	a.c.Close()
	want := []api.NodeKey{b.t.Key(), c.t.Key()}
	for m := range ch {
		if f := folderOf(m, "docs"); f != nil && equalKeys(f.Keep.Keepers, want) {
			break
		}
	}
	if since := time.Since(dropped); since < keeperTimeout {
		t.Fatalf("reassigned after %s, before the %s timeout", since, keeperTimeout)
	}

	// A brief reconnect inside the timeout changes nothing.
	b.c.Close()
	b2 := NewClient(b.t, e.srvT.Addr())
	defer b2.Close()
	if _, err := b2.Register(ctx, api.Node{Addr: b.t.Addr()}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * keeperTimeout)
	if k := keepersOf(e, "docs"); !equalKeys(k, want) {
		t.Fatalf("keepers after reconnect = %v", k)
	}

	// With no connected candidate, a stale keeper is kept rather than dropped.
	c.c.Close()
	time.Sleep(2 * keeperTimeout)
	e.srv.Reconcile(ctx)
	if k := keepersOf(e, "docs"); len(k) != 2 || !contains(k, c.t.Key()) {
		t.Fatalf("keepers with nobody to take over = %v", k)
	}

	// When a comes back it fills the slot c left.
	a2 := NewClient(a.t, e.srvT.Addr())
	defer a2.Close()
	if _, err := a2.Register(ctx, api.Node{Addr: a.t.Addr()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a to replace c", func() bool {
		return equalKeys(keepersOf(e, "docs"), []api.NodeKey{b.t.Key(), a.t.Key()})
	})
}

func TestRemove(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")
	c.c.JoinFolder(ctx, "docs")
	a.c.Keep(ctx, "docs", policy())

	if err := e.srv.Remove(ctx, "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("remove unknown: %v", err)
	}
	if err := e.srv.Remove(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	m := e.srv.Membership()
	f := folderOf(m, "docs")
	if len(m.Nodes) != 2 || contains(f.Members, a.t.Key()) || !equalKeys(f.Keep.Keepers, []api.NodeKey{b.t.Key(), c.t.Key()}) {
		t.Fatalf("after remove: nodes=%v folder=%+v keep=%+v", m.Nodes, f, f.Keep)
	}
	if _, err := a.c.AddFolder(ctx, "x"); err == nil {
		t.Fatal("removed node can still call")
	}
	if _, err := NewClient(a.t, e.srvT.Addr()).Register(ctx, api.Node{}); err == nil {
		t.Fatal("removed node can still dial")
	}
}

func TestReceiveOnlyKeepersLast(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")
	c.c.JoinFolder(ctx, "docs")
	if err := e.srv.SetReceiveOnly(ctx, "docs", b.t.Key(), true); err != nil {
		t.Fatal(err)
	}
	a.c.Keep(ctx, "docs", policy())
	if k := keepersOf(e, "docs"); !equalKeys(k, []api.NodeKey{a.t.Key(), c.t.Key()}) {
		t.Fatalf("keepers = %v, want a and c over receive-only b", k)
	}
	if f := folderOf(e.srv.Membership(), "docs"); !equalKeys(f.ReceiveOnly, []api.NodeKey{b.t.Key()}) {
		t.Fatalf("receive-only = %v", f.ReceiveOnly)
	}
	if err := e.srv.SetReceiveOnly(ctx, "docs", "nodekey:nobody", true); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member receive-only: %v", err)
	}
}

func TestStateIsSavedBeforePublish(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a := e.node("a")
	a.c.AddFolder(ctx, "docs")
	saved, _ := e.store.Load(ctx)
	if saved.Version != e.srv.Membership().Version || saved.folder("docs") == nil {
		t.Fatalf("saved = %+v", saved)
	}
	n := e.store.Saves
	a.c.JoinFolder(ctx, "docs") // no change
	a.c.Register(ctx, api.Node{Name: "a", Addr: a.t.Addr(), Device: "dev-a"})
	if e.store.Saves != n {
		t.Fatalf("no-op calls saved state %d times", e.store.Saves-n)
	}
}

func TestValidatePolicy(t *testing.T) {
	ok := []api.KeepPolicy{
		policy(),
		{Grace: 0, MaxDefer: time.Hour},
		{Grace: time.Hour, MaxDefer: 2 * time.Hour, Expire: 3 * time.Hour},
	}
	for _, p := range ok {
		if err := ValidatePolicy(p); err != nil {
			t.Errorf("%+v: %v", p, err)
		}
	}
}

func TestFolderID(t *testing.T) {
	id, err := newFolderID()
	if err != nil || len(id) != 11 || id[5] != '-' {
		t.Fatalf("id = %q, %v", id, err)
	}
}

func TestLeaveFolder(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")
	c.c.JoinFolder(ctx, "docs")
	e.srv.SetReceiveOnly(ctx, "docs", c.t.Key(), true)
	if _, err := a.c.Keep(ctx, "docs", policy()); err != nil {
		t.Fatal(err)
	}
	if k := keepersOf(e, "docs"); !equalKeys(k, []api.NodeKey{a.t.Key(), b.t.Key()}) {
		t.Fatalf("keepers = %v", k)
	}
	ch, err := a.c.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	<-ch

	// b leaves: c, though receive-only, is the only one left to keep.
	if err := b.c.LeaveFolder(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	want := []api.NodeKey{a.t.Key(), c.t.Key()}
	for m := range ch {
		f := folderOf(m, "docs")
		if !contains(f.Members, b.t.Key()) {
			if !equalKeys(f.Keep.Keepers, want) {
				t.Fatalf("keepers after b left = %v, want %v", f.Keep.Keepers, want)
			}
			break
		}
	}

	// Leaving again, or as a non-member, is a no-op.
	saves := e.store.Saves
	if err := b.c.LeaveFolder(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	if e.store.Saves != saves {
		t.Fatal("no-op leave saved state")
	}
	if err := b.c.LeaveFolder(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("leave missing folder: %v", err)
	}

	// c leaves: it drops out of receive-only too, and a keeps alone.
	if err := c.c.LeaveFolder(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	f := folderOf(e.srv.Membership(), "docs")
	if !equalKeys(f.Members, []api.NodeKey{a.t.Key()}) || len(f.ReceiveOnly) != 0 || !equalKeys(f.Keep.Keepers, []api.NodeKey{a.t.Key()}) {
		t.Fatalf("after c left: %+v keep=%+v", f, f.Keep)
	}

	// The last member leaving keeps the folder and its archive.
	a.c.LeaveFolder(ctx, "docs")
	f = folderOf(e.srv.Membership(), "docs")
	if f == nil || f.Keep == nil || len(f.Members) != 0 || len(f.Keep.Keepers) != 0 {
		t.Fatalf("after everyone left: %+v", f)
	}
	// Rejoining brings keeping back.
	b.c.JoinFolder(ctx, "docs")
	if k := keepersOf(e, "docs"); !equalKeys(k, []api.NodeKey{b.t.Key()}) {
		t.Fatalf("keepers after rejoin = %v", k)
	}
}

func online(e *env, key api.NodeKey) bool {
	for _, n := range e.srv.Nodes() {
		if n.Key == key {
			return n.Online
		}
	}
	return false
}

// A node whose connection is open but which has stopped reading and writing
// (a half-dead tailcat connection) goes offline after the idle timeout, and
// its keeping moves on after the keeper timeout.
func TestHeartbeatDropsSilentNode(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b, c := e.node("a"), e.node("b"), e.node("c")
	a.c.AddFolder(ctx, "docs")
	b.c.JoinFolder(ctx, "docs")
	c.c.JoinFolder(ctx, "docs")
	a.c.Keep(ctx, "docs", policy())

	// A healthy idle client stays online across many ping intervals.
	time.Sleep(3 * idleTimeout)
	if !online(e, b.t.Key()) {
		t.Fatal("idle but healthy client was dropped")
	}

	// a's real client goes away and a raw connection that asks to watch,
	// then never reads or writes again, takes its place.
	a.c.Close()
	raw, err := a.t.Dial(ctx, e.srvT.Addr(), api.PortRegistry)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write([]byte(`{"id":1,"method":"watch"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the silent connection to be counted", func() bool { return online(e, a.t.Key()) })
	silent := time.Now()

	eventually(t, "a to go offline", func() bool { return !online(e, a.t.Key()) })
	if since := time.Since(silent); since < idleTimeout/2 {
		t.Fatalf("dropped after %s, well before the %s idle timeout", since, idleTimeout)
	}
	eventually(t, "c to take a's keeping", func() bool {
		return equalKeys(keepersOf(e, "docs"), []api.NodeKey{b.t.Key(), c.t.Key()})
	})
}

// A client gives up on a server that stops answering instead of hanging:
// calls fail and Watch redials.
func TestClientDropsSilentServer(t *testing.T) {
	ctx := ctx(t)
	nw := fake.NewNetwork()
	mute, node := nw.Transport("mute"), nw.Transport("node")
	mute.Allow(node.Key())
	dials := make(chan struct{}, 100)
	mute.Handle(api.PortRegistry, func(conn net.Conn, peer api.NodeKey) {
		dials <- struct{}{}
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
	c := NewClient(node, mute.Addr())
	c.IdleTimeout, c.RetryMin, c.RetryMax = idleTimeout, 5*time.Millisecond, 20*time.Millisecond
	defer c.Close()

	start := time.Now()
	if _, err := c.Durable(ctx, nil); err == nil {
		t.Fatal("call to a mute server succeeded")
	}
	if since := time.Since(start); since > 20*idleTimeout {
		t.Fatalf("call took %s to fail", since)
	}

	if _, err := c.Watch(ctx); err != nil {
		t.Fatal(err)
	}
	// One dial for the call, then the watch's first dial and two redials.
	for i := 0; i < 4; i++ {
		select {
		case <-dials:
		case <-ctx.Done():
			t.Fatalf("watch did not redial a mute server (%d dials)", i)
		}
	}
}

func TestPathsAt(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a, b := e.node("a"), e.node("b")
	a.c.AddFolder(ctx, "docs")
	pn, err := a.c.Keep(ctx, "docs", policy())
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	if _, err := a.c.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-1", ModTime: t0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	mid := time.Now()
	time.Sleep(5 * time.Millisecond)
	a.c.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-2", ModTime: t0.Add(time.Hour)})
	a.c.ClaimPath(ctx, api.PathClaim{Permanode: pn, Path: "b.txt", File: "sha224-3", ModTime: t0})

	then, err := a.c.PathsAt(ctx, pn, mid)
	if err != nil {
		t.Fatal(err)
	}
	if got := then["a.txt"]; got.File != "sha224-1" || got.Claim == "" {
		t.Fatalf("a.txt at mid = %+v", got)
	}
	if _, ok := then["b.txt"]; ok {
		t.Fatal("b.txt present before it was claimed")
	}
	now, _ := a.c.PathsAt(ctx, pn, time.Now())
	if now["a.txt"].File != "sha224-2" || now["b.txt"].File != "sha224-3" {
		t.Fatalf("paths now = %+v", now)
	}
	if _, err := b.c.PathsAt(ctx, pn, mid); !errors.Is(err, ErrNotMember) {
		t.Fatalf("non-member PathsAt: %v", err)
	}
}

func TestGrantScope(t *testing.T) {
	e := newEnv(t)
	ctx := ctx(t)
	a := e.node("a")
	key := a.t.Key()

	if got := e.srv.Scope(key); got != ScopeBlobs {
		t.Fatalf("default scope %q, want %q", got, ScopeBlobs)
	}
	if got := e.srv.Scope("nodekey:unknown"); got != "" {
		t.Fatalf("unapproved node scope %q, want empty", got)
	}
	if err := e.srv.Grant(ctx, "a", ScopeRead); err != nil {
		t.Fatal(err)
	}
	if got := e.srv.Scope(key); got != ScopeRead {
		t.Fatalf("after grant: %q", got)
	}
	for _, bad := range []string{"write", "owner", ""} {
		if err := e.srv.Grant(ctx, "a", bad); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("grant %q: %v, want ErrBadRequest", bad, err)
		}
	}
	if err := e.srv.Grant(ctx, "nobody", ScopeRead); !errors.Is(err, ErrNotFound) {
		t.Fatalf("grant unknown: %v", err)
	}

	// The scope is persisted, and survives a restart.
	e.srv.Close()
	e.start()
	if got := e.srv.Scope(key); got != ScopeRead {
		t.Fatalf("after restart: %q", got)
	}
	var found bool
	for _, n := range e.srv.Nodes() {
		if n.Key == key {
			found = n.Scope == ScopeRead
		}
	}
	if !found {
		t.Fatal("Nodes() does not report the read scope")
	}
	if err := e.srv.Grant(ctx, string(key), ScopeBlobs); err != nil {
		t.Fatal(err)
	}
	if got := e.srv.Scope(key); got != ScopeBlobs {
		t.Fatalf("back to blobs: %q", got)
	}
}
