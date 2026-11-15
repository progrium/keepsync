package fake

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

func TestTransportAllowList(t *testing.T) {
	n := NewNetwork()
	a, b := n.Transport("a"), n.Transport("b")
	b.Handle(api.PortRegistry, func(c net.Conn, peer api.NodeKey) {
		io.WriteString(c, string(peer))
		c.Close()
	})
	if _, err := a.Dial(context.Background(), b.Addr(), api.PortRegistry); err == nil {
		t.Fatal("dial allowed before Allow")
	}
	b.Allow(a.Key())
	c, err := a.Dial(context.Background(), b.Addr(), api.PortRegistry)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(c)
	if string(got) != string(a.Key()) {
		t.Fatalf("peer = %q", got)
	}
}

func TestClaimRules(t *testing.T) {
	ctx := context.Background()
	pk := NewPerkeep()
	pn, _ := pk.NewPermanode(ctx, "f")
	t0 := time.Unix(1000, 0)
	c := api.PathClaim{Permanode: pn, Path: "a.txt", File: "sha224-1", ModTime: t0}
	if r, _ := pk.ClaimPath(ctx, c); r != api.ClaimWritten {
		t.Fatalf("first claim = %v", r)
	}
	if r, _ := pk.ClaimPath(ctx, c); r != api.ClaimUnchanged {
		t.Fatalf("same claim = %v", r)
	}
	c.File = "sha224-0"
	if r, _ := pk.ClaimPath(ctx, c); r != api.ClaimRejectedOlder {
		t.Fatalf("not-newer claim = %v", r)
	}
	c.ModTime = t0.Add(time.Second)
	if r, _ := pk.ClaimPath(ctx, c); r != api.ClaimWritten {
		t.Fatalf("newer claim = %v", r)
	}
}
