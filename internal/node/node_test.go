package node

import (
	"context"
	"testing"

	"github.com/progrium/keepsync/internal/config"
)

func TestResolveServerAddr(t *testing.T) {
	a, err := ResolveServer(context.Background(), " tcpGFwWCDJ ")
	if err != nil || a != "tcpGFwWCDJ" {
		t.Fatalf("%q %v", a, err)
	}
	if _, err := ResolveServer(context.Background(), ""); err == nil {
		t.Fatal("want error")
	}
}

func TestInitOffline(t *testing.T) {
	dir := t.TempDir()
	res, err := Init(context.Background(), InitOptions{Dir: dir, Name: "laptop", Server: "tcXYZ"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Config.Name != "laptop" || res.Device == "" || len(res.Key) < 10 {
		t.Fatalf("%+v", res)
	}
	cfg, err := config.LoadNode(dir)
	if err != nil || cfg.Server != "tcXYZ" {
		t.Fatalf("%+v %v", cfg, err)
	}
	if _, err := Init(context.Background(), InitOptions{Dir: dir, Server: "tcXYZ"}); err == nil {
		t.Fatal("second init should fail")
	}
}
