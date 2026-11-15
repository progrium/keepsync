package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/registry"
)

func TestFileStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := &FileStore{Path: filepath.Join(dir, "sub", "registry.json"), Mirror: filepath.Join(dir, "mnt", ".keepsync", "registry.json")}
	st, err := s.Load(ctx)
	if err != nil || st.Version != 0 || len(st.Nodes) != 0 {
		t.Fatalf("empty load: %+v %v", st, err)
	}
	want := registry.State{Version: 3, Nodes: []registry.NodeRecord{{Node: api.Node{Name: "a", Key: "nodekey:aa"}, Registered: true}},
		Folders: []api.Folder{{Name: "docs", ID: "x", Members: []api.NodeKey{"nodekey:aa"}}}}
	if err := s.Save(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := (&FileStore{Path: s.Path}).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := os.ReadFile(s.Path)
	b, err := os.ReadFile(s.Mirror)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("mirror differs: %v", err)
	}
	os.Remove(s.Mirror)
	if err := s.SyncMirror(); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(s.Mirror); !bytes.Equal(a, b) {
		t.Fatal("SyncMirror did not restore the mirror")
	}
	if got.Version != 3 || got.Nodes[0].Name != "a" || !got.Nodes[0].Registered || got.Folders[0].Members[0] != "nodekey:aa" {
		t.Fatalf("got %+v", got)
	}
}
