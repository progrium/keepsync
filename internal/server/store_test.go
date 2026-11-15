package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/registry"
)

func TestFileStore(t *testing.T) {
	ctx := context.Background()
	s := &FileStore{Path: filepath.Join(t.TempDir(), "sub", "registry.json")}
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
	if got.Version != 3 || got.Nodes[0].Name != "a" || !got.Nodes[0].Registered || got.Folders[0].Members[0] != "nodekey:aa" {
		t.Fatalf("got %+v", got)
	}
}
