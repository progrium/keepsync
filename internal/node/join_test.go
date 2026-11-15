package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

func TestJoinRefusesNonEmptyDirWithoutMerge(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	dir := t.TempDir()
	// Syncthing's and keepsync's own files don't make a directory non-empty.
	os.WriteFile(filepath.Join(dir, ".stignore"), nil, 0o644)
	os.Mkdir(filepath.Join(dir, ".stfolder"), 0o755)
	if err := tn.checkJoinDir(FolderParams{Name: "photos", Path: dir}); err != nil {
		t.Fatalf("empty dir refused: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "a.jpg"), []byte("x"), 0o644)
	err := tn.checkJoinDir(FolderParams{Name: "photos", Path: dir})
	if err == nil || !strings.Contains(err.Error(), "--merge") {
		t.Fatalf("non-empty dir without --merge: %v", err)
	}
	if err := tn.checkJoinDir(FolderParams{Name: "photos", Path: dir, Merge: true}); err != nil {
		t.Fatalf("--merge refused: %v", err)
	}
	// Rejoining the folder already attached at the same path is fine.
	if err := tn.checkJoinDir(FolderParams{Name: "docs", Path: tn.dir}); err != nil {
		t.Fatalf("rejoin refused: %v", err)
	}
	if err := tn.checkJoinDir(FolderParams{Name: "new", Path: filepath.Join(dir, "missing")}); err != nil {
		t.Fatalf("new dir refused: %v", err)
	}
}
