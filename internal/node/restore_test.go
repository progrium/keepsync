package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"perkeep.org/pkg/blobserver/memory"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
)

func TestRestoreFile(t *testing.T) {
	ctx := context.Background()
	sto := new(memory.Storage)
	mt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, sto, "a.txt", mt, strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "sub", "a.txt")
	st, _, err := restoreFile(ctx, sto, api.BlobRef(ref.String()), dst, false)
	if err != nil || st != "restored" {
		t.Fatalf("%s %v", st, err)
	}
	b, _ := os.ReadFile(dst)
	fi, _ := os.Stat(dst)
	if string(b) != "hello" || !fi.ModTime().Equal(mt) {
		t.Fatalf("got %q %v", b, fi.ModTime())
	}
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), dst, false); st != "unchanged" {
		t.Fatalf("second restore: %s", st)
	}
	os.WriteFile(dst, []byte("local edit"), 0o644)
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), dst, false); st != "skipped" {
		t.Fatalf("differing file: %s", st)
	}
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), dst, true); st != "restored" {
		t.Fatalf("force: %s", st)
	}
	if b, _ := os.ReadFile(dst); string(b) != "hello" {
		t.Fatalf("force wrote %q", b)
	}
}
