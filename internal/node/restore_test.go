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
	st, _, err := restoreFile(ctx, sto, api.BlobRef(ref.String()), 0, dst, false)
	if err != nil || st != "restored" {
		t.Fatalf("%s %v", st, err)
	}
	b, _ := os.ReadFile(dst)
	fi, _ := os.Stat(dst)
	if string(b) != "hello" || !fi.ModTime().Equal(mt) {
		t.Fatalf("got %q %v", b, fi.ModTime())
	}
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), 0, dst, false); st != "unchanged" {
		t.Fatalf("second restore: %s", st)
	}
	os.WriteFile(dst, []byte("local edit"), 0o644)
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), 0, dst, false); st != "skipped" {
		t.Fatalf("differing file: %s", st)
	}
	if st, _, _ := restoreFile(ctx, sto, api.BlobRef(ref.String()), 0, dst, true); st != "restored" {
		t.Fatalf("force: %s", st)
	}
	if b, _ := os.ReadFile(dst); string(b) != "hello" {
		t.Fatalf("force wrote %q", b)
	}
}

func TestRestoreFileMode(t *testing.T) {
	ctx := context.Background()
	sto := new(memory.Storage)
	ref, err := schema.WriteFileFromReaderWithModTime(ctx, sto, "run.sh", time.Now(), strings.NewReader("#!/bin/sh\n"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, tc := range []struct {
		mode, want os.FileMode
	}{{0o755, 0o755}, {0o600, 0o600}, {0, 0o644}} {
		dst := filepath.Join(dir, tc.want.String())
		if _, _, err := restoreFile(ctx, sto, api.BlobRef(ref.String()), tc.mode, dst, false); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(dst); fi.Mode().Perm() != tc.want {
			t.Errorf("mode %v restored as %v, want %v", tc.mode, fi.Mode().Perm(), tc.want)
		}
	}
}
