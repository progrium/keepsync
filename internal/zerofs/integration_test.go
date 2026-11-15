package zerofs

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRealZeroFS runs the real zerofs binary against an S3-compatible bucket
// (MinIO in practice): write, flush, stop, wipe the local cache, start again
// and read back from the bucket. It is skipped unless these are set:
//
//	KEEPSYNC_ZEROFS_BIN       path to zerofs (the pinned release)
//	KEEPSYNC_TEST_S3_ENDPOINT e.g. http://127.0.0.1:9000
//	KEEPSYNC_TEST_S3_BUCKET   an existing, empty bucket
//	KEEPSYNC_TEST_S3_KEY / KEEPSYNC_TEST_S3_SECRET
//
// Optional: KEEPSYNC_ZEROFS_MOUNT (default "fuse").
func TestRealZeroFS(t *testing.T) {
	bin := os.Getenv("KEEPSYNC_ZEROFS_BIN")
	endpoint := os.Getenv("KEEPSYNC_TEST_S3_ENDPOINT")
	bucket := os.Getenv("KEEPSYNC_TEST_S3_BUCKET")
	if bin == "" || endpoint == "" || bucket == "" {
		t.Skip("set KEEPSYNC_ZEROFS_BIN and KEEPSYNC_TEST_S3_* to run against real zerofs")
	}
	mode := MountMode(os.Getenv("KEEPSYNC_ZEROFS_MOUNT"))
	if mode == "" {
		mode = MountFUSE
	}
	pw, err := GeneratePassword()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "zfsit")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := Config{
		Binary:  bin,
		DataDir: filepath.Join(dir, "data"),
		Storage: Storage{
			URL:             "s3://" + bucket + "/zerofs-it-" + time.Now().Format("20060102-150405"),
			Endpoint:        endpoint,
			Region:          "us-east-1",
			AccessKeyID:     os.Getenv("KEEPSYNC_TEST_S3_KEY"),
			SecretAccessKey: os.Getenv("KEEPSYNC_TEST_S3_SECRET"),
			AllowHTTP:       true,
		},
		Password:     pw,
		Mount:        mode,
		CacheSizeGB:  1,
		StartTimeout: time.Minute,
		Output:       testWriter{t},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop(context.Background()) })

	data := make([]byte, 4<<20)
	rand.Read(data)
	name := filepath.Join(s.Dir(), "packs", "pack-00000.blobs")
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if m, _ := isMountPoint(s.Dir()); m {
		t.Fatal("still mounted after Stop")
	}

	// Drop the local cache so the read has to come from the bucket.
	if err := os.RemoveAll(filepath.Join(cfg.DataDir, "cache")); err != nil {
		t.Fatal(err)
	}
	s2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Stop(context.Background()) })
	got, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes that differ from the %d written", len(got), len(data))
	}
	if err := s2.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("zerofs: %s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}
