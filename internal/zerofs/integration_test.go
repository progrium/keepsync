package zerofs

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/files"
	"perkeep.org/pkg/blobserver/storagetest"
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

// TestRealZeroFSNineP runs Perkeep's blob storage conformance tests on a
// files.Storage over NineP against the real zerofs, with the same
// variables as TestRealZeroFS. zerofs's 9P differs from the p9 package's
// own server in ways the unit tests can't see (partial walks, directory
// entry types).
func TestRealZeroFSNineP(t *testing.T) {
	bin := os.Getenv("KEEPSYNC_ZEROFS_BIN")
	endpoint := os.Getenv("KEEPSYNC_TEST_S3_ENDPOINT")
	bucket := os.Getenv("KEEPSYNC_TEST_S3_BUCKET")
	if bin == "" || endpoint == "" || bucket == "" {
		t.Skip("set KEEPSYNC_ZEROFS_BIN and KEEPSYNC_TEST_S3_* to run against real zerofs")
	}
	pw, err := GeneratePassword()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "zfs9p")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s, err := New(Config{
		Binary:  bin,
		DataDir: filepath.Join(dir, "data"),
		Storage: Storage{
			URL:             "s3://" + bucket + "/zerofs-9p-" + time.Now().Format("20060102-150405"),
			Endpoint:        endpoint,
			Region:          "us-east-1",
			AccessKeyID:     os.Getenv("KEEPSYNC_TEST_S3_KEY"),
			SecretAccessKey: os.Getenv("KEEPSYNC_TEST_S3_SECRET"),
			AllowHTTP:       true,
		},
		Password:     pw,
		Mount:        MountNineP,
		StartTimeout: time.Minute,
		Output:       testWriter{t},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop(context.Background()) })
	fs := s.FS()
	i := 0
	storagetest.Test(t, func(t *testing.T) blobserver.Storage {
		i++
		root := fmt.Sprintf("/blobs%d", i)
		if err := fs.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		return files.NewStorage(fs, root)
	})
	if _, err := fs.Stat("no/such/deep/path"); !os.IsNotExist(err) {
		t.Errorf("stat of a missing deep path: %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestRealZeroFSReconnect kills the real zerofs under a 9p supervisor,
// twice, and checks that the supervisor restarts it and redials the same
// client, which then reads back what was flushed and takes new writes.
// Same variables as TestRealZeroFS.
func TestRealZeroFSReconnect(t *testing.T) {
	bin := os.Getenv("KEEPSYNC_ZEROFS_BIN")
	endpoint := os.Getenv("KEEPSYNC_TEST_S3_ENDPOINT")
	bucket := os.Getenv("KEEPSYNC_TEST_S3_BUCKET")
	if bin == "" || endpoint == "" || bucket == "" {
		t.Skip("set KEEPSYNC_ZEROFS_BIN and KEEPSYNC_TEST_S3_* to run against real zerofs")
	}
	pw, _ := GeneratePassword()
	dir, err := os.MkdirTemp("", "zfsrc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s, err := New(Config{
		Binary:  bin,
		DataDir: filepath.Join(dir, "data"),
		Storage: Storage{
			URL:             "s3://" + bucket + "/zerofs-reconnect-" + time.Now().Format("20060102-150405"),
			Endpoint:        endpoint,
			Region:          "us-east-1",
			AccessKeyID:     os.Getenv("KEEPSYNC_TEST_S3_KEY"),
			SecretAccessKey: os.Getenv("KEEPSYNC_TEST_S3_SECRET"),
			AllowHTTP:       true,
		},
		Password:      pw,
		Mount:         MountNineP,
		CheckInterval: 200 * time.Millisecond,
		StartTimeout:  time.Minute,
		Output:        testWriter{t},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop(context.Background()) })
	fs := s.FS()
	sto := files.NewStorage(fs, "/blobs")
	fs.MkdirAll("blobs", 0o700)

	var refs []blob.Ref
	put := func(i int) {
		t.Helper()
		b := []byte(fmt.Sprintf("blob %d %s", i, time.Now()))
		br := blob.RefFromBytes(b)
		if _, err := blobserver.Receive(ctx, sto, br, bytes.NewReader(b)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		refs = append(refs, br)
	}
	for round := range 2 {
		for i := range 20 {
			put(round*100 + i)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		pid := s.proc.Process.Pid
		s.mu.Unlock()
		t0 := time.Now()
		syscall.Kill(pid, syscall.SIGKILL)
		deadline := time.Now().Add(2 * time.Minute)
		for {
			s.mu.Lock()
			newPid := 0
			if s.proc != nil {
				newPid = s.proc.Process.Pid
			}
			s.mu.Unlock()
			if newPid != pid && s.Health().State == HealthUp {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: not back after kill: %+v", round, s.Health())
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Logf("round %d: zerofs back %v after SIGKILL", round, time.Since(t0).Round(time.Millisecond))
		for _, br := range refs {
			rc, _, err := sto.Fetch(ctx, br)
			if err != nil {
				t.Fatalf("round %d: fetch %s after reconnect: %v", round, br, err)
			}
			rc.Close()
		}
	}
	put(999)
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
