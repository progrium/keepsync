package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/progrium/keepsync/internal/config"
)

func TestZeroFSCacheLimit(t *testing.T) {
	const gib = 1 << 30
	for _, c := range []struct {
		gb   float64
		want int64
	}{
		{0.5, 2 * gib},                     // both floors
		{2, 2 * gib},                       // still both floors
		{10, gib + (10e9 - gib)},           // metadata floor
		{100, 10e9 + 90e9},                 // a tenth for metadata
		{1000, 16*gib + (1000e9 - 16*gib)}, // metadata cap
	} {
		if got := zerofsCacheLimit(c.gb); got != c.want {
			t.Errorf("%g GB: got %d, want %d", c.gb, got, c.want)
		}
	}
}

func TestDirSizeSkipsMount(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "a"), 0o700)
	os.MkdirAll(filepath.Join(root, "mnt"), 0o700)
	os.WriteFile(filepath.Join(root, "a", "f"), make([]byte, 64<<10), 0o600)
	os.WriteFile(filepath.Join(root, "mnt", "huge"), make([]byte, 1<<20), 0o600)
	n, err := dirSize(root, filepath.Join(root, "mnt"))
	if err != nil {
		t.Fatal(err)
	}
	if n < 64<<10 || n >= 1<<20 {
		t.Fatalf("dirSize = %d, want about 64 KiB without the mount", n)
	}
}

func TestBucketSizeFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "x"), make([]byte, 100), 0o600)
	os.WriteFile(filepath.Join(dir, "y"), make([]byte, 50), 0o600)
	b, n, err := bucketSize(context.Background(), nil, config.Storage{URL: "file://" + dir})
	if err != nil || b != 150 || n != 2 {
		t.Fatalf("got %d bytes, %d objects, %v", b, n, err)
	}
}

func TestBucketSizeS3Pages(t *testing.T) {
	pages := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bkt" || r.URL.Query().Get("prefix") != "keep/" {
			http.Error(w, "bad request "+r.URL.String(), 400)
			return
		}
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			http.Error(w, "unsigned", 403)
			return
		}
		pages++
		if r.URL.Query().Get("continuation-token") == "" {
			w.Write([]byte(`<ListBucketResult><Contents><Key>a</Key><Size>10</Size></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>t2</NextContinuationToken></ListBucketResult>`))
			return
		}
		w.Write([]byte(`<ListBucketResult><Contents><Key>b</Key><Size>5</Size></Contents><Contents><Key>c</Key><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer srv.Close()
	b, n, err := bucketSize(context.Background(), srv.Client(), config.Storage{
		URL: "s3://bkt/keep", Endpoint: srv.URL, Region: "us-east-1", AccessKeyID: "k", SecretAccessKey: "s",
	})
	if err != nil || b != 16 || n != 3 || pages != 2 {
		t.Fatalf("got %d bytes, %d objects, %d pages, %v", b, n, pages, err)
	}
}
