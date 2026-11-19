package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/progrium/keepsync/internal/config"
)

// fakeS3 is a path-style S3 for one bucket. fail maps a method to an S3
// error code it answers with.
type fakeS3 struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	fail    map[string]string
	methods []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.methods = append(f.methods, r.Method)
	if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		http.Error(w, "unsigned", http.StatusForbidden)
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket != f.bucket {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>`)
		return
	}
	if code := f.fail[r.Method]; code != "" {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `<Error><Code>`+code+`</Code><Message>no</Message></Error>`)
		return
	}
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.objects[key] = b
	case http.MethodGet:
		b, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		w.Write(b)
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	}
}

func checkFake(t *testing.T, f *fakeS3, bucketURL string, allowHTTP bool) error {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return CheckBucket(context.Background(), srv.Client(), config.Storage{
		URL: bucketURL, Endpoint: srv.URL, Region: "us-east-1",
		AccessKeyID: "k", SecretAccessKey: "s", AllowHTTP: allowHTTP,
	})
}

func TestCheckBucketOK(t *testing.T) {
	f := &fakeS3{bucket: "bkt", objects: map[string][]byte{}}
	if err := checkFake(t, f, "s3://bkt/keepsync", true); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.methods, ",") != "PUT,GET,DELETE" {
		t.Errorf("requests %v", f.methods)
	}
	if len(f.objects) != 0 {
		t.Errorf("left objects behind: %v", f.objects)
	}
}

// TestCheckBucketMissing: issue #6's case, a bucket that doesn't exist,
// reported as S3 reports it.
func TestCheckBucketMissing(t *testing.T) {
	f := &fakeS3{bucket: "bkt", objects: map[string][]byte{}}
	err := checkFake(t, f, "s3://ks-trial/keepsync", true)
	if err == nil || !strings.Contains(err.Error(), "NoSuchBucket: The specified bucket does not exist") || !strings.Contains(err.Error(), "s3://ks-trial/keepsync/.keepsync-init-check-") {
		t.Fatalf("got %v", err)
	}
}

// TestCheckBucketReadDeniedStillDeletes: a failed read is reported, and the
// probe object is still deleted.
func TestCheckBucketReadDeniedStillDeletes(t *testing.T) {
	f := &fakeS3{bucket: "bkt", objects: map[string][]byte{}, fail: map[string]string{http.MethodGet: "AccessDenied"}}
	err := checkFake(t, f, "s3://bkt/keepsync", true)
	if err == nil || !strings.Contains(err.Error(), "read") || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("got %v", err)
	}
	if len(f.objects) != 0 {
		t.Errorf("left objects behind: %v", f.objects)
	}
}

func TestCheckBucketPlainHTTPNeedsFlag(t *testing.T) {
	f := &fakeS3{bucket: "bkt", objects: map[string][]byte{}}
	err := checkFake(t, f, "s3://bkt/keepsync", false)
	if err == nil || !strings.Contains(err.Error(), "--allow-http") {
		t.Fatalf("got %v", err)
	}
	if len(f.methods) != 0 {
		t.Errorf("sent requests: %v", f.methods)
	}
}

func TestCheckBucketSkips(t *testing.T) {
	for _, st := range []config.Storage{
		{URL: "file:///nonexistent/keepsync"},
		{URL: "s3://bkt/keepsync", Endpoint: "https://127.0.0.1:1"}, // no keys: ZeroFS's own credential chain
	} {
		if err := CheckBucket(context.Background(), http.DefaultClient, st); err != nil {
			t.Errorf("%+v: %v", st, err)
		}
	}
}
