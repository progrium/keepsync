package ctl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type addParams struct{ A, B int }

func TestCall(t *testing.T) {
	// Unix socket paths are short-limited; t.TempDir can be long.
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "s")
	ctx := context.Background()

	if err := Call(ctx, sock, "add", nil, nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("want ErrNotRunning, got %v", err)
	}

	s := NewServer()
	HandleFunc(s, "add", func(ctx context.Context, p addParams) (any, error) { return p.A + p.B, nil })
	HandleFunc(s, "fail", func(ctx context.Context, p struct{}) (any, error) { return nil, errors.New("boom") })
	if err := s.Listen(sock); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := NewServer().Listen(sock); err == nil {
		t.Fatal("second listener should fail")
	}

	var n int
	if err := Call(ctx, sock, "add", addParams{2, 3}, &n); err != nil || n != 5 {
		t.Fatalf("add: %v %v", n, err)
	}
	if err := Call(ctx, sock, "fail", nil, nil); err == nil || err.Error() != "boom" {
		t.Fatalf("fail: %v", err)
	}
	if err := Call(ctx, sock, "nope", nil, nil); err == nil || !strings.Contains(err.Error(), "unknown method") {
		t.Fatalf("nope: %v", err)
	}
}
