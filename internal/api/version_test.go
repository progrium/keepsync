package api

import (
	"testing"
	"time"
)

func v(s string) Version {
	x, err := ParseVersion(s)
	if err != nil {
		panic(err)
	}
	return x
}

func TestVersionCompare(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want Ordering
	}{
		{"", "", Equal},
		{"A:1", "A:1", Equal},
		{"A:2", "A:1", Newer},
		{"A:1", "A:2", Older},
		{"A:1,B:1", "A:1", Newer},
		{"A:1", "A:1,B:1", Older},
		{"A:2", "A:1,B:1", Concurrent},
		{"A:1,B:0", "A:1", Equal},
	} {
		if got := v(c.a).Compare(v(c.b)); got != c.want {
			t.Errorf("%q vs %q = %v, want %v", c.a, c.b, got, c.want)
		}
	}
	if s := v("B:2,A:1").String(); s != "A:1,B:2" {
		t.Errorf("String = %q", s)
	}
	if _, err := ParseVersion("A1"); err == nil {
		t.Error("bad version parsed")
	}
}

func TestSupersedes(t *testing.T) {
	t0 := time.Unix(1000, 0)
	old := PathEntry{File: "f1", ModTime: t0, Version: v("A:1")}
	// A newer version wins even with an older mtime (recreated, restored).
	if !Supersedes(PathClaim{File: "f2", ModTime: t0.Add(-time.Hour), Version: v("A:2")}, old) {
		t.Error("newer version with older mtime rejected")
	}
	// An older version loses even with a newer mtime (fast clock, lagging).
	if Supersedes(PathClaim{File: "f2", ModTime: t0.Add(time.Hour), Version: v("A:0")}, PathEntry{File: "f1", ModTime: t0, Version: v("A:1,B:1")}) {
		t.Error("older version accepted")
	}
	// Concurrent versions fall back to mtime.
	if !Supersedes(PathClaim{File: "f2", ModTime: t0.Add(time.Second), Version: v("B:1")}, old) {
		t.Error("concurrent, newer mtime rejected")
	}
	// No version on either side: the mtime rule.
	if Supersedes(PathClaim{File: "f2", ModTime: t0}, PathEntry{File: "f1", ModTime: t0}) {
		t.Error("same mtime without versions accepted")
	}
}
