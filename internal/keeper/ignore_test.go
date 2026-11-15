package keeper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPatterns(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "more.stignore"), []byte("*.bak\n"), 0o644)
	p, err := ParsePatterns([]string{
		"// comment",
		"",
		"!important.log",
		"*.log",
		"/root-only",
		"node_modules",
		"docs/**/draft*",
		"(?i)thumbs.db",
		"(?d).DS_Store",
		"a?c",
		"[xy].tmp",
		"#include more.stignore",
	}, dir)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		rel  string
		want bool
	}{
		{"x.log", true},
		{"deep/dir/x.log", true},
		{"important.log", false},
		{"sub/important.log", false},
		{"root-only", true},
		{"root-only/file", true},
		{"sub/root-only", false},
		{"node_modules", true},
		{"a/node_modules/pkg/index.js", true},
		{"docs/draft1.md", true},
		{"docs/x/y/draft.md", true},
		{"other/draft.md", false},
		{"Thumbs.DB", true},
		{".DS_Store", true},
		{"abc", true},
		{"abbc", false},
		{"x.tmp", true},
		{"z.tmp", false},
		{"file.bak", true},
		{"notes.txt", false},
		{"log", false},
	}
	for _, tt := range tests {
		if got := p.Excluded(tt.rel, false); got != tt.want {
			t.Errorf("Excluded(%q) = %v, want %v", tt.rel, got, tt.want)
		}
	}
}

func TestAlwaysExcluded(t *testing.T) {
	for rel, want := range map[string]bool{
		".keepsync":              true,
		".stignore":              true,
		".keepignore":            true,
		"sub/.keepsync":          false,
		".syncthing.a.txt.tmp":   true,
		"d/~syncthing~a.txt.tmp": true,
		".keepsync-123.tmp":      true,
		"a.txt":                  false,
	} {
		if got := alwaysExcluded(rel, false); got != want {
			t.Errorf("alwaysExcluded(%q) = %v", rel, got)
		}
	}
	if !alwaysExcluded("x/.stversions", true) || !alwaysExcluded(".stfolder", true) {
		t.Error("Syncthing dirs not excluded")
	}
}
