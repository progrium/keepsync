package pkserver

import (
	"context"
	"testing"
	"time"

	"golang.org/x/text/unicode/norm"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
)

const accented = "naïve-文件.txt"

// TestNFCAndNFDAreOnePath: a name kept in NFC on one node and in NFD on a
// macOS node is one kept file, at its NFC name (issue #10).
func TestNFCAndNFDAreOnePath(t *testing.T) {
	ctx := context.Background()
	k := newKeptEnv(t)
	nfc, nfd := norm.NFC.String(accented), norm.NFD.String(accented)
	if nfc == nfd {
		t.Fatal("test name has one spelling")
	}
	t0 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	f1, f2 := k.put(), k.put()
	if r := k.claim(nfc, f1, t0, ver("A", 1)); r != api.ClaimWritten {
		t.Fatal(r)
	}
	if r := k.claim("dïr/"+nfd, f1, t0, ver("A", 1)); r != api.ClaimWritten {
		t.Fatal(r)
	}
	// The macOS node's newer version, in NFD, updates the same file.
	if r := k.claim(nfd, f2, t0.Add(time.Hour), ver("A", 1, "B", 1)); r != api.ClaimWritten {
		t.Fatal(r)
	}
	paths := k.paths()
	if len(paths) != 2 || paths[nfc].File != f2 || paths[norm.NFC.String("dïr/"+accented)].File != f1 {
		t.Fatalf("paths: %v", keys(paths))
	}
	h, err := k.srv.History(ctx, k.root, nfd)
	if err != nil || len(h) != 2 {
		t.Fatalf("history by NFD name: %v, %v", h, err)
	}
}

// TestLegacySpellingsFold: an archive kept before paths were normalized can
// name one file in both spellings. They read as one path, the newest kept,
// and a new claim updates it.
func TestLegacySpellingsFold(t *testing.T) {
	ctx := context.Background()
	k := newKeptEnv(t)
	nfc, nfd := norm.NFC.String(accented), norm.NFD.String(accented)
	t0 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	older, newer := k.put(), k.put()
	k.legacyClaim(nfd, older, t0, ver("A", 1))
	k.legacyClaim(nfc, newer, t0.Add(time.Hour), ver("A", 1, "B", 1))
	paths := k.paths()
	if len(paths) != 1 || paths[nfc].File != newer {
		t.Fatalf("paths: %v (%v)", keys(paths), paths)
	}
	latest := k.put()
	if r := k.claim(nfd, latest, t0.Add(2*time.Hour), ver("A", 2, "B", 1)); r != api.ClaimWritten {
		t.Fatal(r)
	}
	k.srv.treeMu.Lock()
	delete(k.srv.trees, k.root) // reload from the claims
	k.srv.treeMu.Unlock()
	if paths := k.paths(); len(paths) != 1 || paths[nfc].File != latest {
		t.Fatalf("after a new claim: %v (%v)", keys(paths), paths)
	}
	if _, err := k.srv.History(ctx, k.root, nfc); err != nil {
		t.Fatal(err)
	}
}

// legacyClaim names a new file at the root by name exactly as given, the
// way the server did before it normalized paths, and drops the cached tree.
func (k *keptEnv) legacyClaim(name string, file api.BlobRef, mt time.Time, v api.Version) {
	k.t.Helper()
	ctx := context.Background()
	root, ok := blob.Parse(string(k.root))
	ref, ok2 := blob.Parse(string(file))
	if !ok || !ok2 {
		k.t.Fatal("bad refs")
	}
	k.srv.claimMu.Lock()
	defer k.srv.claimMu.Unlock()
	pn, err := k.srv.signAndUpload(ctx, schema.NewUnsignedPermanode())
	if err != nil {
		k.t.Fatal(err)
	}
	ops := contentOps(pn, ref, api.PathClaim{Permanode: k.root, Path: name, File: file, ModTime: mt, Version: v}, nil)
	ops = append(ops, schema.NewSetAttributeClaim(root, pathAttrPrefix+name, pn.String()))
	multi, err := schema.NewMultiClaim(ops...)
	if err != nil {
		k.t.Fatal(err)
	}
	if _, _, err := k.srv.signClaim(ctx, multi, time.Time{}); err != nil {
		k.t.Fatal(err)
	}
	k.srv.treeMu.Lock()
	delete(k.srv.trees, k.root)
	k.srv.treeMu.Unlock()
}

func keys(m map[string]api.PathEntry) []string {
	var out []string
	for p := range m {
		out = append(out, p)
	}
	return out
}
