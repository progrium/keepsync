package pkserver

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/search"

	"github.com/progrium/keepsync/internal/api"
)

// Attributes on a kept folder's permanode. camliPath:<path> is Perkeep's
// own; mtimeAttrPrefix+<path> holds the mtime the path's current file was
// claimed with, set in the same multi claim, so the newer-mtime rule needs
// only the index and survives a reindex from the bucket.
const (
	pathAttrPrefix  = "camliPath:"
	mtimeAttrPrefix = "keepsyncMtime:"
	kindAttr        = "keepsyncKind"
	kindFolder      = "folder"
)

// NewPermanode creates a permanode for a kept folder, titled title.
func (s *Server) NewPermanode(ctx context.Context, title string) (api.BlobRef, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	pn, err := s.signAndUpload(ctx, schema.NewUnsignedPermanode())
	if err != nil {
		return "", err
	}
	multi, err := schema.NewMultiClaim(
		schema.NewSetAttributeClaim(pn, "title", title),
		schema.NewSetAttributeClaim(pn, kindAttr, kindFolder),
	)
	if err != nil {
		return "", err
	}
	multi.SetMultiClaimPermanode(pn)
	if _, err := s.signAndUpload(ctx, multi); err != nil {
		return "", err
	}
	return api.BlobRef(pn.String()), nil
}

// ClaimPath sets camliPath:<c.Path> on c.Permanode to c.File. It writes
// nothing if the path already points at c.File, and rejects a claim whose
// ModTime is not newer than the current entry's.
func (s *Server) ClaimPath(ctx context.Context, c api.PathClaim) (api.ClaimResult, error) {
	pn, ok := blob.Parse(string(c.Permanode))
	if !ok {
		return 0, fmt.Errorf("pkserver: bad permanode %q", c.Permanode)
	}
	file, ok := blob.Parse(string(c.File))
	if !ok {
		return 0, fmt.Errorf("pkserver: bad file ref %q", c.File)
	}
	if c.Path == "" {
		return 0, fmt.Errorf("pkserver: empty path")
	}
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.treeMu.Lock()
	tree, err := s.treeLocked(ctx, c.Permanode)
	s.treeMu.Unlock()
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	cur, ok := tree[c.Path]
	s.treeMu.Unlock()
	if ok {
		if cur.File == c.File {
			return api.ClaimUnchanged, nil
		}
		if !c.ModTime.After(cur.ModTime) {
			return api.ClaimRejectedOlder, nil
		}
	}
	multi, err := schema.NewMultiClaim(
		schema.NewSetAttributeClaim(pn, pathAttrPrefix+c.Path, file.String()),
		schema.NewSetAttributeClaim(pn, mtimeAttrPrefix+c.Path, schema.RFC3339FromTime(c.ModTime)),
	)
	if err != nil {
		return 0, err
	}
	multi.SetMultiClaimPermanode(pn)
	claim, err := s.signAndUpload(ctx, multi)
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	tree[c.Path] = api.PathEntry{File: c.File, ModTime: c.ModTime, Claim: api.BlobRef(claim.String())}
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// Paths returns the current entries of a kept folder's tree.
func (s *Server) Paths(ctx context.Context, permanode api.BlobRef) (map[string]api.PathEntry, error) {
	s.treeMu.Lock()
	defer s.treeMu.Unlock()
	t, err := s.treeLocked(ctx, permanode)
	if err != nil {
		return nil, err
	}
	out := make(map[string]api.PathEntry, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out, nil
}

// PathsAt returns the entries of a kept folder's tree as it was at time
// at: the result of the claims signed at or before at. A path first
// claimed later is absent, and a path changed later has its earlier entry.
// at is when the server kept a version, not the file's mtime.
func (s *Server) PathsAt(ctx context.Context, permanode api.BlobRef, at time.Time) (map[string]api.PathEntry, error) {
	if at.IsZero() {
		return nil, fmt.Errorf("pkserver: PathsAt needs a time")
	}
	return s.loadTree(ctx, permanode, at)
}

// treeLocked returns the cached tree of permanode, loading it from the
// index on first use. This server is the only signer, so after loading,
// ClaimPath keeps the cache current. The caller holds treeMu.
func (s *Server) treeLocked(ctx context.Context, permanode api.BlobRef) (map[string]api.PathEntry, error) {
	if t, ok := s.trees[permanode]; ok {
		return t, nil
	}
	t, err := s.loadTree(ctx, permanode, time.Time{})
	if err != nil {
		return nil, err
	}
	s.trees[permanode] = t
	return t, nil
}

// loadTree folds the server's claims on permanode, read from the index,
// into its tree, counting only the claims signed at or before at, or all
// of them if at is zero.
func (s *Server) loadTree(ctx context.Context, permanode api.BlobRef, at time.Time) (map[string]api.PathEntry, error) {
	pn, ok := blob.Parse(string(permanode))
	if !ok {
		return nil, fmt.Errorf("pkserver: bad permanode %q", permanode)
	}
	res, err := s.bs.GetClaims(ctx, &search.ClaimsRequest{Permanode: pn})
	if err != nil {
		return nil, fmt.Errorf("pkserver: claims of %s: %w", permanode, err)
	}
	if len(res.Claims) == 0 {
		return nil, fmt.Errorf("pkserver: unknown permanode %s", permanode)
	}
	claims := res.Claims
	sort.SliceStable(claims, func(i, j int) bool {
		return time.Time(claims[i].Date).Before(time.Time(claims[j].Date))
	})
	type val struct {
		v     string
		claim blob.Ref
	}
	attrs := map[string]*val{}
	for _, c := range claims {
		if c.Signer != s.pubRef {
			continue // only the server's own claims count
		}
		if !at.IsZero() && time.Time(c.Date).After(at) {
			break // sorted by date, so every later claim is after at too
		}
		switch c.Type {
		case "set-attribute", "add-attribute":
			attrs[c.Attr] = &val{c.Value, c.BlobRef}
		case "del-attribute":
			if c.Value == "" || (attrs[c.Attr] != nil && attrs[c.Attr].v == c.Value) {
				delete(attrs, c.Attr)
			}
		}
	}
	t := map[string]api.PathEntry{}
	for k, v := range attrs {
		p, ok := strings.CutPrefix(k, pathAttrPrefix)
		if !ok {
			continue
		}
		e := api.PathEntry{File: api.BlobRef(v.v), Claim: api.BlobRef(v.claim.String())}
		if mt := attrs[mtimeAttrPrefix+p]; mt != nil {
			if tm, err := time.Parse(time.RFC3339Nano, mt.v); err == nil {
				e.ModTime = tm
			}
		}
		t[p] = e
	}
	return t, nil
}

// signAndUpload signs bb with the server's identity and stores it. Claim
// dates strictly increase, so that a later claim on an attribute always
// wins even within one clock tick. The caller holds claimMu.
func (s *Server) signAndUpload(ctx context.Context, bb *schema.Builder) (blob.Ref, error) {
	now := time.Now().UTC()
	if !now.After(s.lastClaim) {
		now = s.lastClaim.Add(time.Microsecond)
	}
	s.lastClaim = now
	signed, err := bb.SignAt(ctx, s.signer, now)
	if err != nil {
		return blob.Ref{}, fmt.Errorf("pkserver: signing: %w", err)
	}
	pr, err := s.bs.Upload(ctx, client.NewUploadHandleFromString(signed))
	if err != nil {
		return blob.Ref{}, fmt.Errorf("pkserver: storing claim: %w", err)
	}
	return pr.BlobRef, nil
}
