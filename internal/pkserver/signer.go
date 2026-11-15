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

// The kept tree uses Perkeep's own conventions (docs/design.md, "Kept tree
// layout"): the root permanode has title and camliRoot; each directory is
// a permanode named by camliPath:<name> in its parent; each file is a
// permanode with camliContent and keepsyncMtime, named by camliPath:<name>
// in its directory. keepsyncMtime is the mtime the current content was
// kept with, set in the same claim as camliContent, so the newer-mtime
// rule needs only the index and comes back with a reindex.
const (
	pathAttrPrefix = "camliPath:"
	contentAttr    = "camliContent"
	mtimeAttr      = "keepsyncMtime"
	kindAttr       = "keepsyncKind"
	kindFolder     = "folder"
	kindDir        = "dir"
)

// treeNode is one permanode of a kept tree.
type treeNode struct {
	pn        blob.Ref
	nameClaim blob.Ref // the claim naming it in its parent; zero for the root

	// A directory (or the root) has children; a file has content.
	children map[string]*treeNode
	isFile   bool
	content  blob.Ref
	mtime    time.Time
	// contentClaim is the claim that set content and mtime.
	contentClaim blob.Ref
	// lastClaim is the date of the newest claim on pn. A new claim on pn is
	// dated after it, so it wins when the index folds claims by date, even
	// if the clock is now behind (docs/design.md, Edge cases).
	lastClaim time.Time
}

func newDirNode(pn, nameClaim blob.Ref) *treeNode {
	return &treeNode{pn: pn, nameClaim: nameClaim, children: map[string]*treeNode{}}
}

// NewPermanode creates the root permanode of a kept folder, with title and
// camliRoot set to title.
func (s *Server) NewPermanode(ctx context.Context, title string) (api.BlobRef, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	pn, err := s.signAndUpload(ctx, schema.NewUnsignedPermanode())
	if err != nil {
		return "", err
	}
	multi, err := schema.NewMultiClaim(
		schema.NewSetAttributeClaim(pn, "title", title),
		schema.NewSetAttributeClaim(pn, "camliRoot", title),
		schema.NewSetAttributeClaim(pn, kindAttr, kindFolder),
	)
	if err != nil {
		return "", err
	}
	multi.SetMultiClaimPermanode(pn)
	_, date, err := s.signClaim(ctx, multi, time.Time{})
	if err != nil {
		return "", err
	}
	root := newDirNode(pn, blob.Ref{})
	root.lastClaim = date
	s.treeMu.Lock()
	s.trees[api.BlobRef(pn.String())] = root
	s.treeMu.Unlock()
	return api.BlobRef(pn.String()), nil
}

// splitPath splits a slash path into its components, refusing empty, "."
// and ".." components and absolute paths.
func splitPath(p string) ([]string, error) {
	if p == "" || strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("pkserver: bad path %q", p)
	}
	parts := strings.Split(p, "/")
	for _, c := range parts {
		if c == "" || c == "." || c == ".." {
			return nil, fmt.Errorf("pkserver: bad path %q", p)
		}
	}
	return parts, nil
}

// ClaimPath keeps c.File at c.Path under the root permanode c.Permanode.
// It writes nothing if the file there already has c.File as content, and
// rejects a claim whose ModTime is not newer than the file's
// keepsyncMtime. Each write is one signed multi claim: on the file's
// permanode for a changed file; or, for a new file, setting its content
// and naming it and any missing directories, whose permanodes it creates.
func (s *Server) ClaimPath(ctx context.Context, c api.PathClaim) (api.ClaimResult, error) {
	if _, ok := blob.Parse(string(c.Permanode)); !ok {
		return 0, fmt.Errorf("pkserver: bad permanode %q", c.Permanode)
	}
	file, ok := blob.Parse(string(c.File))
	if !ok {
		return 0, fmt.Errorf("pkserver: bad file ref %q", c.File)
	}
	parts, err := splitPath(c.Path)
	if err != nil {
		return 0, err
	}
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.treeMu.Lock()
	root, err := s.treeLocked(ctx, c.Permanode)
	s.treeMu.Unlock()
	if err != nil {
		return 0, err
	}

	// Find the deepest existing directory on the path. Only ClaimPath,
	// under claimMu, changes the tree, so it can be read without treeMu.
	dir := root
	i := 0
	for ; i < len(parts)-1; i++ {
		next, ok := dir.children[parts[i]]
		if !ok {
			break
		}
		if next.isFile {
			return 0, fmt.Errorf("pkserver: %s: %s is a file", c.Path, strings.Join(parts[:i+1], "/"))
		}
		dir = next
	}
	base := parts[len(parts)-1]
	mtime := schema.RFC3339FromTime(c.ModTime)

	if i == len(parts)-1 {
		if cur, ok := dir.children[base]; ok {
			if !cur.isFile {
				return 0, fmt.Errorf("pkserver: %s is a directory", c.Path)
			}
			if cur.content == file {
				return api.ClaimUnchanged, nil
			}
			if !c.ModTime.After(cur.mtime) {
				return api.ClaimRejectedOlder, nil
			}
			multi, err := schema.NewMultiClaim(
				schema.NewSetAttributeClaim(cur.pn, contentAttr, file.String()),
				schema.NewSetAttributeClaim(cur.pn, mtimeAttr, mtime),
			)
			if err != nil {
				return 0, err
			}
			claim, date, err := s.signClaim(ctx, multi.SetMultiClaimPermanode(cur.pn), cur.lastClaim)
			if err != nil {
				return 0, err
			}
			s.treeMu.Lock()
			cur.content, cur.mtime, cur.contentClaim, cur.lastClaim = file, c.ModTime, claim, date
			s.treeMu.Unlock()
			return api.ClaimWritten, nil
		}
	}

	// A new file, and the directories from parts[i] down to it.
	var ops []*schema.Builder
	type made struct {
		parent *treeNode
		name   string
		node   *treeNode
	}
	var news []made
	parent := dir
	for ; i < len(parts); i++ {
		pn, err := s.signAndUpload(ctx, schema.NewUnsignedPermanode())
		if err != nil {
			return 0, err
		}
		n := newDirNode(pn, blob.Ref{})
		if i == len(parts)-1 {
			n.children = nil
			n.isFile, n.content, n.mtime = true, file, c.ModTime
			ops = append(ops,
				schema.NewSetAttributeClaim(pn, contentAttr, file.String()),
				schema.NewSetAttributeClaim(pn, mtimeAttr, mtime))
		} else {
			ops = append(ops,
				schema.NewSetAttributeClaim(pn, "title", parts[i]),
				schema.NewSetAttributeClaim(pn, kindAttr, kindDir))
		}
		ops = append(ops, schema.NewSetAttributeClaim(parent.pn, pathAttrPrefix+parts[i], pn.String()))
		news = append(news, made{parent, parts[i], n})
		parent = n
	}
	multi, err := schema.NewMultiClaim(ops...)
	if err != nil {
		return 0, err
	}
	// The claim changes dir's attributes; the other permanodes are new.
	claim, date, err := s.signClaim(ctx, multi, dir.lastClaim)
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	dir.lastClaim = date
	for _, m := range news {
		m.node.nameClaim, m.node.lastClaim = claim, date
		if m.node.isFile {
			m.node.contentClaim = claim
		}
		m.parent.children[m.name] = m.node
	}
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// Paths returns the current entries of a kept folder's tree, by slash
// path.
func (s *Server) Paths(ctx context.Context, permanode api.BlobRef) (map[string]api.PathEntry, error) {
	s.treeMu.Lock()
	defer s.treeMu.Unlock()
	root, err := s.treeLocked(ctx, permanode)
	if err != nil {
		return nil, err
	}
	return flatten(root), nil
}

// PathsAt returns the entries of a kept folder's tree as it was at time
// at: the result of the claims signed at or before at. A path first
// claimed later is absent, and a path changed later has its earlier entry.
// at is when the server kept a version, not the file's mtime.
func (s *Server) PathsAt(ctx context.Context, permanode api.BlobRef, at time.Time) (map[string]api.PathEntry, error) {
	if at.IsZero() {
		return nil, fmt.Errorf("pkserver: PathsAt needs a time")
	}
	root, err := s.loadTree(ctx, permanode, at)
	if err != nil {
		return nil, err
	}
	return flatten(root), nil
}

// flatten returns the files under root by slash path. Each entry's Claims
// hold every blob besides the file that recovering it by name needs: the
// file's permanode and the claims setting its content and naming it, and
// the permanode of each directory above it, up to and including the root,
// with the claims naming them.
func flatten(root *treeNode) map[string]api.PathEntry {
	out := map[string]api.PathEntry{}
	var walk func(n *treeNode, prefix string, chain []blob.Ref)
	walk = func(n *treeNode, prefix string, chain []blob.Ref) {
		for name, c := range n.children {
			p := prefix + name
			if !c.isFile {
				walk(c, p+"/", append(chain[:len(chain):len(chain)], c.pn, c.nameClaim))
				continue
			}
			refs := append(chain[:len(chain):len(chain)], c.pn, c.nameClaim, c.contentClaim)
			out[p] = api.PathEntry{File: api.BlobRef(c.content.String()), ModTime: c.mtime, Claims: uniqueRefs(refs)}
		}
	}
	walk(root, "", []blob.Ref{root.pn})
	return out
}

func uniqueRefs(refs []blob.Ref) []api.BlobRef {
	seen := map[blob.Ref]bool{}
	var out []api.BlobRef
	for _, r := range refs {
		if r.Valid() && !seen[r] {
			seen[r] = true
			out = append(out, api.BlobRef(r.String()))
		}
	}
	return out
}

// treeLocked returns the cached tree of permanode, loading it from the
// index on first use. This server is the only signer, so after loading,
// ClaimPath keeps the cache current. The caller holds treeMu.
func (s *Server) treeLocked(ctx context.Context, permanode api.BlobRef) (*treeNode, error) {
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

// attrVal is an attribute's current value and the claim that set it.
type attrVal struct {
	v     string
	claim blob.Ref
}

// attrsOf folds the server's claims on pn, read from the index, into its
// current attributes, counting only the claims signed at or before at, or
// all of them if at is zero. It also returns the date of the server's newest
// claim on pn, whatever at is, and reports false if pn has no claims at all.
func (s *Server) attrsOf(ctx context.Context, pn blob.Ref, at time.Time) (map[string]attrVal, time.Time, bool, error) {
	res, err := s.bs.GetClaims(ctx, &search.ClaimsRequest{Permanode: pn})
	if err != nil {
		return nil, time.Time{}, false, fmt.Errorf("pkserver: claims of %s: %w", pn, err)
	}
	claims := res.Claims
	sort.SliceStable(claims, func(i, j int) bool {
		return time.Time(claims[i].Date).Before(time.Time(claims[j].Date))
	})
	var latest time.Time
	for _, c := range claims {
		if c.Signer == s.pubRef && time.Time(c.Date).After(latest) {
			latest = time.Time(c.Date)
		}
	}
	attrs := map[string]attrVal{}
	for _, c := range claims {
		if c.Signer != s.pubRef {
			continue // only the server's own claims count
		}
		if !at.IsZero() && time.Time(c.Date).After(at) {
			break // sorted by date, so every later claim is after at too
		}
		switch c.Type {
		case "set-attribute", "add-attribute":
			attrs[c.Attr] = attrVal{c.Value, c.BlobRef}
		case "del-attribute":
			if cur, ok := attrs[c.Attr]; c.Value == "" || (ok && cur.v == c.Value) {
				delete(attrs, c.Attr)
			}
		}
	}
	return attrs, latest, len(claims) > 0, nil
}

// loadTree walks a kept tree from its root permanode, as of at (or now, if
// at is zero).
func (s *Server) loadTree(ctx context.Context, permanode api.BlobRef, at time.Time) (*treeNode, error) {
	pn, ok := blob.Parse(string(permanode))
	if !ok {
		return nil, fmt.Errorf("pkserver: bad permanode %q", permanode)
	}
	attrs, latest, ok, err := s.attrsOf(ctx, pn, at)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("pkserver: unknown permanode %s", permanode)
	}
	if attrs[kindAttr].v != kindFolder {
		// A directory's or file's permanode, or someone else's.
		return nil, fmt.Errorf("pkserver: unknown permanode %s: not a kept folder's root", permanode)
	}
	root := newDirNode(pn, blob.Ref{})
	root.lastClaim = latest
	visited := map[blob.Ref]bool{pn: true}
	if err := s.loadChildren(ctx, root, attrs, at, visited); err != nil {
		return nil, err
	}
	return root, nil
}

func (s *Server) loadChildren(ctx context.Context, dir *treeNode, attrs map[string]attrVal, at time.Time, visited map[blob.Ref]bool) error {
	for k, v := range attrs {
		name, ok := strings.CutPrefix(k, pathAttrPrefix)
		if !ok {
			continue
		}
		cpn, ok := blob.Parse(v.v)
		if !ok || visited[cpn] {
			continue // not a permanode, or a cycle
		}
		visited[cpn] = true
		cattrs, latest, _, err := s.attrsOf(ctx, cpn, at)
		if err != nil {
			return err
		}
		n := newDirNode(cpn, v.claim)
		n.lastClaim = latest
		if content, ok := cattrs[contentAttr]; ok {
			ref, ok := blob.Parse(content.v)
			if !ok {
				continue
			}
			n.children = nil
			n.isFile, n.content, n.contentClaim = true, ref, content.claim
			if mt, ok := cattrs[mtimeAttr]; ok {
				if tm, err := time.Parse(time.RFC3339Nano, mt.v); err == nil {
					n.mtime = tm
				}
			}
		} else if err := s.loadChildren(ctx, n, cattrs, at, visited); err != nil {
			return err
		}
		dir.children[name] = n
	}
	return nil
}

// signAndUpload signs bb with the server's identity and stores it. The
// caller holds claimMu.
func (s *Server) signAndUpload(ctx context.Context, bb *schema.Builder) (blob.Ref, error) {
	ref, _, err := s.signClaim(ctx, bb, time.Time{})
	return ref, err
}

// signClaim is signAndUpload, returning the date it signed at. Dates
// strictly increase within a run, so a later claim on an attribute always
// wins even within one clock tick, and are after notBefore, the newest
// claim already on the permanodes bb changes, so a clock that went back
// across a restart cannot date a claim before the one it replaces. The
// caller holds claimMu.
func (s *Server) signClaim(ctx context.Context, bb *schema.Builder, notBefore time.Time) (blob.Ref, time.Time, error) {
	now := s.clock().UTC()
	for _, after := range []time.Time{s.lastClaim, notBefore} {
		if !now.After(after) {
			now = after.Add(time.Microsecond)
		}
	}
	s.lastClaim = now
	s.signed++
	signed, err := bb.SignAt(ctx, s.signer, now)
	if err != nil {
		return blob.Ref{}, time.Time{}, fmt.Errorf("pkserver: signing: %w", err)
	}
	pr, err := s.bs.Upload(ctx, client.NewUploadHandleFromString(signed))
	if err != nil {
		return blob.Ref{}, time.Time{}, fmt.Errorf("pkserver: storing claim: %w", err)
	}
	return pr.BlobRef, now, nil
}

// clock is the time claims are dated with: time.Now, or Server.now in
// tests.
func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}
