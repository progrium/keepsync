package pkserver

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
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
// in its directory. keepsyncMtime and keepsyncVersion are the mtime and
// Syncthing version the current content was kept with, set in the same
// claim as camliContent, so api.Supersedes needs only the index and comes
// back with a reindex. keepsyncGone and keepsyncGoneAt record why and when
// the file left sync; the next content claim deletes them.
const (
	pathAttrPrefix = "camliPath:"
	contentAttr    = "camliContent"
	mtimeAttr      = "keepsyncMtime"
	versionAttr    = "keepsyncVersion"
	modeAttr       = "keepsyncMode"
	goneAttr       = "keepsyncGone"
	goneAtAttr     = "keepsyncGoneAt"
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
	version  api.Version
	// contentClaim is the claim that set content, mtime and version, and
	// keptAt its date.
	contentClaim blob.Ref
	keptAt       time.Time
	gone         api.GoneReason
	goneAt       time.Time
	// lastClaim is the date of the newest claim on pn. A new claim on pn is
	// dated after it, so it wins when the index folds claims by date, even
	// if the clock is now behind (docs/design.md, Edge cases).
	lastClaim time.Time
	// mode is the file's permission bits, or 0 if not recorded.
	mode os.FileMode
}

// entry is a file node's PathEntry, less its Claims.
func (n *treeNode) entry() api.PathEntry {
	return api.PathEntry{
		File:    api.BlobRef(n.content.String()),
		ModTime: n.mtime,
		Version: n.version,
		Mode:    n.mode,
		KeptAt:  n.keptAt,
		Gone:    n.gone,
		GoneAt:  n.goneAt,
	}
}

// newestMtime is the newest mtime of the files under a directory node.
func newestMtime(n *treeNode) time.Time {
	if n.isFile {
		return n.mtime
	}
	var t time.Time
	for _, c := range n.children {
		if m := newestMtime(c); m.After(t) {
			t = m
		}
	}
	return t
}

// contentOps are the attribute operations that set a file permanode's
// content to c's, clearing a version or gone mark that cur (the node's
// current state, or nil for a new permanode) has and c doesn't replace.
func contentOps(pn blob.Ref, file blob.Ref, c api.PathClaim, cur *treeNode) []*schema.Builder {
	ops := []*schema.Builder{
		schema.NewSetAttributeClaim(pn, contentAttr, file.String()),
		schema.NewSetAttributeClaim(pn, mtimeAttr, schema.RFC3339FromTime(c.ModTime)),
	}
	if len(c.Version) > 0 {
		ops = append(ops, schema.NewSetAttributeClaim(pn, versionAttr, c.Version.String()))
	} else if cur != nil && len(cur.version) > 0 {
		ops = append(ops, schema.NewDelAttributeClaim(pn, versionAttr, ""))
	}
	if c.Mode != 0 {
		ops = append(ops, schema.NewSetAttributeClaim(pn, modeAttr, formatMode(c.Mode)))
	} else if cur != nil && cur.mode != 0 {
		ops = append(ops, schema.NewDelAttributeClaim(pn, modeAttr, ""))
	}
	if cur != nil && cur.gone != api.GoneNone {
		ops = append(ops,
			schema.NewDelAttributeClaim(pn, goneAttr, ""),
			schema.NewDelAttributeClaim(pn, goneAtAttr, ""))
	}
	return ops
}

// setContent records in a file node the content claim just signed, at date.
func (s *Server) setContent(n *treeNode, file blob.Ref, c api.PathClaim, claim blob.Ref, date time.Time) {
	n.content, n.mtime, n.version, n.mode = file, c.ModTime, c.Version, c.Mode
	n.contentClaim, n.keptAt, n.lastClaim = claim, date, date
	n.gone, n.goneAt = api.GoneNone, time.Time{}
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
// It writes nothing if the file there already has c.File as content and
// no gone mark (with a mark, see unmarkGone), and
// rejects a claim that api.Supersedes says is not newer than the file's
// current content. Each write is one signed multi claim: on the file's
// permanode for a changed file; or, for a new file, setting its content
// and naming it and any missing directories, whose permanodes it creates.
//
// A path whose existing entry is the other type is retargeted: the name
// gets a new permanode of the right type, and the old one stays in
// history. A file becomes a directory whenever a path goes through it. A
// directory becomes a file only for a claim with a Version, or with a
// ModTime newer than the newest file under the directory.
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

	// Follow the existing directories down the path. Only signing code,
	// under claimMu, changes the tree, so it can be read without treeMu.
	dir := root
	i := 0
	for ; i < len(parts); i++ {
		child, ok := dir.children[parts[i]]
		if !ok {
			break
		}
		last := i == len(parts)-1
		if !last && !child.isFile {
			dir = child
			continue
		}
		if last && child.isFile {
			return s.updateFile(ctx, child, file, c)
		}
		if last && len(c.Version) == 0 && !c.ModTime.After(newestMtime(child)) {
			return api.ClaimRejectedOlder, nil // directory→file, not newer
		}
		break // retarget: file→directory, or directory→file
	}

	// A new file, and the directories from parts[i] down to it, each
	// named in its parent (which renames a retargeted entry).
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
			n.isFile = true
			ops = append(ops, contentOps(pn, file, c, nil)...)
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
			s.setContent(m.node, file, c, claim, date)
		}
		m.parent.children[m.name] = m.node
	}
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// updateFile keeps new content on an existing file permanode, in one claim.
// The caller holds claimMu.
func (s *Server) updateFile(ctx context.Context, n *treeNode, file blob.Ref, c api.PathClaim) (api.ClaimResult, error) {
	if n.content == file {
		if n.gone != api.GoneNone {
			return s.unmarkGone(ctx, n, c)
		}
		if c.Mode != 0 && c.Mode != n.mode && api.Supersedes(c, n.entry()) {
			return s.setMode(ctx, n, c)
		}
		return api.ClaimUnchanged, nil
	}
	if !api.Supersedes(c, n.entry()) {
		return api.ClaimRejectedOlder, nil
	}
	multi, err := schema.NewMultiClaim(contentOps(n.pn, file, c, n)...)
	if err != nil {
		return 0, err
	}
	claim, date, err := s.signClaim(ctx, multi.SetMultiClaimPermanode(n.pn), n.lastClaim)
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	s.setContent(n, file, c, claim, date)
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// unmarkGone handles the content a file already has being kept again
// while it has a gone mark, as when an expired file is restored: one claim
// deletes the mark and, if api.Supersedes accepts c, also updates the
// mtime and version, so later comparisons use the newest. The content
// claim, and so KeptAt and History, stay as they were. The caller holds
// claimMu.
func (s *Server) unmarkGone(ctx context.Context, n *treeNode, c api.PathClaim) (api.ClaimResult, error) {
	ops := []*schema.Builder{
		schema.NewDelAttributeClaim(n.pn, goneAttr, ""),
		schema.NewDelAttributeClaim(n.pn, goneAtAttr, ""),
	}
	newer := api.Supersedes(c, n.entry())
	if newer {
		ops = append(ops, schema.NewSetAttributeClaim(n.pn, mtimeAttr, schema.RFC3339FromTime(c.ModTime)))
		if len(c.Version) > 0 {
			ops = append(ops, schema.NewSetAttributeClaim(n.pn, versionAttr, c.Version.String()))
		} else if len(n.version) > 0 {
			ops = append(ops, schema.NewDelAttributeClaim(n.pn, versionAttr, ""))
		}
	}
	multi, err := schema.NewMultiClaim(ops...)
	if err != nil {
		return 0, err
	}
	_, date, err := s.signClaim(ctx, multi.SetMultiClaimPermanode(n.pn), n.lastClaim)
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	n.gone, n.goneAt, n.lastClaim = api.GoneNone, time.Time{}, date
	if newer {
		n.mtime, n.version = c.ModTime, c.Version
	}
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// setMode records a permission change with the same content (a chmod):
// one claim sets the mode, mtime and version. The content claim, and so
// KeptAt and History, stay as they were. The caller holds claimMu.
func (s *Server) setMode(ctx context.Context, n *treeNode, c api.PathClaim) (api.ClaimResult, error) {
	ops := []*schema.Builder{
		schema.NewSetAttributeClaim(n.pn, modeAttr, formatMode(c.Mode)),
		schema.NewSetAttributeClaim(n.pn, mtimeAttr, schema.RFC3339FromTime(c.ModTime)),
	}
	if len(c.Version) > 0 {
		ops = append(ops, schema.NewSetAttributeClaim(n.pn, versionAttr, c.Version.String()))
	}
	multi, err := schema.NewMultiClaim(ops...)
	if err != nil {
		return 0, err
	}
	_, date, err := s.signClaim(ctx, multi.SetMultiClaimPermanode(n.pn), n.lastClaim)
	if err != nil {
		return 0, err
	}
	s.treeMu.Lock()
	n.mode, n.mtime, n.lastClaim = c.Mode, c.ModTime, date
	if len(c.Version) > 0 {
		n.version = c.Version
	}
	s.treeMu.Unlock()
	return api.ClaimWritten, nil
}

// formatMode and parseMode store permission bits as octal, like Perkeep's
// own unixPermission.
func formatMode(m os.FileMode) string { return fmt.Sprintf("0%o", m.Perm()) }

func parseMode(s string) os.FileMode {
	v, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0
	}
	return os.FileMode(v).Perm()
}

// findFile returns the file node at path in root's tree, or nil. The
// caller holds claimMu or treeMu.
func findFile(root *treeNode, parts []string) *treeNode {
	n := root
	for _, p := range parts {
		if n.isFile {
			return nil
		}
		c, ok := n.children[p]
		if !ok {
			return nil
		}
		n = c
	}
	if !n.isFile {
		return nil
	}
	return n
}

// MarkGone records why a kept file left sync, setting keepsyncGone and
// keepsyncGoneAt on its permanode in one claim. It writes nothing if the
// path holds no file, holds other content than g.File, or already has the
// mark.
func (s *Server) MarkGone(ctx context.Context, g api.GoneClaim) error {
	if g.Reason == api.GoneNone {
		return fmt.Errorf("pkserver: gone mark without a reason")
	}
	file, ok := blob.Parse(string(g.File))
	if !ok {
		return fmt.Errorf("pkserver: bad file ref %q", g.File)
	}
	parts, err := splitPath(g.Path)
	if err != nil {
		return err
	}
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	s.treeMu.Lock()
	root, err := s.treeLocked(ctx, g.Permanode)
	s.treeMu.Unlock()
	if err != nil {
		return err
	}
	n := findFile(root, parts)
	if n == nil || n.content != file || n.gone == g.Reason {
		return nil
	}
	multi, err := schema.NewMultiClaim(
		schema.NewSetAttributeClaim(n.pn, goneAttr, string(g.Reason)),
		schema.NewSetAttributeClaim(n.pn, goneAtAttr, schema.RFC3339FromTime(time.Now().UTC())),
	)
	if err != nil {
		return err
	}
	_, date, err := s.signClaim(ctx, multi.SetMultiClaimPermanode(n.pn), n.lastClaim)
	if err != nil {
		return err
	}
	s.treeMu.Lock()
	n.gone, n.goneAt, n.lastClaim = g.Reason, date, date
	s.treeMu.Unlock()
	return nil
}

// History returns every content value the file permanode now at path has
// had, oldest first, folded from its claims. Each entry's Claims are those
// recovering that version by name needs: the current chain of directories
// and naming claims, and that version's content claim. Gone marks are not
// part of history.
func (s *Server) History(ctx context.Context, permanode api.BlobRef, path string) ([]api.PathEntry, error) {
	parts, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	s.treeMu.Lock()
	root, err := s.treeLocked(ctx, permanode)
	var chain []blob.Ref
	var n *treeNode
	if err == nil {
		chain = []blob.Ref{root.pn}
		n = root
		for _, p := range parts {
			c, ok := n.children[p]
			if !ok || n.isFile {
				n = nil
				break
			}
			n = c
			chain = append(chain, n.pn, n.nameClaim)
		}
	}
	s.treeMu.Unlock()
	if err != nil {
		return nil, err
	}
	if n == nil || !n.isFile {
		return nil, fmt.Errorf("pkserver: %s: no kept file at %s", permanode, path)
	}

	claims, err := s.claimsOf(ctx, n.pn)
	if err != nil {
		return nil, err
	}
	// Group the attribute operations by claim; a content claim sets
	// camliContent, and its keepsyncMtime and keepsyncVersion are in the
	// same claim.
	type version struct {
		claim blob.Ref
		date  time.Time
		attrs map[string]string
	}
	var order []*version
	byClaim := map[blob.Ref]*version{}
	for _, c := range claims {
		v := byClaim[c.BlobRef]
		if v == nil {
			v = &version{claim: c.BlobRef, date: time.Time(c.Date), attrs: map[string]string{}}
			byClaim[c.BlobRef] = v
			order = append(order, v)
		}
		if c.Type == "set-attribute" || c.Type == "add-attribute" {
			v.attrs[c.Attr] = c.Value
		}
	}
	var out []api.PathEntry
	for _, v := range order {
		content, ok := v.attrs[contentAttr]
		if !ok {
			continue
		}
		e := api.PathEntry{File: api.BlobRef(content), KeptAt: v.date}
		if tm, err := time.Parse(time.RFC3339Nano, v.attrs[mtimeAttr]); err == nil {
			e.ModTime = tm
		}
		if ver, err := api.ParseVersion(v.attrs[versionAttr]); err == nil {
			e.Version = ver
		}
		e.Mode = parseMode(v.attrs[modeAttr])
		e.Claims = uniqueRefs(append(chain[:len(chain):len(chain)], v.claim))
		out = append(out, e)
	}
	return out, nil
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
			e := c.entry()
			e.Claims = uniqueRefs(refs)
			out[p] = e
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
	date  time.Time
}

// attrsOf folds the server's claims on pn, read from the index, into its
// current attributes, counting only the claims signed at or before at, or
// all of them if at is zero. It also returns the date of the server's newest
// claim on pn, whatever at is, and reports false if pn has no claims at all.
func (s *Server) attrsOf(ctx context.Context, pn blob.Ref, at time.Time) (map[string]attrVal, time.Time, bool, error) {
	claims, err := s.claimsOf(ctx, pn)
	if err != nil {
		return nil, time.Time{}, false, err
	}
	var latest time.Time
	if len(claims) > 0 {
		latest = time.Time(claims[len(claims)-1].Date) // oldest first
	}
	attrs := map[string]attrVal{}
	for _, c := range claims {
		if !at.IsZero() && time.Time(c.Date).After(at) {
			break // sorted by date, so every later claim is after at too
		}
		switch c.Type {
		case "set-attribute", "add-attribute":
			attrs[c.Attr] = attrVal{c.Value, c.BlobRef, time.Time(c.Date)}
		case "del-attribute":
			if cur, ok := attrs[c.Attr]; c.Value == "" || (ok && cur.v == c.Value) {
				delete(attrs, c.Attr)
			}
		}
	}
	return attrs, latest, len(claims) > 0, nil
}

// claimsOf returns the server's own claims on pn, from the index, oldest
// first.
func (s *Server) claimsOf(ctx context.Context, pn blob.Ref) ([]*search.ClaimsItem, error) {
	res, err := s.bs.GetClaims(ctx, &search.ClaimsRequest{Permanode: pn})
	if err != nil {
		return nil, fmt.Errorf("pkserver: claims of %s: %w", pn, err)
	}
	var claims []*search.ClaimsItem
	for _, c := range res.Claims {
		if c.Signer == s.pubRef { // only the server's own claims count
			claims = append(claims, c)
		}
	}
	sort.SliceStable(claims, func(i, j int) bool {
		return time.Time(claims[i].Date).Before(time.Time(claims[j].Date))
	})
	return claims, nil
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
			n.isFile, n.content, n.contentClaim, n.keptAt = true, ref, content.claim, content.date
			if mt, ok := cattrs[mtimeAttr]; ok {
				if tm, err := time.Parse(time.RFC3339Nano, mt.v); err == nil {
					n.mtime = tm
				}
			}
			if v, ok := cattrs[versionAttr]; ok {
				if ver, err := api.ParseVersion(v.v); err == nil {
					n.version = ver
				}
			}
			if m, ok := cattrs[modeAttr]; ok {
				n.mode = parseMode(m.v)
			}
			if g, ok := cattrs[goneAttr]; ok {
				n.gone, n.goneAt = api.GoneReason(g.v), g.date
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
	if err := s.stashSigned(pr.BlobRef, signed); err != nil {
		return blob.Ref{}, time.Time{}, err
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
