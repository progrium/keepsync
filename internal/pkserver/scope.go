package pkserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"perkeep.org/pkg/auth"
	"perkeep.org/pkg/types/camtypes"

	"github.com/progrium/keepsync/internal/api"
)

// Node scopes, as registry.Server.Scope returns them (see docs/design.md,
// "Perkeep gateway"). They are the same strings as registry.ScopeBlobs and
// registry.ScopeRead.
const (
	ScopeBlobs = "blobs"
	ScopeRead  = "read"
)

// nodeGrant is what every node may do over tailcat (the blobs scope):
// upload, stat, and fetch blobs, plus discovery, which upload needs. Not
// sign, enumerate, search, or remove.
const nodeGrant = auth.OpVivify

// peerKey is the context key of the node a tunnelled request came from.
type peerKeyCtx struct{}

// roots are the URL prefixes of the perkeepd handlers that the read scope
// opens, from the server's own discovery.
type roots struct {
	blob, search, sign, ui, status, help string
}

func (s *Server) loadRoots(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/?camli.mode=config", nil)
	if err != nil {
		return err
	}
	res, err := s.owner.Do(req)
	if err != nil {
		return fmt.Errorf("pkserver: discovery: %w", err)
	}
	defer res.Body.Close()
	var d camtypes.Discovery
	if err := json.NewDecoder(res.Body).Decode(&d); err != nil {
		return fmt.Errorf("pkserver: discovery: %w", err)
	}
	s.roots = roots{
		blob:   d.BlobRoot,
		search: d.SearchRoot,
		sign:   d.JSONSignRoot,
		status: d.StatusRoot,
		help:   d.HelpRoot,
	}
	if d.UIDiscovery != nil {
		s.roots.ui = d.UIDiscovery.UIRoot
	}
	return nil
}

// searchGets and searchPosts are the read-only endpoints of the search
// handler, under $searchRoot/camli/search/. "ws" is the WebSocket the web
// UI uses for live query results; it only answers queries.
var (
	searchGets = map[string]bool{
		"recent": true, "permanodeattr": true, "describe": true,
		"exif": true, "claims": true, "files": true, "signerattrvalue": true,
		"signerpaths": true, "edgesto": true, "ws": true,
	}
	searchPosts = map[string]bool{"describe": true, "query": true}
)

// needsRead reports whether r is one of the read requests that the read
// scope adds to the blobs scope: enumerating blobs, search, the web UI,
// status, help, and the signing handler's discovery and public key.
// Requests it accepts are served to read nodes with every operation
// granted, as the handlers behind them check for auth.OpAll, so it must
// accept nothing that writes, signs, or deletes. Fetching, statting, and
// uploading blobs, and discovery, are the blobs scope's: it leaves those
// to nodeGrant.
func (rt roots) needsRead(r *http.Request) bool {
	p := r.URL.Path
	if !strings.HasPrefix(p, "/") || (p != path.Clean(p) && p != path.Clean(p)+"/") {
		// Nothing may reach a handler under another name than the one
		// checked here.
		return false
	}
	isGet := r.Method == "GET" || r.Method == "HEAD"
	isPost := r.Method == "POST"
	if r.URL.Query().Get("camli.mode") == "uploadhelper" {
		return false
	}
	under := func(root string) (string, bool) {
		if root == "" || !strings.HasPrefix(root, "/") {
			return "", false
		}
		return strings.CutPrefix(p, root)
	}
	if p == "/" && wantsDiscovery(r) {
		return false // the blobs scope's: uploads need it
	}
	if p == "/" || p == "/favicon.ico" {
		return isGet
	}
	if action, ok := under(rt.blob + "camli/"); ok {
		return action == "enumerate-blobs" && isGet
	}
	if suffix, ok := under(rt.search + "camli/search/"); ok {
		return (isGet && searchGets[suffix]) || (isPost && searchPosts[suffix])
	}
	if suffix, ok := under(rt.sign); ok {
		// GETs are its discovery and public key; verify checks a
		// signature. Signing is for a future write scope.
		return isGet || (isPost && suffix == "camli/sig/verify")
	}
	if suffix, ok := under(rt.ui); ok {
		if strings.HasPrefix(suffix, "importshare") {
			return false // imports a share into the archive
		}
		if strings.HasPrefix(suffix, "download/") {
			return isGet || isPost // POST zips several files
		}
		return isGet
	}
	if _, ok := under(rt.status); ok {
		return isGet
	}
	if _, ok := under(rt.help); ok {
		return isGet
	}
	return false
}

// scopeOf returns the scope of a node, from Config.Scope.
func (s *Server) scopeOf(peer api.NodeKey) string {
	if s.cfg.Scope == nil {
		return ScopeBlobs
	}
	return s.cfg.Scope(peer)
}

// filter enforces node scopes on tunnelled requests, and keeps every
// request without full access away from handlers that check no operation,
// such as /debug/.
func (s *Server) filter(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g, _ := auth.GrantCtxKey.ValueOk(r.Context())
		if g != auth.OpAll && strings.HasPrefix(r.URL.Path, "/debug/") {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		peer, fromNode := r.Context().Value(peerKeyCtx{}).(api.NodeKey)
		if !fromNode {
			h.ServeHTTP(w, r)
			return
		}
		scope := s.scopeOf(peer)
		switch {
		case scope != ScopeBlobs && scope != ScopeRead:
			// No longer approved, or a scope this server doesn't know.
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		case !s.roots.needsRead(r):
			// The blobs scope: whatever nodeGrant allows.
		case scope == ScopeRead:
			// The policy decided; let the handlers' own checks pass.
			r = r.WithContext(auth.GrantCtxKey.WithValue(r.Context(), auth.OpAll))
		default:
			http.Error(w, "this node's Perkeep scope is blobs, which does not include search or the web UI (on the server: keepsync server grant <name> read)", http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// wantsDiscovery reports whether r asks for the server's discovery JSON,
// as perkeep.org/pkg/server's root handler decides.
func wantsDiscovery(r *http.Request) bool {
	return r.Header.Get("Accept") == "text/x-camli-configuration" ||
		r.URL.Query().Get("camli.mode") == "config"
}
