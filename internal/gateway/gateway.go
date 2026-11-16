// Package gateway serves the server's Perkeep on a node's localhost, so
// stock Perkeep tools (the web UI, pk-get, pk-mount, the search API) can
// read the archive with no extra ports or DNS.
//
// A Gateway listens on a loopback address (DefaultListen) and proxies HTTP
// to the server's perkeepd over the node's tailcat transport, on
// api.PortPerkeep. It requires a per-node token as the password of HTTP
// basic auth, the way Perkeep's userpass auth sends it; WriteClientConfig
// writes a Perkeep client config that does. What a node may reach is
// decided on the server by its scope (see pkserver.Config.Scope): the
// default blobs scope gives the tools nothing beyond blobs, and `keepsync
// server grant <name> read` opens search and the web UI. Nothing through
// the gateway can sign or delete.
//
// See docs/design.md, "Perkeep gateway".
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// DefaultListen is where the gateway listens unless configured otherwise:
// Perkeep's usual port, on localhost only.
const DefaultListen = "127.0.0.1:3179"

// User is the basic-auth user name in the client config. The gateway
// checks only the password.
const User = "keepsync"

// upstream is the URL perkeepd is reached at through the transport. The
// host is never resolved, and matches the server's baseURL.
const upstream = "http://perkeep.keepsync"

// Config configures a Gateway.
type Config struct {
	// Listen is the loopback address to listen on. Default DefaultListen.
	// Addresses that are not loopback are refused.
	Listen string
	// Transport is the node's tailcat endpoint, and Server the server's
	// address on it. Both are required.
	Transport api.Transport
	Server    api.Addr
	// Token is the password clients must send (see LoadToken). Required.
	Token string
	// Logf, if set, receives log lines. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Gateway is a localhost HTTP proxy to the server's perkeepd.
type Gateway struct {
	cfg   Config
	logf  func(string, ...any)
	proxy *httputil.ReverseProxy
	ln    net.Listener
	hs    *http.Server
}

// New returns a Gateway for cfg. It does not listen until Start.
func New(cfg Config) (*Gateway, error) {
	if cfg.Transport == nil || cfg.Server == "" {
		return nil, errors.New("gateway: Transport and Server are required")
	}
	if len(cfg.Token) < 16 {
		return nil, errors.New("gateway: Token is required (see LoadToken)")
	}
	if cfg.Listen == "" {
		cfg.Listen = DefaultListen
	}
	if err := checkLoopback(cfg.Listen); err != nil {
		return nil, err
	}
	g := &Gateway{cfg: cfg, logf: cfg.Logf}
	if g.logf == nil {
		g.logf = log.Printf
	}
	up, _ := url.Parse(upstream)
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return cfg.Transport.Dial(ctx, cfg.Server, api.PortPerkeep)
		},
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(up)
			pr.Out.Host = up.Host
			// The token is for the gateway; perkeepd authorizes the
			// node by its tailcat key.
			pr.Out.Header.Del("Authorization")
			pr.Out.Header.Del("Cookie")
			// ServeHTTP checked it; perkeepd would compare it with its
			// own host name and refuse WebSockets.
			pr.Out.Header.Del("Origin")
		},
		Transport: tr,
		ModifyResponse: func(res *http.Response) error {
			// Point absolute redirects back at the gateway.
			if loc := res.Header.Get("Location"); strings.HasPrefix(loc, upstream) {
				res.Header.Set("Location", strings.TrimPrefix(loc, upstream))
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			g.logf("gateway: %s %s: %v", r.Method, r.URL.Path, err)
			http.Error(w, "keepsync gateway: server unreachable: "+err.Error(), http.StatusBadGateway)
		},
		ErrorLog: log.New(logWriter{g.logf}, "gateway: proxy: ", 0),
	}
	return g, nil
}

// Start listens on cfg.Listen and serves until Close.
func (g *Gateway) Start() error {
	ln, err := net.Listen("tcp", g.cfg.Listen)
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	g.ln = ln
	g.hs = &http.Server{
		Handler:           g,
		ReadHeaderTimeout: 30 * time.Second,
		ErrorLog:          log.New(logWriter{g.logf}, "gateway: http: ", 0),
	}
	go g.hs.Serve(ln)
	return nil
}

// Addr is the address the gateway listens on, once started.
func (g *Gateway) Addr() string {
	if g.ln == nil {
		return g.cfg.Listen
	}
	return g.ln.Addr().String()
}

// URL is the base URL for Perkeep clients, once started.
func (g *Gateway) URL() string { return "http://" + g.Addr() }

// Close stops listening and closes open connections.
func (g *Gateway) Close() error {
	if g.hs == nil {
		return nil
	}
	return g.hs.Close()
}

// ServeHTTP checks the request's token and origin, then proxies it.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only names that resolve to this machine: a page on another site
	// can't reach the gateway by rebinding its own DNS name to 127.0.0.1.
	if !loopbackHost(r.Host) {
		http.Error(w, "keepsync gateway: bad Host", http.StatusForbidden)
		return
	}
	// Browsers resend saved basic-auth credentials for cross-site
	// requests too. A cross-site WebSocket could read the results, so
	// refuse any request whose Origin is another site.
	if o := r.Header.Get("Origin"); o != "" {
		u, err := url.Parse(o)
		if err != nil || u.Host != r.Host {
			http.Error(w, "keepsync gateway: cross-origin request refused", http.StatusForbidden)
			return
		}
	}
	if _, pass, ok := r.BasicAuth(); !ok || subtle.ConstantTimeCompare([]byte(pass), []byte(g.cfg.Token)) != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="keepsync gateway (see keepsync gateway config)"`)
		http.Error(w, "keepsync gateway: unauthorized", http.StatusUnauthorized)
		return
	}
	if !readRequest(r) {
		http.Error(w, "keepsync gateway: read-only (writes through the gateway are not supported yet)", http.StatusForbidden)
		return
	}
	g.proxy.ServeHTTP(w, r)
}

// readPosts are the POST endpoints that only read: search queries and
// describes, blob stats, signature checks, and multi-file downloads.
var readPosts = []string{
	"/camli/search/query", "/camli/search/describe", "/camli/stat",
	"/camli/sig/verify",
}

// readRequest reports whether r is one the gateway passes on. The server
// enforces the node's scope as well; this keeps the gateway read-only
// even for blobs, which the node's own scope could upload.
func readRequest(r *http.Request) bool {
	if r.URL.Query().Get("camli.mode") == "uploadhelper" {
		return false
	}
	switch r.Method {
	case "GET", "HEAD":
		return true
	case "POST":
		for _, suffix := range readPosts {
			if strings.HasSuffix(r.URL.Path, suffix) {
				return true
			}
		}
		return strings.Contains(r.URL.Path, "/download/")
	}
	return false
}

func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("gateway: listen %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("gateway: listen %q: only loopback addresses are allowed", addr)
}

func loopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// LoadToken returns the gateway token stored at path, creating a random
// one (mode 0600) if there is none yet.
func LoadToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		if t := strings.TrimSpace(string(b)); len(t) >= 16 {
			return t, nil
		}
		return "", fmt.Errorf("gateway: token file %s is too short", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	t := hex.EncodeToString(raw)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return LoadToken(path) // lost a race with another process
	}
	if err != nil {
		return "", err
	}
	if _, err := f.WriteString(t + "\n"); err != nil {
		f.Close()
		return "", err
	}
	return t, f.Close()
}

type logWriter struct{ logf func(string, ...any) }

func (w logWriter) Write(p []byte) (int, error) {
	w.logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
