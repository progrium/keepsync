// Package ctl is the control socket between keepsync CLI commands and the
// running server or node process.
//
// The running process listens on a Unix socket in its directory (owner-only)
// and serves JSON over HTTP: each method is a POST to /<method> whose body
// is the JSON parameters. A reply is the JSON result, or a JSON error object
// with a non-2xx status.
package ctl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ErrNotRunning is returned by Call when nothing is listening on the socket.
var ErrNotRunning = errors.New("ctl: process not running")

// Handler serves one method. params is the raw JSON body (may be empty).
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// Server serves methods on a control socket.
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	srv      *http.Server
	ln       net.Listener
	path     string
}

// NewServer returns a Server with no methods.
func NewServer() *Server { return &Server{handlers: map[string]Handler{}} }

// Handle registers fn for method.
func (s *Server) Handle(method string, fn Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = fn
}

// HandleFunc registers a typed handler: params decode into P.
func HandleFunc[P any](s *Server, method string, fn func(ctx context.Context, p P) (any, error)) {
	s.Handle(method, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var p P
		if len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &p); err != nil {
				return nil, fmt.Errorf("bad params: %w", err)
			}
		}
		return fn(ctx, p)
	})
}

type errorBody struct {
	Error string `json:"error"`
}

// Listen starts serving on the socket at path. A stale socket left by a
// process that is gone is removed; a live one is an error.
func (s *Server) Listen(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
		c.Close()
		return fmt.Errorf("ctl: %s is in use: is another process already running?", path)
	}
	os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return err
	}
	s.ln, s.path = ln, path
	s.srv = &http.Server{Handler: http.HandlerFunc(s.serveHTTP)}
	go s.srv.Serve(ln)
	return nil
}

// Close stops serving and removes the socket.
func (s *Server) Close() error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Close()
	os.Remove(s.path)
	return err
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	method := strings.TrimPrefix(r.URL.Path, "/")
	s.mu.RLock()
	h := s.handlers[method]
	s.mu.RUnlock()
	if r.Method != http.MethodPost || h == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errorBody{fmt.Sprintf("unknown method %q", method)})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errorBody{err.Error()})
		return
	}
	out, err := h(r.Context(), body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errorBody{err.Error()})
		return
	}
	json.NewEncoder(w).Encode(out)
}

// Call invokes method on the process listening at path, decoding the
// result into out (which may be nil).
func Call(ctx context.Context, path, method string, params, out any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	c := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		},
	}}
	defer c.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://ctl/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			return fmt.Errorf("%w (no socket at %s)", ErrNotRunning, path)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var e errorBody
		if json.NewDecoder(resp.Body).Decode(&e) == nil && e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("ctl: %s: %s", method, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
