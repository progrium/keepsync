package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Wire protocol on api.PortRegistry.
//
// Each side writes a stream of JSON values. The client sends requests and
// the server answers each with a response carrying the same ID, in any
// order, so one connection carries many concurrent calls. A "watch" request
// turns its connection into a stream: the server answers it with the current
// membership and then one more response with the same ID per change, until
// either side closes the connection.
//
// Heartbeat: the server sends {"id":0,"ping":true} on every connection
// every PingInterval, and the client answers each with a "pong" request
// (ID 0, no response). Each side closes a connection on which it has
// received nothing for its idle timeout, because a tailcat connection to a
// peer that vanished is never reset: reads just block. A node therefore
// stops counting as online within IdleTimeout of going silent, and a client
// stops waiting on a dead server within its own IdleTimeout.
//
// The caller's identity is the authenticated tailcat key of the connection,
// never a field in a request.

const (
	mRegister   = "register"
	mWatch      = "watch"
	mAddFolder  = "addFolder"
	mJoinFolder = "joinFolder"
	mKeep       = "keep"
	mClaimPath  = "claimPath"
	mPaths      = "paths"
	mPathsAt    = "paths_at"
	mMarkGone   = "mark_gone"
	mHistory    = "history"
	mDurable    = "durable"
	mLeave      = "leaveFolder"
	mUnkeep     = "unkeep"
	mPong       = "pong"
)

type request struct {
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type response struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *wireError      `json:"error,omitempty"`
	Ping   bool            `json:"ping,omitempty"`
}

type wireError struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type folderParams struct {
	Name string `json:"name"`
}

type keepParams struct {
	Folder string         `json:"folder"`
	Policy api.KeepPolicy `json:"policy"`
}

type permanodeParams struct {
	Permanode api.BlobRef `json:"permanode"`
}

type historyParams struct {
	Permanode api.BlobRef `json:"permanode"`
	Path      string      `json:"path"`
}

type pathsAtParams struct {
	Permanode api.BlobRef `json:"permanode"`
	At        time.Time   `json:"at"`
}

type refsParams struct {
	Refs []api.BlobRef `json:"refs"`
}

// Errors returned by the registry, on the server and (decoded from the wire)
// by the client, so callers can test them with errors.Is.
var (
	ErrNotFound      = errors.New("registry: not found")
	ErrExists        = errors.New("registry: already exists")
	ErrNotMember     = errors.New("registry: not a member of the folder")
	ErrInvalidPolicy = errors.New("registry: invalid keep policy")
	ErrUnknownNode   = errors.New("registry: unknown node")
	ErrBadRequest    = errors.New("registry: bad request")
	ErrClosed        = errors.New("registry: closed")
)

var errCodes = map[error]string{
	ErrNotFound:      "not_found",
	ErrExists:        "exists",
	ErrNotMember:     "not_member",
	ErrInvalidPolicy: "invalid_policy",
	ErrUnknownNode:   "unknown_node",
	ErrBadRequest:    "bad_request",
}

func toWireError(err error) *wireError {
	if err == nil {
		return nil
	}
	for e, code := range errCodes {
		if errors.Is(err, e) {
			return &wireError{Code: code, Msg: err.Error()}
		}
	}
	return &wireError{Code: "internal", Msg: err.Error()}
}

// remoteError is an error decoded from the wire. It matches its sentinel
// with errors.Is and keeps the server's message.
type remoteError struct {
	sentinel error
	msg      string
}

func (e *remoteError) Error() string { return e.msg }
func (e *remoteError) Unwrap() error { return e.sentinel }

func (w *wireError) err() error {
	for e, code := range errCodes {
		if code == w.Code {
			return &remoteError{sentinel: e, msg: w.Msg}
		}
	}
	return fmt.Errorf("registry server: %s", w.Msg)
}

// ValidatePolicy checks the keep policy invariant from the design:
// Grace < MaxDefer < Expire, with Expire == 0 meaning nothing expires.
func ValidatePolicy(p api.KeepPolicy) error {
	if p.Grace < 0 || p.MaxDefer <= p.Grace {
		return fmt.Errorf("%w: grace (%s) must be less than max-defer (%s)", ErrInvalidPolicy, p.Grace, p.MaxDefer)
	}
	if p.Expire < 0 || (p.Expire != 0 && p.Expire <= p.MaxDefer) {
		return fmt.Errorf("%w: max-defer (%s) must be less than expire (%s), or expire must be 0", ErrInvalidPolicy, p.MaxDefer, p.Expire)
	}
	return nil
}

// Default timings for the server.
const (
	DefaultKeeperTimeout = 2 * time.Minute
	DefaultPingInterval  = 10 * time.Second
	DefaultIdleTimeout   = 30 * time.Second
)

// watchdog closes conn once nothing has been received on it (as recorded in
// last, unix nanoseconds) for idle. It returns when done is closed.
func watchdog(conn net.Conn, idle time.Duration, last *atomic.Int64, done <-chan struct{}) {
	tick := time.NewTicker(idle / 4)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			if time.Since(time.Unix(0, last.Load())) > idle {
				conn.Close()
				return
			}
		}
	}
}
