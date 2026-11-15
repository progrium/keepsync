package server

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/kit"
	"github.com/progrium/keepsync/internal/registry"
)

// Control socket methods. The CLI calls these by name.
const (
	MethodAdd    = "server.add"
	MethodRemove = "server.remove"
	MethodNodes  = "server.nodes"
	MethodStatus = "server.status"
)

// AddParams is the body of MethodAdd.
type AddParams struct {
	Key  api.NodeKey `json:"key"`
	Name string      `json:"name,omitempty"`
}

// RemoveParams is the body of MethodRemove.
type RemoveParams struct {
	Name string `json:"name"` // name or key
}

// Status is the result of MethodStatus.
type Status struct {
	Addr      api.Addr            `json:"addr"`
	Key       api.NodeKey         `json:"key"`
	Nodes     []registry.NodeInfo `json:"nodes"`
	Folders   []api.Folder        `json:"folders"`
	Version   uint64              `json:"version"`
	LastFlush time.Time           `json:"lastFlush,omitzero"`
	FlushErr  string              `json:"flushErr,omitempty"`
	ZeroFSDir string              `json:"zerofsDir"`
}

func (s *Server) handlers() {
	s.baselineHandler()
	ready := func() error {
		if !s.up.Load() {
			return errors.New("server is still starting")
		}
		return nil
	}
	ctl.HandleFunc(s.ctl, MethodAdd, func(ctx context.Context, p AddParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return nil, s.reg.Add(ctx, p.Key, p.Name)
	})
	ctl.HandleFunc(s.ctl, MethodRemove, func(ctx context.Context, p RemoveParams) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return nil, s.reg.Remove(ctx, p.Name)
	})
	ctl.HandleFunc(s.ctl, MethodNodes, func(ctx context.Context, _ struct{}) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		return s.reg.Nodes(), nil
	})
	ctl.HandleFunc(s.ctl, MethodStatus, func(ctx context.Context, _ struct{}) (any, error) {
		if err := ready(); err != nil {
			return nil, err
		}
		m := s.reg.Membership()
		st := Status{Addr: s.tr.Addr(), Key: s.tr.Key(), Nodes: s.reg.Nodes(), Folders: m.Folders, Version: m.Version, ZeroFSDir: s.zfs.Dir()}
		s.mu.Lock()
		st.LastFlush = s.lastFlush
		if s.flushErr != nil {
			st.FlushErr = s.flushErr.Error()
		}
		s.mu.Unlock()
		return st, nil
	})
}

// MethodBaseline drains, flushes and writes a drill baseline (kit.Snapshot)
// of the live server to a file.
const MethodBaseline = "server.baseline"

// BaselineParams is the body of MethodBaseline.
type BaselineParams struct {
	Path string `json:"path"`
}

func (s *Server) baselineHandler() {
	ctl.HandleFunc(s.ctl, MethodBaseline, func(ctx context.Context, p BaselineParams) (any, error) {
		if !s.up.Load() {
			return nil, errors.New("server is still starting")
		}
		if p.Path == "" {
			return nil, errors.New("a path is required")
		}
		if err := s.pk.Drain(ctx); err != nil {
			return nil, err
		}
		s.flush(ctx)
		s.mu.Lock()
		ferr := s.flushErr
		s.mu.Unlock()
		if ferr != nil {
			return nil, ferr
		}
		b, err := kit.Snapshot(ctx, s.pk)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		return nil, writeFileAtomic(p.Path, data)
	})
}
