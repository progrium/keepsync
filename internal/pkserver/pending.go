package pkserver

import (
	"context"
	"errors"

	"github.com/progrium/keepsync/internal/api"
)

// errNotYet marks api.Signer methods added for design decisions 1–4 that
// the signer does not implement yet.
var errNotYet = errors.New("pkserver: not implemented yet")

// MarkGone records why a kept file left sync. Not implemented yet.
func (s *Server) MarkGone(ctx context.Context, g api.GoneClaim) error { return errNotYet }

// History returns the content versions kept at path. Not implemented yet.
func (s *Server) History(ctx context.Context, permanode api.BlobRef, path string) ([]api.PathEntry, error) {
	return nil, errNotYet
}
