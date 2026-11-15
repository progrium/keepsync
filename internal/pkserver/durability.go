package pkserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
)

// watermark tracks which blobs are in the bucket.
//
// A blob is durable once it was seen in the diskpacked replica before a
// ZeroFS flush started, and that flush then succeeded. gen counts flush
// starts; seen records the gen at which a blob was first seen in the
// replica; done is the gen of the latest flush that succeeded. So a blob is
// durable iff seen < done.
//
// Only blobs someone asked about are tracked: Durable queues the ones not
// seen yet, and Flush stats the queue in the replica just before it
// flushes, so a blob asked about is durable after one flush once the
// asynchronous replication has copied it.
type watermark struct {
	mu      sync.Mutex
	gen     uint64
	done    uint64
	seen    map[blob.Ref]uint64
	pending map[blob.Ref]bool
	deps    map[blob.Ref][]blob.Ref // the blobs each schema blob references
	durable map[blob.Ref]bool       // refs whose whole closure is durable
}

func (w *watermark) init() {
	w.seen = map[blob.Ref]uint64{}
	w.pending = map[blob.Ref]bool{}
	w.deps = map[blob.Ref][]blob.Ref{}
	w.durable = map[blob.Ref]bool{}
}

// Durable reports, for each ref, whether it and every blob it references
// are in the bucket. A ref the server doesn't have is not durable.
//
// Referenced blobs are: a file's or bytes' parts, recursively; a claim's
// permanodes; and, for a claim or permanode, the server's public key, which
// a reindex needs to verify them.
func (s *Server) Durable(ctx context.Context, refs []api.BlobRef) ([]bool, error) {
	out := make([]bool, len(refs))
	for i, r := range refs {
		br, ok := blob.Parse(string(r))
		if !ok {
			return nil, fmt.Errorf("pkserver: bad blobref %q", r)
		}
		d, err := s.closureDurable(ctx, br)
		if err != nil {
			return nil, err
		}
		out[i] = d
	}
	return out, nil
}

func (s *Server) closureDurable(ctx context.Context, root blob.Ref) (bool, error) {
	w := &s.wm
	w.mu.Lock()
	if w.durable[root] {
		w.mu.Unlock()
		return true, nil
	}
	w.mu.Unlock()

	all := true
	visited := map[blob.Ref]bool{}
	stack := []blob.Ref{root}
	for len(stack) > 0 {
		br := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[br] {
			continue
		}
		visited[br] = true
		deps, err := s.depsOf(ctx, br)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil // not on the server at all
		}
		if err != nil {
			return false, err
		}
		stack = append(stack, deps...)

		w.mu.Lock()
		g, ok := w.seen[br]
		if !ok {
			w.pending[br] = true
		}
		if !ok || g >= w.done {
			all = false
		}
		w.mu.Unlock()
	}
	if all {
		w.mu.Lock()
		w.durable[root] = true
		w.mu.Unlock()
	}
	return all, nil
}

// depsOf returns the blobs that br references, fetching and parsing it from
// the primary storage the first time.
func (s *Server) depsOf(ctx context.Context, br blob.Ref) ([]blob.Ref, error) {
	w := &s.wm
	w.mu.Lock()
	deps, ok := w.deps[br]
	w.mu.Unlock()
	if ok {
		return deps, nil
	}
	rc, _, err := s.bs.Fetch(ctx, br)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(rc, 16<<20))
	rc.Close()
	if err != nil {
		return nil, err
	}
	deps = []blob.Ref{}
	if schema.LikelySchemaBlob(data) {
		if b, err := schema.BlobFromReader(br, bytes.NewReader(data)); err == nil {
			deps = s.schemaDeps(b)
		}
	}
	w.mu.Lock()
	w.deps[br] = deps
	w.mu.Unlock()
	return deps, nil
}

func (s *Server) schemaDeps(b *schema.Blob) []blob.Ref {
	var deps []blob.Ref
	switch b.Type() {
	case schema.TypeFile, schema.TypeBytes:
		for _, p := range b.ByteParts() {
			if p.BlobRef.Valid() {
				deps = append(deps, p.BlobRef)
			}
			if p.BytesRef.Valid() {
				deps = append(deps, p.BytesRef)
			}
		}
	case schema.TypePermanode:
		deps = append(deps, s.pubRef)
	case schema.TypeClaim:
		deps = append(deps, s.pubRef)
		c, _ := b.AsClaim()
		if pn := c.ModifiedPermanode(); pn.Valid() {
			deps = append(deps, pn)
		}
		if acs, err := c.AttributeClaims(); err == nil {
			for _, ac := range acs {
				if ac.Permanode.Valid() {
					deps = append(deps, ac.Permanode)
				}
			}
		}
	}
	return deps
}

// Flush makes everything written so far durable: it flushes ZeroFS and,
// once that succeeds, advances the watermark over the blobs that were in
// the replica before the flush started.
func (s *Server) Flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	w := &s.wm

	w.mu.Lock()
	pending := make([]blob.Ref, 0, len(w.pending))
	for br := range w.pending {
		pending = append(pending, br)
	}
	w.mu.Unlock()

	present := map[blob.Ref]bool{}
	for len(pending) > 0 {
		n := min(len(pending), 1000)
		batch := pending[:n]
		pending = pending[n:]
		err := s.replica.StatBlobs(ctx, batch, func(sb blob.SizedRef) error {
			present[sb.Ref] = true
			return nil
		})
		if err != nil {
			return fmt.Errorf("pkserver: stat replica: %w", err)
		}
	}

	w.mu.Lock()
	for br := range present {
		if _, ok := w.seen[br]; !ok {
			w.seen[br] = w.gen
		}
		delete(w.pending, br)
	}
	w.gen++
	start := w.gen
	w.mu.Unlock()

	if err := s.cfg.ZeroFS.Flush(ctx); err != nil {
		return fmt.Errorf("pkserver: flush: %w", err)
	}

	w.mu.Lock()
	if start > w.done {
		w.done = start
	}
	w.mu.Unlock()
	return nil
}

// Drain waits until the asynchronous replication to the diskpacked replica
// has copied everything it has queued, so that a Flush after it puts every
// blob stored so far in the bucket. A clean stop is Drain, Flush, Close.
func (s *Server) Drain(ctx context.Context) error {
	prefix := "/sync-to-" + replicaName + "/"
	for {
		n, err := s.syncBacklog(ctx, prefix)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pkserver: drain: %d blobs still to replicate: %w", n, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// syncBacklog returns how many blobs the sync handler at prefix has yet to
// copy, from perkeepd's status handler.
func (s *Server) syncBacklog(ctx context.Context, prefix string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/status/status.json", nil)
	if err != nil {
		return 0, err
	}
	res, err := s.owner.Do(req)
	if err != nil {
		return 0, fmt.Errorf("pkserver: status: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("pkserver: status: %s", res.Status)
	}
	var st struct {
		Sync map[string]struct {
			BlobsToCopy int `json:"blobsToCopy"`
		} `json:"sync"`
	}
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		return 0, fmt.Errorf("pkserver: status: %w", err)
	}
	sync, ok := st.Sync[prefix]
	if !ok {
		return 0, fmt.Errorf("pkserver: status has no sync %s", prefix)
	}
	return sync.BlobsToCopy, nil
}
