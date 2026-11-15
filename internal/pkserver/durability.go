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
	"path/filepath"
	"sync"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
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

	restarts := s.zfsRestarts()
	if restarts != s.restarts {
		if err := s.afterRestart(ctx); err != nil {
			return err
		}
		s.restarts = restarts
		s.claimMu.Lock()
		s.stashChecked = false
		s.claimMu.Unlock()
	}
	// The signed blobs stashed so far are in the blob store (put back
	// above if a restart lost them), so this flush covers them.
	stashed, err := s.checkStash(ctx)
	if err != nil {
		return err
	}

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
	if s.zfsRestarts() != restarts {
		// What was stat'd above may have died with the old process, and
		// this flush only covers the new one. The next Flush recovers.
		return errors.New("pkserver: flush: zerofs restarted during the flush")
	}

	w.mu.Lock()
	if start > w.done {
		w.done = start
	}
	w.mu.Unlock()
	s.unstash(stashed)
	return nil
}

// Drain waits until the asynchronous replication to the diskpacked replica
// has copied everything it has queued, so that a Flush after it puts every
// blob stored so far in the bucket. A clean stop is Drain, Flush, Close.
//
// In cache mode blobs are written straight to ZeroFS, so there is nothing
// to drain.
func (s *Server) Drain(ctx context.Context) error {
	if s.cacheMode() {
		return nil
	}
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

// restartCounter is implemented by zerofs.Supervisor.
type restartCounter interface{ Restarts() uint64 }

// zfsRestarts returns how many times ZeroFS died and was started again, or
// 0 if the ZeroFS in use doesn't say.
func (s *Server) zfsRestarts() uint64 {
	if rc, ok := s.cfg.ZeroFS.(restartCounter); ok {
		return rc.Restarts()
	}
	return 0
}

// afterRestart repairs the bookkeeping after ZeroFS died and came back.
// Writes it had not flushed are gone, so blobs seen in the replica since
// the last successful flush are forgotten (Durable reports them again only
// once a later flush covers them), and in store mode every blob in the
// local primary that the replica lost is copied to it again: perkeepd's
// replication already counted those as copied and won't retry them. In
// cache mode the primary is the store on ZeroFS, so lost blobs are gone
// from the server; nodes still hold them, since they were never reported
// durable.
func (s *Server) afterRestart(ctx context.Context) error {
	w := &s.wm
	w.mu.Lock()
	forgot := 0
	for br, g := range w.seen {
		if g >= w.done {
			delete(w.seen, br)
			w.pending[br] = true
			forgot++
		}
	}
	w.mu.Unlock()
	if s.cacheMode() {
		// Signed blobs are put back by checkStash; content comes back
		// when a keeper puts its file again before expiring it.
		s.logf("pkserver: zerofs restarted; %d blobs it had not flushed are no longer counted as stored", forgot)
		return nil
	}
	copied, err := s.repairReplica(ctx)
	if err != nil {
		return fmt.Errorf("pkserver: repairing the replica after a zerofs restart: %w", err)
	}
	s.logf("pkserver: zerofs restarted; copied %d blobs it lost back to the replica", copied)
	return nil
}

// repairReplica copies every blob in the primary storage that the replica
// doesn't have, and returns how many it copied.
func (s *Server) repairReplica(ctx context.Context) (int, error) {
	copied := 0
	var batch []blob.Ref
	check := func() error {
		have := map[blob.Ref]bool{}
		err := s.replica.StatBlobs(ctx, batch, func(sb blob.SizedRef) error {
			have[sb.Ref] = true
			return nil
		})
		if err != nil {
			return err
		}
		for _, br := range batch {
			if have[br] {
				continue
			}
			rc, _, err := s.bs.Fetch(ctx, br)
			if err != nil {
				return err
			}
			_, err = s.replica.ReceiveBlob(ctx, br, rc)
			rc.Close()
			if err != nil {
				return err
			}
			copied++
		}
		batch = batch[:0]
		return nil
	}
	err := blobserver.EnumerateAll(ctx, s.bs, func(sb blob.SizedRef) error {
		batch = append(batch, sb.Ref)
		if len(batch) == 1000 {
			return check()
		}
		return nil
	})
	if err == nil && len(batch) > 0 {
		err = check()
	}
	return copied, err
}

// In cache mode the blob store is on ZeroFS, so a claim or permanode the
// server signed exists only there until a flush covers it, and a ZeroFS
// that dies first loses it. Nodes can put content again, but only the
// server can sign, and a fresh signature is a different blob from the one
// the index and the tree already name. So every blob the server signs is
// stashed in Dir/unflushed until a flush covers it, and put back from
// there when ZeroFS restarted.
const stashDir = "unflushed"

// stashSigned keeps a copy of a signed blob just stored, in cache mode.
// The caller holds claimMu.
func (s *Server) stashSigned(ref blob.Ref, signed string) error {
	if !s.cacheMode() {
		return nil // the local primary holds it; repairReplica covers it
	}
	dir := filepath.Join(s.cfg.Dir, stashDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("pkserver: stash: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("pkserver: stash: %w", err)
	}
	_, err = tmp.WriteString(signed)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(dir, ref.String()))
	}
	if err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("pkserver: stash: %w", err)
	}
	return nil
}

// checkStash lists the stashed blobs and returns them. Unless they were
// checked since ZeroFS last restarted (and since this server started, in
// case it stopped with blobs stashed), it first puts back any the blob
// store lacks. It holds claimMu so that no blob is stored but not yet
// stashed meanwhile.
func (s *Server) checkStash(ctx context.Context) ([]blob.Ref, error) {
	if !s.cacheMode() {
		return nil, nil
	}
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	dir := filepath.Join(s.cfg.Dir, stashDir)
	ents, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("pkserver: stash: %w", err)
	}
	var refs []blob.Ref
	for _, e := range ents {
		if br, ok := blob.Parse(e.Name()); ok {
			refs = append(refs, br)
		}
	}
	if s.stashChecked || len(refs) == 0 {
		s.stashChecked = true
		return refs, nil
	}
	have := map[blob.Ref]bool{}
	for start := 0; start < len(refs); start += 1000 {
		err := s.bs.StatBlobs(ctx, refs[start:min(start+1000, len(refs))], func(sb blob.SizedRef) error {
			have[sb.Ref] = true
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("pkserver: stash: %w", err)
		}
	}
	put := 0
	for _, br := range refs {
		if have[br] {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, br.String()))
		if err != nil {
			return nil, fmt.Errorf("pkserver: stash: %w", err)
		}
		if _, err := s.bs.ReceiveBlob(ctx, br, bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("pkserver: putting back signed blob %s: %w", br, err)
		}
		put++
	}
	if put > 0 {
		s.logf("pkserver: put back %d signed blobs zerofs lost", put)
	}
	s.stashChecked = true
	return refs, nil
}

// unstash drops stashed blobs that a successful flush covered.
func (s *Server) unstash(refs []blob.Ref) {
	dir := filepath.Join(s.cfg.Dir, stashDir)
	for _, br := range refs {
		if err := os.Remove(filepath.Join(dir, br.String())); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logf("pkserver: stash: %v", err)
		}
	}
}
