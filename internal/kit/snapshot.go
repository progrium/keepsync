package kit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/schema"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/pkserver"
)

// Baseline is what a drill compares the restored server against: the live
// server's blobs, kept trees and file contents. The live server makes it
// with Snapshot (perkeepd runs once per process, so the drill cannot open
// the live server itself) and the CLI hands it to Drill.
type Baseline struct {
	Taken time.Time `json:"taken"`
	// Blobs is every blobref with its size, sorted.
	Blobs []SizedRef `json:"blobs"`
	// Permanodes is every permanode blob, sorted.
	Permanodes []api.BlobRef `json:"permanodes"`
	// Files is every kept path's current file, by "<permanode>/<path>".
	Files map[string]FileSum `json:"files"`
}

// SizedRef is a blobref and its size.
type SizedRef struct {
	Ref  api.BlobRef `json:"ref"`
	Size uint32      `json:"size"`
}

// FileSum is a kept file: its file schema blob and the SHA-256 of its
// contents.
type FileSum struct {
	File   api.BlobRef `json:"file"`
	Size   int64       `json:"size"`
	SHA256 string      `json:"sha256"`
}

// scan is the content of a server, read through an owner client.
type scan struct {
	Baseline
	BadBlobs []api.BlobRef // blobs that failed their hash check
	Unread   []string      // paths whose file could not be read
}

// Snapshot reads the whole of srv into a Baseline. It reads every blob, so
// it costs as much as a full read of the store; run it from the live
// server process when the drill starts.
func Snapshot(ctx context.Context, srv *pkserver.Server) (*Baseline, error) {
	c, err := ownerClient(srv, "")
	if err != nil {
		return nil, err
	}
	s, err := scanServer(ctx, srv, c, 8)
	if err != nil {
		return nil, err
	}
	if len(s.BadBlobs) > 0 {
		return nil, fmt.Errorf("kit: %d blobs on the live server fail their hash check, first %s", len(s.BadBlobs), s.BadBlobs[0])
	}
	if len(s.Unread) > 0 {
		return nil, fmt.Errorf("kit: %d kept files on the live server cannot be read, first %s", len(s.Unread), s.Unread[0])
	}
	return &s.Baseline, nil
}

// scanServer reads every blob of the server behind c, verifying each hash,
// finds the permanodes, reads their trees with srv.Paths and hashes every
// kept file.
func scanServer(ctx context.Context, srv *pkserver.Server, c *client.Client, workers int) (*scan, error) {
	s := &scan{Baseline: Baseline{Taken: time.Now().UTC(), Files: map[string]FileSum{}}}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	refs := make(chan blob.SizedRef, 256)
	var enumErr error
	go func() { enumErr = c.EnumerateBlobs(ctx, refs, "", -1) }()
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sb := range refs {
				b, err := fetchVerified(ctx, c, sb.Ref)
				mu.Lock()
				s.Blobs = append(s.Blobs, SizedRef{api.BlobRef(sb.Ref.String()), sb.Size})
				switch {
				case errors.Is(err, errBadHash):
					s.BadBlobs = append(s.BadBlobs, api.BlobRef(sb.Ref.String()))
				case err != nil:
					if firstErr == nil {
						firstErr = fmt.Errorf("kit: reading %s: %w", sb.Ref, err)
						cancel()
					}
				case isPermanode(sb.Ref, b):
					s.Permanodes = append(s.Permanodes, api.BlobRef(sb.Ref.String()))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if enumErr != nil {
		return nil, fmt.Errorf("kit: listing blobs: %w", enumErr)
	}
	sort.Slice(s.Blobs, func(i, j int) bool { return s.Blobs[i].Ref < s.Blobs[j].Ref })
	sort.Slice(s.Permanodes, func(i, j int) bool { return s.Permanodes[i] < s.Permanodes[j] })

	for _, pn := range s.Permanodes {
		tree, err := srv.Paths(ctx, pn)
		if err != nil {
			// A permanode with no claims has no tree. pkserver reports
			// that as unknown; anything else is a real failure.
			if strings.Contains(err.Error(), "unknown permanode") {
				continue
			}
			return nil, err
		}
		for p, e := range tree {
			key := string(pn) + "/" + p
			fs, err := hashFile(ctx, c, e.File)
			if err != nil {
				s.Unread = append(s.Unread, key)
				continue
			}
			s.Files[key] = fs
		}
	}
	sort.Strings(s.Unread)
	return s, nil
}

func isPermanode(br blob.Ref, b []byte) bool {
	if len(b) == 0 || b[0] != '{' || len(b) > schema.MaxSchemaBlobSize {
		return false
	}
	sb, err := schema.BlobFromReader(br, strings.NewReader(string(b)))
	return err == nil && sb.Type() == schema.TypePermanode
}

func hashFile(ctx context.Context, f blob.Fetcher, file api.BlobRef) (FileSum, error) {
	br, ok := blob.Parse(string(file))
	if !ok {
		return FileSum{}, fmt.Errorf("bad fileref %q", file)
	}
	fr, err := schema.NewFileReader(ctx, f, br)
	if err != nil {
		return FileSum{}, err
	}
	defer fr.Close()
	h := sha256.New()
	n, err := io.Copy(h, fr)
	if err != nil {
		return FileSum{}, err
	}
	return FileSum{File: file, Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}
