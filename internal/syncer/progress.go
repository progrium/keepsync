package syncer

import (
	"errors"
	"fmt"

	"github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/protocol"

	"github.com/progrium/keepsync/internal/api"
)

var _ api.SyncStatusReporter = (*Syncer)(nil)

// FolderSync reports the folder's state and how much this node and each
// peer sharing the folder still need.
func (s *Syncer) FolderSync(folderID string) (api.FolderSync, error) {
	app := s.appRef.Load()
	if app == nil {
		return api.FolderSync{}, errors.New("syncer: not running")
	}
	// Start sets wrapper before publishing appRef, so it is safe to read.
	fc, ok := s.wrapper.Folder(folderID)
	if !ok {
		return api.FolderSync{}, fmt.Errorf("syncer: unknown folder %q", folderID)
	}
	in := app.Internals
	var out api.FolderSync
	state, since, err := in.FolderState(folderID)
	out.State, out.StateSince = state, since
	if err != nil {
		out.State, out.Error = "error", err.Error()
	}
	local, err := in.Completion(s.id, folderID)
	if err != nil {
		return out, fmt.Errorf("syncer: folder %q: %w", folderID, err)
	}
	out.Local = completion(local)
	// Bytes of partly pulled files that are already fetched.
	if done := in.FolderProgressBytesCompleted(folderID); done > 0 {
		out.Local.NeedBytes = max(0, out.Local.NeedBytes-done)
	}
	for _, d := range fc.DeviceIDs() {
		if d == s.id {
			continue
		}
		ps := api.PeerSync{Device: api.DeviceID(d.String())}
		if c, err := in.Completion(d, folderID); err == nil {
			ps.Known = c.RemoteState.String() == "valid"
			ps.SyncCompletion = completion(c)
		}
		out.Peers = append(out.Peers, ps)
	}
	return out, nil
}

func completion(c model.FolderCompletion) api.SyncCompletion {
	return api.SyncCompletion{
		Pct: c.CompletionPct, GlobalBytes: c.GlobalBytes, GlobalItems: c.GlobalItems,
		NeedBytes: c.NeedBytes, NeedItems: c.NeedItems, NeedDeletes: c.NeedDeletes,
	}
}

// NeedFiles lists up to limit files this node still needs, in the order
// the puller takes them, and how many are needed in all.
func (s *Syncer) NeedFiles(folderID string, limit int) ([]api.NeedFile, int, error) {
	app := s.appRef.Load()
	if app == nil {
		return nil, 0, errors.New("syncer: not running")
	}
	if _, ok := s.wrapper.Folder(folderID); !ok {
		return nil, 0, fmt.Errorf("syncer: unknown folder %q", folderID)
	}
	in := app.Internals
	total := 0
	if c, err := in.NeedSize(folderID, protocol.LocalDeviceID); err == nil {
		total = c.Files + c.Directories + c.Symlinks + c.Deleted
	}
	if limit <= 0 {
		return nil, total, nil
	}
	progress, queued, rest, err := in.NeedFolderFiles(folderID, 1, limit)
	if err != nil {
		return nil, total, fmt.Errorf("syncer: folder %q: %w", folderID, err)
	}
	var out []api.NeedFile
	for _, g := range []struct {
		stage string
		files []protocol.FileInfo
	}{{"pulling", progress}, {"queued", queued}, {"waiting", rest}} {
		for _, f := range g.files {
			if len(out) == limit {
				break
			}
			out = append(out, api.NeedFile{
				Path: f.Name, Size: f.Size, Deleted: f.IsDeleted(), Dir: f.IsDirectory(), Stage: g.stage,
			})
		}
	}
	return out, max(total, len(out)), nil
}
