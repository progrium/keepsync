package syncer

import (
	"errors"
	"path/filepath"

	"github.com/syncthing/syncthing/lib/protocol"

	"github.com/progrium/keepsync/internal/api"
)

// FileState reports path in Syncthing's global index, and whether this
// node's own index entry holds that global version.
//
// An unknown folder or path is reported Known=false with no error. A path
// that is deleted globally and absent locally counts as current, the same
// way Syncthing does not need it.
func (s *Syncer) FileState(folderID, path string) (api.FileState, error) {
	app := s.appRef.Load()
	if app == nil {
		return api.FileState{}, errors.New("syncer: not running")
	}
	// Start sets wrapper before publishing appRef, so it is safe to read.
	if _, ok := s.wrapper.Folder(folderID); !ok {
		return api.FileState{}, nil
	}
	name := filepath.ToSlash(filepath.Clean(path))

	global, ok, err := app.Internals.GlobalFileInfo(folderID, name)
	if err != nil {
		return api.FileState{}, err
	}
	if !ok {
		return api.FileState{}, nil
	}
	st := api.FileState{
		Known:   true,
		Version: versionOf(global.Version),
		Deleted: global.IsDeleted(),
		Dir:     global.IsDirectory(),
	}

	local, ok, err := app.Internals.LocalFileInfo(folderID, name)
	if err != nil {
		return api.FileState{}, err
	}
	switch {
	case !ok:
		st.Current = global.IsDeleted()
	case local.IsInvalid():
		st.Current = false
	default:
		st.Current = local.Version.Equal(global.Version) && local.IsDeleted() == global.IsDeleted()
	}
	return st, nil
}

func versionOf(v protocol.Vector) api.Version {
	if len(v.Counters) == 0 {
		return nil
	}
	out := make(api.Version, len(v.Counters))
	for i, c := range v.Counters {
		out[i] = api.VersionCounter{ID: c.ID.String(), Value: c.Value}
	}
	return out
}
