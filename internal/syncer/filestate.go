package syncer

import "github.com/progrium/keepsync/internal/api"

// FileState reports path in Syncthing's global index. Until it reads the
// index, every path is reported known and current with no version, which
// keeps the keeper's previous behaviour (the mtime rule).
func (s *Syncer) FileState(folderID, path string) (api.FileState, error) {
	return api.FileState{Known: true, Current: true}, nil
}
