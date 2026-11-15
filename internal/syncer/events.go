package syncer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"github.com/syncthing/syncthing/lib/events"

	"github.com/progrium/keepsync/internal/api"
)

// pumpEvents translates Syncthing events into api.SyncEvents until the
// subscription is closed by Stop.
func (s *Syncer) pumpEvents(sub events.Subscription) {
	defer sub.Unsubscribe()
	for {
		select {
		case <-s.ctx.Done():
			return
		case ev, ok := <-sub.C():
			if !ok {
				return
			}
			for _, out := range s.translate(ev) {
				select {
				case s.events <- out:
				case <-s.ctx.Done():
					return
				}
			}
		}
	}
}

func (s *Syncer) translate(ev events.Event) []api.SyncEvent {
	switch ev.Type {
	case events.DeviceConnected, events.DeviceDisconnected:
		id := field(ev.Data, "id")
		if id == "" {
			return nil
		}
		t := api.SyncPeerConnected
		if ev.Type == events.DeviceDisconnected {
			t = api.SyncPeerDisconnected
		}
		return []api.SyncEvent{{Type: t, Device: api.DeviceID(id), Time: ev.Time}}

	case events.LocalChangeDetected:
		if field(ev.Data, "type") != "file" {
			return nil
		}
		folder := field(ev.Data, "folder")
		path := filepath.ToSlash(field(ev.Data, "path"))
		out := []api.SyncEvent{{
			Type: api.SyncLocalChange, Folder: folder, Path: path,
			Device: s.DeviceID(), Time: ev.Time,
		}}
		if field(ev.Data, "action") != "deleted" {
			out = append(out, s.conflictEvent(folder, path, ev.Time)...)
		}
		return out

	case events.ItemFinished:
		if field(ev.Data, "type") != "file" || hasError(ev.Data) {
			return nil
		}
		folder := field(ev.Data, "folder")
		path := filepath.ToSlash(field(ev.Data, "item"))
		out := []api.SyncEvent{{
			Type: api.SyncItemChanged, Folder: folder, Path: path,
			Device: s.modifiedBy(folder, path), Time: ev.Time,
		}}
		if field(ev.Data, "action") != "delete" {
			out = append(out, s.conflictEvent(folder, path, ev.Time)...)
		}
		return out
	}
	return nil
}

// conflictEvent returns a SyncConflict event if path is a conflict copy.
// Device is the device whose losing version the copy holds, from the short
// ID in the copy's name. The vendored Syncthing names copies after the
// losing version's ModifiedBy (see third_party/syncthing/KEEPSYNC.md), so
// every node that makes a copy of the same conflict names the same device.
func (s *Syncer) conflictEvent(folder, path string, t time.Time) []api.SyncEvent {
	short, ok := ConflictDevice(path)
	if !ok {
		return nil
	}
	return []api.SyncEvent{{
		Type: api.SyncConflict, Folder: folder, Path: path,
		Device: s.deviceForShort(short), Time: t,
	}}
}

// ModifiedBy reports the device that last changed path in the global
// index (api.Syncer).
func (s *Syncer) ModifiedBy(folderID, path string) (api.DeviceID, bool) {
	d := s.modifiedBy(folderID, path)
	return d, d != ""
}

var conflictRE = regexp.MustCompile(`\.sync-conflict-\d{8}-\d{6}-([A-Z0-9]{7})`)

// IsConflict reports whether path names a Syncthing conflict copy.
func IsConflict(path string) bool {
	_, ok := ConflictDevice(path)
	return ok
}

// ConflictDevice returns the short device ID embedded in a conflict copy's
// name ("a.sync-conflict-20260101-120000-ABCDEFG.txt" -> "ABCDEFG"): the
// device whose losing version the copy holds.
func ConflictDevice(path string) (string, bool) {
	m := conflictRE.FindStringSubmatch(filepath.Base(path))
	if m == nil {
		return "", false
	}
	return m[1], true
}

// ConflictOriginal returns the path of the file a conflict copy belongs to.
func ConflictOriginal(path string) (string, bool) {
	dir, base := filepath.Split(path)
	loc := conflictRE.FindStringIndex(base)
	if loc == nil {
		return "", false
	}
	return dir + base[:loc[0]] + base[loc[1]:], true
}

// modifiedBy looks up who last changed path in the global index.
func (s *Syncer) modifiedBy(folder, path string) api.DeviceID {
	app := s.appRef.Load()
	if app == nil {
		return ""
	}
	fi, ok, err := app.Internals.GlobalFileInfo(folder, path)
	if err != nil || !ok {
		return ""
	}
	return s.deviceForShort(fi.ModifiedBy.String())
}

func (s *Syncer) deviceForShort(short string) api.DeviceID {
	return (*s.shorts.Load())[short]
}

func field(data any, key string) string {
	switch d := data.(type) {
	case map[string]string:
		return d[key]
	case map[string]any:
		switch v := d[key].(type) {
		case string:
			return v
		case nil:
			return ""
		case fmt.Stringer:
			return v.String()
		}
	}
	return ""
}

func hasError(data any) bool {
	d, ok := data.(map[string]any)
	if !ok {
		return false
	}
	e, ok := d["error"].(*string)
	return ok && e != nil
}
