package api

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Version is a Syncthing version vector: one counter per device that has
// changed a file, keyed by the device's short ID. It records which edits a
// version descends from, independent of any clock, so it tells a new
// version from a stale one where mtimes cannot (see docs/design.md, "Kept
// tree layout").
type Version []VersionCounter

// VersionCounter is one device's counter in a Version.
type VersionCounter struct {
	ID    string `json:"id"` // short device ID
	Value uint64 `json:"v"`
}

// Ordering is how two versions relate.
type Ordering int

const (
	Equal      Ordering = iota
	Newer               // the receiver descends from the other
	Older               // the other descends from the receiver
	Concurrent          // neither descends from the other
)

func (o Ordering) String() string {
	switch o {
	case Equal:
		return "equal"
	case Newer:
		return "newer"
	case Older:
		return "older"
	default:
		return "concurrent"
	}
}

// Compare reports how v relates to w. A missing counter counts as 0.
func (v Version) Compare(w Version) Ordering {
	vm, wm := v.counters(), w.counters()
	greater, less := false, false
	for id, a := range vm {
		if b := wm[id]; a > b {
			greater = true
		} else if a < b {
			less = true
		}
	}
	for id, b := range wm {
		if _, ok := vm[id]; !ok && b > 0 {
			less = true
		}
	}
	switch {
	case greater && less:
		return Concurrent
	case greater:
		return Newer
	case less:
		return Older
	}
	return Equal
}

// DescendsFrom reports whether v is w or a later version of it.
func (v Version) DescendsFrom(w Version) bool {
	o := v.Compare(w)
	return o == Equal || o == Newer
}

func (v Version) counters() map[string]uint64 {
	m := make(map[string]uint64, len(v))
	for _, c := range v {
		if c.Value > m[c.ID] {
			m[c.ID] = c.Value
		}
	}
	return m
}

// String encodes v as "ID:value,ID:value", sorted by ID, which is how it
// is stored in claims.
func (v Version) String() string {
	m := v.counters()
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id + ":" + strconv.FormatUint(m[id], 10)
	}
	return strings.Join(parts, ",")
}

// ParseVersion decodes String's encoding. The empty string is the empty
// version.
func ParseVersion(s string) (Version, error) {
	if s == "" {
		return nil, nil
	}
	var v Version
	for _, p := range strings.Split(s, ",") {
		id, n, ok := strings.Cut(p, ":")
		if !ok || id == "" {
			return nil, fmt.Errorf("api: bad version counter %q", p)
		}
		val, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("api: bad version counter %q", p)
		}
		v = append(v, VersionCounter{ID: id, Value: val})
	}
	return v, nil
}

// Supersedes reports whether claim c may replace the kept entry cur at the
// same path (its content differs). Versions decide when both are known:
// c must descend from cur. Concurrent versions, or a missing version on
// either side, fall back to the newer-mtime rule.
func Supersedes(c PathClaim, cur PathEntry) bool {
	if len(c.Version) > 0 && len(cur.Version) > 0 {
		switch c.Version.Compare(cur.Version) {
		case Newer:
			return true
		case Older, Equal:
			return false
		}
	}
	return c.ModTime.After(cur.ModTime)
}
