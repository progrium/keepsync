package syncer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/progrium/keepsync/internal/api"
)

func (n *node) state(path string) api.FileState {
	n.t.Helper()
	st, err := n.s.FileState("docs", path)
	if err != nil {
		n.t.Fatal(err)
	}
	return st
}

func TestFileState(t *testing.T) {
	a, b := pair(t)

	if st := a.state("nope.txt"); st.Known {
		t.Errorf("unknown path reported known: %+v", st)
	}
	if st, err := a.s.FileState("nofolder", "x"); err != nil || st.Known {
		t.Errorf("unknown folder: %+v, %v", st, err)
	}

	// A local edit is current, and its version increments.
	a.write("x.txt", "one")
	a.scan("docs")
	v1 := a.state("x.txt")
	if !v1.Known || !v1.Current || v1.Deleted || v1.Dir || len(v1.Version) == 0 {
		t.Fatalf("after first write: %+v", v1)
	}
	a.write("x.txt", "two, longer")
	a.scan("docs")
	v2 := a.state("x.txt")
	if !v2.Current || v2.Version.Compare(v1.Version) != api.Newer {
		t.Fatalf("after edit: %+v, was %+v", v2, v1)
	}
	short := a.s.id.Short().String()
	var mine bool
	for _, c := range v2.Version {
		mine = mine || c.ID == short
	}
	if !mine {
		t.Errorf("version %v has no counter for %s", v2.Version, short)
	}
	eventually(t, "x.txt on b", fileIs(filepath.Join(b.dir, "x.txt"), "two, longer"))
	eventually(t, "b current", func() bool {
		st := b.state("x.txt")
		return st.Current && st.Version.Compare(v2.Version) == api.Equal
	})

	// A peer's newer version is not current on a node that has not pulled
	// it. b ignores pending.txt, so it learns the version but never pulls.
	b.write(".stignore", "pending.txt\n")
	b.scan("docs")
	a.write("pending.txt", "from a")
	a.scan("docs")
	eventually(t, "b knows pending.txt", func() bool { return b.state("pending.txt").Known })
	if st := b.state("pending.txt"); st.Current {
		t.Errorf("b reports an unpulled file current: %+v", st)
	}
	if st := a.state("pending.txt"); !st.Current {
		t.Errorf("a reports its own file not current: %+v", st)
	}
	// Once b stops ignoring it and pulls, it is current.
	if err := os.Remove(filepath.Join(b.dir, ".stignore")); err != nil {
		t.Fatal(err)
	}
	b.scan("docs")
	eventually(t, "b pulls pending.txt", func() bool { return b.state("pending.txt").Current })

	// A delete shows Deleted, with a version descending from the file's.
	if err := os.Remove(filepath.Join(a.dir, "x.txt")); err != nil {
		t.Fatal(err)
	}
	a.scan("docs")
	del := a.state("x.txt")
	if !del.Known || !del.Deleted || !del.Current || del.Version.Compare(v2.Version) != api.Newer {
		t.Fatalf("after delete: %+v, file was %+v", del, v2)
	}
	eventually(t, "delete on b", func() bool {
		st := b.state("x.txt")
		return st.Deleted && st.Current && st.Version.Compare(del.Version) == api.Equal
	})

	// A file recreated after a delete descends from the delete.
	a.write("x.txt", "three")
	a.scan("docs")
	re := a.state("x.txt")
	if re.Deleted || !re.Current || re.Version.Compare(del.Version) != api.Newer {
		t.Fatalf("after recreate: %+v, delete was %+v", re, del)
	}
}
