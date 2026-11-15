package registry

import (
	"context"
	"sync"

	"github.com/progrium/keepsync/internal/api"
)

// NodeRecord is one approved node as the registry stores it.
type NodeRecord struct {
	api.Node
	// Registered is set once the node has called Register, so Addr and
	// Device are known. Only registered nodes appear in Membership.Nodes.
	Registered bool `json:"registered"`
	// NamedByAdmin is set when the name came from `server add --name`, in
	// which case Register does not overwrite it.
	NamedByAdmin bool `json:"namedByAdmin,omitempty"`
}

// State is everything the registry persists: the allow list (approved
// nodes), the folders and their keep policies, and the membership version.
type State struct {
	Version uint64       `json:"version"`
	Nodes   []NodeRecord `json:"nodes"`
	Folders []api.Folder `json:"folders"`
}

// Store persists registry State. The design keeps it in Perkeep so it comes
// back from the bucket on recovery; MemStore is the in-memory stand-in.
type Store interface {
	// Load returns the saved state, or the zero State if none was saved.
	Load(ctx context.Context) (State, error)
	// Save replaces the saved state. The registry calls it before
	// publishing any change, so a published version is always durable.
	Save(ctx context.Context, s State) error
}

// MemStore is an in-memory Store.
type MemStore struct {
	mu sync.Mutex
	s  State
	// Saves counts calls to Save, for tests.
	Saves int
}

var _ Store = (*MemStore)(nil)

func (m *MemStore) Load(ctx context.Context) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.s.clone(), nil
}

func (m *MemStore) Save(ctx context.Context, s State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.s = s.clone()
	m.Saves++
	return nil
}

func (s State) clone() State {
	out := State{Version: s.Version}
	out.Nodes = append([]NodeRecord(nil), s.Nodes...)
	out.Folders = make([]api.Folder, 0, len(s.Folders))
	for _, f := range s.Folders {
		out.Folders = append(out.Folders, cloneFolder(f))
	}
	return out
}

func cloneFolder(f api.Folder) api.Folder {
	f.Members = append([]api.NodeKey(nil), f.Members...)
	f.ReceiveOnly = append([]api.NodeKey(nil), f.ReceiveOnly...)
	if f.Keep != nil {
		k := *f.Keep
		k.Keepers = append([]api.NodeKey(nil), k.Keepers...)
		f.Keep = &k
	}
	return f
}

func (s *State) node(key api.NodeKey) *NodeRecord {
	for i := range s.Nodes {
		if s.Nodes[i].Key == key {
			return &s.Nodes[i]
		}
	}
	return nil
}

func (s *State) folder(name string) *api.Folder {
	for i := range s.Folders {
		if s.Folders[i].Name == name {
			return &s.Folders[i]
		}
	}
	return nil
}

func (s *State) keptFolder(pn api.BlobRef) *api.Folder {
	for i := range s.Folders {
		if k := s.Folders[i].Keep; k != nil && k.Permanode == pn {
			return &s.Folders[i]
		}
	}
	return nil
}

// membership is the view published to nodes: registered nodes only.
func (s State) membership() api.Membership {
	c := s.clone()
	m := api.Membership{Version: c.Version, Folders: c.Folders}
	for _, n := range c.Nodes {
		if n.Registered {
			m.Nodes = append(m.Nodes, n.Node)
		}
	}
	return m
}

func contains(keys []api.NodeKey, k api.NodeKey) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

func without(keys []api.NodeKey, k api.NodeKey) []api.NodeKey {
	out := keys[:0:0]
	for _, x := range keys {
		if x != k {
			out = append(out, x)
		}
	}
	return out
}
