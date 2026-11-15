package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/progrium/keepsync/internal/registry"
)

// Pending leaves. `folder rm` stops syncing a folder even when the server
// cannot be told, but then the node stays a member, and maybe a keeper, of
// a folder it no longer has (docs/design.md, Edge cases). So the leave is
// saved and retried whenever the server answers, until it goes through.

const pendingLeaveFile = "pending-leave.json"

type pendingLeaves struct {
	mu sync.Mutex // serializes reads and writes of the file
}

func (n *Node) pendingLeavePath() string { return filepath.Join(n.cfg.Dir, pendingLeaveFile) }

// loadPendingLeaves returns the folders this node still has to leave. The
// caller holds n.leaves.mu.
func (n *Node) loadPendingLeaves() []string {
	var names []string
	b, err := os.ReadFile(n.pendingLeavePath())
	if err != nil || json.Unmarshal(b, &names) != nil {
		return nil
	}
	return names
}

// savePendingLeaves writes the list, or removes the file when it is
// empty. The caller holds n.leaves.mu.
func (n *Node) savePendingLeaves(names []string) {
	p := n.pendingLeavePath()
	if len(names) == 0 {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			n.logf("pending leaves: %v", err)
		}
		return
	}
	b, _ := json.Marshal(names)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		n.logf("pending leaves: %v", err)
		return
	}
	if err := os.Rename(tmp, p); err != nil {
		n.logf("pending leaves: %v", err)
	}
}

// addPendingLeave records that the server still has to be told this node
// left folder.
func (n *Node) addPendingLeave(folder string) {
	n.leaves.mu.Lock()
	defer n.leaves.mu.Unlock()
	names := n.loadPendingLeaves()
	if !slices.Contains(names, folder) {
		n.savePendingLeaves(append(names, folder))
	}
}

// dropPendingLeave forgets a pending leave, because the folder was
// attached here again.
func (n *Node) dropPendingLeave(folder string) {
	n.leaves.mu.Lock()
	defer n.leaves.mu.Unlock()
	names := n.loadPendingLeaves()
	if i := slices.Index(names, folder); i >= 0 {
		n.savePendingLeaves(slices.Delete(names, i, i+1))
	}
}

// retryLeaves tells the server about every pending leave. A folder that
// was attached here again, or that the server no longer has, is dropped;
// any other failure is kept for the next try.
func (n *Node) retryLeaves(ctx context.Context) {
	n.leaves.mu.Lock()
	defer n.leaves.mu.Unlock()
	names := n.loadPendingLeaves()
	if len(names) == 0 {
		return
	}
	var keep []string
	for _, name := range names {
		if _, err := n.localFolder(name); err == nil {
			continue // joined again since; staying a member is right
		}
		err := n.reg.LeaveFolder(ctx, name)
		switch {
		case err == nil:
			n.logf("left folder %s on the server (it was removed here while the server was unreachable)", name)
		case errors.Is(err, registry.ErrNotFound):
		default:
			keep = append(keep, name)
		}
	}
	if len(keep) != len(names) {
		n.savePendingLeaves(keep)
	}
}
