package node

import (
	"testing"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/api/fake"
)

// TestWatchRegistersAgainWhenForgotten: a node removed and added again
// while running is approved but unknown to the server, and its watch
// reconnects on its own, so it must notice and register again (issue #9).
func TestWatchRegistersAgainWhenForgotten(t *testing.T) {
	tn := newTestNode(t, func(pk *fake.Perkeep) api.BlobPutter { return pk })
	done := make(chan struct{})
	go func() {
		defer close(done)
		tn.watch(tn.ctx)
	}()
	defer func() { tn.stop(); <-done }()

	waitFor(t, "watching", func() bool {
		tn.mu.Lock()
		defer tn.mu.Unlock()
		return tn.watching
	})
	tn.freg.Forget(tn.tr.Key())
	waitFor(t, "registered again", func() bool {
		m := tn.freg.Membership()
		for _, nd := range m.Nodes {
			if nd.Key == tn.tr.Key() {
				return nd.Device == "DEVICE-A"
			}
		}
		return false
	})
}
