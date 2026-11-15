package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/keeper"
	"github.com/progrium/keepsync/internal/kit"
	"github.com/progrium/keepsync/internal/node"
	"github.com/progrium/keepsync/internal/registry"
	"github.com/progrium/keepsync/internal/server"
)

const syncTimeout = 90 * time.Second

// TestCluster runs one server and three nodes through the milestone 1 to 4
// flows. perkeepd runs once per process, so every step shares the cluster
// and the steps run in order; a step that fails stops the rest.
func TestCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("starts MinIO, ZeroFS, perkeepd and three Syncthings")
	}
	c := newCluster(t)
	var a, b, cc *testNode

	step := func(name string, fn func(t *testing.T)) {
		if !t.Run(name, fn) {
			t.FailNow()
		}
	}

	step("join", func(t *testing.T) {
		a, b, cc = c.addNode("a"), c.addNode("b"), c.addNode("c")
		eventually(t, time.Minute, "all nodes registered and online", func() error {
			var nodes []registry.NodeInfo
			if err := c.serverCall(server.MethodNodes, nil, &nodes); err != nil {
				return err
			}
			if len(nodes) != 3 {
				return fmt.Errorf("%d nodes", len(nodes))
			}
			for _, n := range nodes {
				if !n.Registered || !n.Online {
					return fmt.Errorf("%s registered=%v online=%v", n.Name, n.Registered, n.Online)
				}
			}
			return nil
		})
		for _, n := range []*testNode{a, b, cc} {
			eventually(t, time.Minute, n.name+" watching", func() error {
				st, err := n.status()
				if err != nil {
					return err
				}
				if !st.Registered || !st.Watching {
					return fmt.Errorf("registered=%v watching=%v err=%q", st.Registered, st.Watching, st.ServerErr)
				}
				if st.Device != n.dev {
					return fmt.Errorf("device %s, init said %s", st.Device, n.dev)
				}
				return nil
			})
		}
	})

	step("sync", func(t *testing.T) {
		addFolder(t, a, "docs", b, cc)
		a.write("docs", "hello.txt", "hello from a")
		for _, n := range []*testNode{b, cc} {
			eventually(t, syncTimeout, "hello.txt reaches "+n.name, func() error { return n.hasContent("docs", "hello.txt", "hello from a") })
		}
		eventually(t, syncTimeout, "peers connected", func() error {
			for _, n := range []*testNode{a, b, cc} {
				st, err := n.status()
				if err != nil {
					return err
				}
				if len(st.Peers) != 2 {
					return fmt.Errorf("%s has %d peers", n.name, len(st.Peers))
				}
				for _, p := range st.Peers {
					if !p.Connected {
						return fmt.Errorf("%s: peer %s not connected", n.name, p.Name)
					}
				}
			}
			return nil
		})
		b.write("docs", "hello.txt", "edited on b")
		b.write("docs", "sub/dir/deep.txt", "nested")
		for _, n := range []*testNode{a, cc} {
			eventually(t, syncTimeout, "edit reaches "+n.name, func() error {
				if err := n.hasContent("docs", "hello.txt", "edited on b"); err != nil {
					return err
				}
				return n.hasContent("docs", "sub/dir/deep.txt", "nested")
			})
		}
		if err := os.Remove(cc.path("docs", "sub/dir/deep.txt")); err != nil {
			t.Fatal(err)
		}
		for _, n := range []*testNode{a, b} {
			eventually(t, syncTimeout, "delete reaches "+n.name, func() error { return n.absent("docs", "sub/dir/deep.txt") })
		}
	})

	step("join-merge", func(t *testing.T) {
		if err := a.call(node.MethodFolderAdd, node.FolderParams{Name: "shared", Path: a.dir("shared")}, nil); err != nil {
			t.Fatalf("folder add shared: %v", err)
		}
		b.write("shared", "mine.txt", "already on b")
		join := func(n *testNode, merge bool) error {
			return n.call(node.MethodFolderJoin, node.FolderParams{Name: "shared", Path: n.dir("shared"), Merge: merge}, nil)
		}
		if err := join(b, false); err == nil || !strings.Contains(err.Error(), "not empty") {
			t.Fatalf("join from a non-empty directory without merge: err=%v, want a refusal", err)
		}
		if f, err := serverFolder(c, "shared"); err != nil {
			t.Fatal(err)
		} else if slices.Contains(f.Members, b.key) {
			t.Fatal("a refused join still made b a member")
		}
		if err := join(b, true); err != nil {
			t.Fatalf("join with merge: %v", err)
		}
		// Rejoining at the path it is attached at needs no merge.
		if err := join(b, false); err != nil {
			t.Fatalf("rejoin at the same path: %v", err)
		}
		if err := join(cc, false); err != nil {
			t.Fatalf("join from an empty directory: %v", err)
		}
		for _, n := range []*testNode{a, cc} {
			eventually(t, syncTimeout, "merged file reaches "+n.name, func() error { return n.hasContent("shared", "mine.txt", "already on b") })
		}
	})

	step("keep-grace", func(t *testing.T) {
		const grace = 6 * time.Second
		keep(t, a, "docs", node.KeepParams{Grace: grace, MaxDefer: 30 * time.Second})
		for _, n := range []*testNode{a, b, cc} {
			eventually(t, syncTimeout, ".keepsync reaches "+n.name, func() error {
				m, err := keeper.ReadMapping(n.dir("docs"))
				if err != nil {
					return err
				}
				if m.Permanode == "" {
					return errors.New("no permanode in .keepsync")
				}
				return nil
			})
		}
		keepers := folderKeepers(t, c, "docs")
		if len(keepers) != 2 {
			t.Fatalf("docs has keepers %v, want two", keepers)
		}
		// hello.txt is older than grace by now: it is kept.
		eventually(t, time.Minute, "hello.txt kept", func() error { return wantState(a, "docs", "hello.txt", keeper.StateKept) })

		wrote := time.Now()
		a.write("docs", "notes.txt", "fresh")
		var keptAt time.Time
		eventually(t, time.Minute, "notes.txt kept", func() error {
			ks, err := a.keepStatus("docs")
			if err != nil {
				return err
			}
			f, ok := fileState(ks, "notes.txt")
			if !ok {
				return errors.New("notes.txt not in keep status")
			}
			if f.State == keeper.StateKept {
				keptAt = time.Now()
				return nil
			}
			return fmt.Errorf("notes.txt is %s", f.State)
		})
		if d := keptAt.Sub(wrote); d < grace {
			t.Errorf("notes.txt kept %v after it was written, inside the %v grace", d, grace)
		}
		// Both keepers see the kept tree; a node that is not a keeper reports
		// the same state from the server's tree.
		for _, n := range []*testNode{b, cc} {
			eventually(t, time.Minute, "notes.txt kept as seen by "+n.name, func() error { return wantState(n, "docs", "notes.txt", keeper.StateKept) })
		}
	})

	step("expiry-gated-on-durability", func(t *testing.T) {
		addFolder(t, a, "photos", b, cc)
		keep(t, a, "photos", node.KeepParams{Grace: 2 * time.Second, MaxDefer: 4 * time.Second, Expire: 20 * time.Second})
		old := time.Now().Add(-time.Hour)
		a.write("photos", "old.jpg", "old photo")
		if err := os.Chtimes(a.path("photos", "old.jpg"), old, old); err != nil {
			t.Fatal(err)
		}
		a.write("photos", "new.jpg", "new photo")
		for _, n := range []*testNode{b, cc} {
			eventually(t, syncTimeout, "photos reach "+n.name, func() error {
				if err := n.hasContent("photos", "old.jpg", "old photo"); err != nil {
					return err
				}
				return n.hasContent("photos", "new.jpg", "new photo")
			})
		}
		eventually(t, time.Minute, "old.jpg kept", func() error { return wantState(a, "photos", "old.jpg", keeper.StateKept) })
		// Kept and long past expire, but not durable: the server has not
		// flushed since, so nothing may expire.
		holds(t, 8*time.Second, "old.jpg stays until durable", func() error {
			for _, n := range []*testNode{a, b, cc} {
				if err := n.hasContent("photos", "old.jpg", "old photo"); err != nil {
					return err
				}
			}
			return nil
		})
		c.flush("")
		for _, n := range []*testNode{a, b, cc} {
			eventually(t, syncTimeout, "old.jpg expires on "+n.name, func() error { return n.absent("photos", "old.jpg") })
		}
		for _, n := range []*testNode{a, b, cc} {
			if err := n.hasContent("photos", "new.jpg", "new photo"); err != nil {
				t.Errorf("new.jpg is inside expire and must stay: %v", err)
			}
		}
	})

	t.Run("restart-delete", func(t *testing.T) {
		a.write("docs", "r1.txt", "one")
		a.write("docs", "r2.txt", "two")
		for _, n := range []*testNode{b, cc} {
			eventually(t, syncTimeout, "r1, r2 reach "+n.name, func() error {
				if err := n.hasContent("docs", "r1.txt", "one"); err != nil {
					return err
				}
				return n.hasContent("docs", "r2.txt", "two")
			})
		}
		// Deleted while the node is down.
		cc.kill()
		if err := os.Remove(cc.path("docs", "r1.txt")); err != nil {
			t.Fatal(err)
		}
		cc.start()
		for _, n := range []*testNode{a, b} {
			eventually(t, syncTimeout, "delete made while down reaches "+n.name, func() error { return n.absent("docs", "r1.txt") })
		}
		// Deleted after the restart.
		if err := os.Remove(cc.path("docs", "r2.txt")); err != nil {
			t.Fatal(err)
		}
		for _, n := range []*testNode{a, b} {
			eventually(t, syncTimeout, "delete after restart reaches "+n.name, func() error { return n.absent("docs", "r2.txt") })
		}
	})

	t.Run("folder-leave", func(t *testing.T) {
		if err := cc.call(node.MethodFolderRm, node.FolderParams{Name: "docs"}, nil); err != nil {
			t.Fatal(err)
		}
		eventually(t, time.Minute, "c leaves docs on the server", func() error {
			f, err := serverFolder(c, "docs")
			if err != nil {
				return err
			}
			if slices.Contains(f.Members, cc.key) {
				return errors.New("c is still a member of docs")
			}
			if f.Keep != nil && slices.Contains(f.Keep.Keepers, cc.key) {
				return errors.New("c is still a keeper of docs")
			}
			return nil
		})
		// Files stay on disk, and new files stop arriving.
		if err := cc.hasContent("docs", "hello.txt", "edited on b"); err != nil {
			t.Errorf("leaving must leave files in place: %v", err)
		}
		a.write("docs", "after-leave.txt", "not for c")
		eventually(t, syncTimeout, "after-leave.txt reaches b", func() error { return b.hasContent("docs", "after-leave.txt", "not for c") })
		holds(t, 5*time.Second, "after-leave.txt stays off c", func() error { return cc.absent("docs", "after-leave.txt") })
	})

	step("unkeep", func(t *testing.T) {
		var res node.UnkeepResult
		if err := b.call(node.MethodUnkeep, node.FolderParams{Name: "photos"}, &res); err != nil {
			t.Fatalf("unkeep: %v", err)
		}
		if res.Archive == "" {
			t.Fatal("unkeep reported no archive")
		}
		for _, n := range []*testNode{a, b, cc} {
			eventually(t, time.Minute, "keeper stops on "+n.name, func() error {
				if _, err := n.keepStatus("photos"); err == nil {
					return errors.New("still keeping photos")
				}
				return nil
			})
		}
		// A file past grace and expire, made durable: nothing keeps it and
		// nothing expires it.
		old := time.Now().Add(-time.Hour)
		a.write("photos", "old2.jpg", "unkept photo")
		a.setMtime("photos", "old2.jpg", old)
		for _, n := range []*testNode{b, cc} {
			eventually(t, syncTimeout, "old2.jpg reaches "+n.name, func() error { return n.hasContent("photos", "old2.jpg", "unkept photo") })
		}
		c.flush("")
		holds(t, 10*time.Second, "old2.jpg stays and is not kept", func() error {
			for _, n := range []*testNode{a, b, cc} {
				if err := n.hasContent("photos", "old2.jpg", "unkept photo"); err != nil {
					return err
				}
			}
			return nil
		})
		if _, err := cc.restore(node.RestoreParams{Folder: "photos", Path: "old2.jpg", To: t.TempDir()}); err == nil {
			t.Error("old2.jpg was kept after unkeep")
		}
		// The archive still restores, including the file that expired.
		to := t.TempDir()
		rr, err := cc.restore(node.RestoreParams{Folder: "photos", Path: "old.jpg", To: to})
		if err != nil {
			t.Fatalf("restore after unkeep: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(to, "old.jpg")); string(got) != "old photo" {
			t.Fatalf("restored old.jpg holds %q (%+v)", got, rr)
		}
		// Keeping again continues the same tree.
		var kr node.KeepResult
		if err := a.call(node.MethodKeep, node.KeepParams{Folder: "photos", Grace: 2 * time.Second, MaxDefer: 4 * time.Second, Expire: 20 * time.Second}, &kr); err != nil {
			t.Fatalf("keep again: %v", err)
		}
		if kr.Permanode != res.Archive {
			t.Fatalf("keep again made permanode %s, want the archive %s", kr.Permanode, res.Archive)
		}
		eventually(t, time.Minute, "old2.jpg kept after keep again", func() error { return wantState(a, "photos", "old2.jpg", keeper.StateKept) })
		to = t.TempDir()
		if _, err := cc.restore(node.RestoreParams{Folder: "photos", Path: "old.jpg", To: to}); err != nil {
			t.Fatalf("restore old.jpg after keep again: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(to, "old.jpg")); string(got) != "old photo" {
			t.Fatalf("old.jpg after keep again holds %q", got)
		}
	})

	nodes := func() []*testNode { return []*testNode{a, b, cc} }
	// conflict makes a real Syncthing conflict on rel in notes: loser edits
	// while it is down, winner edits later, then loser comes back.
	conflict := func(t *testing.T, winner, loser *testNode, rel string) {
		loser.kill()
		winner.write("notes", rel, "from "+winner.name+"\n")
		loser.write("notes", rel, "from "+loser.name+"\n")
		loser.setMtime("notes", rel, time.Now().Add(-time.Minute))
		loser.start()
	}

	step("conflict-hook", func(t *testing.T) {
		addFolder(t, a, "notes", b, cc)
		keep(t, a, "notes", node.KeepParams{Grace: time.Hour, MaxDefer: 2 * time.Hour})
		a.write("notes", "f.txt", "base\n")
		a.setMtime("notes", "f.txt", time.Now().Add(-3*time.Hour))
		a.write("notes", "fail-g.txt", "gbase\n")
		a.setMtime("notes", "fail-g.txt", time.Now().Add(-3*time.Hour))
		for _, n := range []*testNode{b, cc} {
			eventually(t, syncTimeout, "notes reach "+n.name, func() error { return n.hasContent("notes", "f.txt", "base\n") })
		}
		eventually(t, time.Minute, "base kept", func() error {
			if err := wantState(a, "notes", "f.txt", keeper.StateKept); err != nil {
				return err
			}
			return wantState(a, "notes", "fail-g.txt", keeper.StateKept)
		})
		keepers := folderKeepers(t, c, "notes")
		var hookN, loser *testNode
		for _, n := range nodes() {
			if n.key == keepers[0] {
				hookN = n
			} else if loser == nil {
				loser = n
			}
		}
		t.Logf("hook node %s, conflicting edit from %s", hookN.name, loser.name)
		conflict(t, hookN, loser, "f.txt")

		for _, n := range nodes() {
			eventually(t, 2*syncTimeout, "merge reaches "+n.name, func() error {
				if err := n.hasContent("notes", "f.txt", "from "+hookN.name+"\nfrom "+loser.name+"\n"); err != nil {
					return err
				}
				if cps := copies(n, "notes", "f.sync-conflict-*"); len(cps) > 0 {
					return fmt.Errorf("conflict copies on %s: %v", n.name, cps)
				}
				return nil
			})
		}
		holds(t, 3*time.Second, "hook ran once", func() error { return nil })
		runs := c.hookRuns("f.txt")
		if len(runs) != 1 {
			t.Fatalf("hook runs for f.txt: %q, want exactly one", runs)
		}
		f := strings.Fields(runs[0])
		if f[0] != hookN.name || f[4] != "base=yes" {
			t.Errorf("hook run %q: want node %s (Keepers[0]) and a base", runs[0], hookN.name)
		}
		if f[3] != loser.name {
			t.Errorf("KEEPSYNC_CONFLICT_NODE is %q, want %q, whose change the copy holds", f[3], loser.name)
		}
		if b, err := os.ReadFile(filepath.Join(c.root, "hookbase-"+hookN.name+"-f.txt")); err != nil || string(b) != "base\n" {
			t.Errorf("KEEPSYNC_BASE held %q (%v), want the last kept version", b, err)
		}
		// Both versions were kept before the merge; the merge itself is
		// inside grace. The hook deleted the copy, so once the keeper marks
		// it deleted in sync, restore skips it unless asked.
		eventually(t, time.Minute, "the copy marked deleted in sync", func() error {
			res, err := hookN.restore(node.RestoreParams{Folder: "notes", Path: "f.txt", To: t.TempDir()})
			if err != nil {
				return err
			}
			all, err := hookN.restore(node.RestoreParams{Folder: "notes", To: t.TempDir()})
			if err != nil {
				return err
			}
			if len(all.Deleted) != 1 || !strings.HasPrefix(all.Deleted[0], "f.sync-conflict-") {
				return fmt.Errorf("skipped as deleted: %v (f.txt alone: %v)", all.Deleted, res.Deleted)
			}
			return nil
		})
		to := t.TempDir()
		if _, err := hookN.restore(node.RestoreParams{Folder: "notes", To: to, Deleted: true}); err != nil {
			t.Fatalf("restore notes: %v", err)
		}
		if got, _ := os.ReadFile(filepath.Join(to, "f.txt")); string(got) != "from "+hookN.name+"\n" {
			t.Errorf("kept f.txt holds %q, want the winning version", got)
		}
		kc, _ := filepath.Glob(filepath.Join(to, "f.sync-conflict-*"))
		if len(kc) != 1 {
			t.Errorf("kept conflict copies: %v, want one", kc)
		} else if got, _ := os.ReadFile(kc[0]); string(got) != "from "+loser.name+"\n" {
			t.Errorf("kept conflict copy holds %q, want the losing version", got)
		}
		for _, n := range nodes() {
			eventually(t, time.Minute, "no conflicts on "+n.name, func() error {
				cs, err := n.conflicts()
				if err != nil {
					return err
				}
				var left []string
				for _, ci := range cs {
					if ci.Folder == "notes" && strings.HasPrefix(filepath.Base(ci.Original), "f.") {
						left = append(left, ci.Path+" "+ci.State)
					}
				}
				if len(left) > 0 {
					return fmt.Errorf("%v", left)
				}
				return nil
			})
		}

		// A failing hook leaves the copy for a human.
		conflict(t, hookN, loser, "fail-g.txt")
		for _, n := range nodes() {
			eventually(t, 2*syncTimeout, "fail-g conflict copy reaches "+n.name, func() error {
				if cps := copies(n, "notes", "fail-g.sync-conflict-*"); len(cps) != 1 {
					return fmt.Errorf("copies %v", cps)
				}
				return nil
			})
		}
		eventually(t, time.Minute, "failed hook shows in conflicts", func() error {
			cs, err := hookN.conflicts()
			if err != nil {
				return err
			}
			for _, ci := range cs {
				if strings.HasPrefix(filepath.Base(ci.Path), "fail-g.sync-conflict-") {
					if ci.State != node.HookFailed {
						return fmt.Errorf("state %s", ci.State)
					}
					return nil
				}
			}
			return errors.New("not listed")
		})
		holds(t, 8*time.Second, "copy stays after the failed hook", func() error {
			for _, n := range nodes() {
				if cps := copies(n, "notes", "fail-g.sync-conflict-*"); len(cps) != 1 {
					return fmt.Errorf("%s: copies %v", n.name, cps)
				}
			}
			return nil
		})
		if runs := c.hookRuns("fail-g.txt"); len(runs) != 1 || !strings.HasPrefix(runs[0], hookN.name+" ") {
			t.Errorf("hook runs for fail-g.txt: %q, want one on %s", runs, hookN.name)
		}
		for _, n := range nodes() {
			if n == hookN {
				continue
			}
			cs, err := n.conflicts()
			if err != nil {
				t.Fatal(err)
			}
			for _, ci := range cs {
				if strings.HasPrefix(filepath.Base(ci.Path), "fail-g.sync-conflict-") && ci.State != node.HookNotMine {
					t.Errorf("%s shows %s for the fail-g copy, want %s", n.name, ci.State, node.HookNotMine)
				}
			}
		}
	})

	step("recover-drill", func(t *testing.T) {
		work := filepath.Join(c.root, "drill")
		if err := os.MkdirAll(work, 0o700); err != nil {
			t.Fatal(err)
		}
		cli := buildCLI(t, work)
		kitPath := filepath.Join(work, kit.FileName)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		pass, err := kit.Write(ctx, c.srvCfg, kitPath, c.srv.Addr())
		if err != nil {
			t.Fatal(err)
		}
		// Baseline after a drain and flush, then copy the bucket.
		base := filepath.Join(work, "baseline.json")
		c.flush(base)
		c.minio.MakeBucket(t, "drill")
		n := c.minio.CopyBucket(t, c.bucket, "drill", c.prefix+"/")
		t.Logf("copied %d objects to the drill bucket", n)

		cmd := exec.CommandContext(ctx, cli, "recover", kitPath, "--drill",
			"--bucket", "s3://drill/"+c.prefix, "--baseline", base)
		cmd.Env = append(os.Environ(), "KEEPSYNC_KIT_PASSPHRASE="+pass, "KEEPSYNC_DIR="+filepath.Join(work, "home"))
		out, err := cmd.CombinedOutput()
		t.Logf("keepsync recover --drill:\n%s", trimLines(string(out), 40))
		if err != nil {
			t.Fatalf("drill failed: %v", err)
		}
		if !strings.Contains(string(out), "PASS") {
			t.Fatal("drill report has no PASS")
		}
		// The drill must refuse the live bucket.
		cmd = exec.CommandContext(ctx, cli, "recover", kitPath, "--drill", "--bucket", c.storage().URL)
		cmd.Env = append(os.Environ(), "KEEPSYNC_KIT_PASSPHRASE="+pass, "KEEPSYNC_DIR="+filepath.Join(work, "home"))
		if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "live bucket") {
			t.Errorf("drill against the live bucket: err=%v\n%s", err, out)
		}
	})

	t.Run("keeper-reassignment", func(t *testing.T) {
		before := folderKeepers(t, c, "photos")
		if len(before) != 2 {
			t.Fatalf("photos has keepers %v, want two", before)
		}
		var dead *testNode
		for _, n := range []*testNode{a, b, cc} {
			if n.key == before[0] {
				dead = n
			}
		}
		var spare *testNode
		for _, n := range []*testNode{a, b, cc} {
			if !slices.Contains(before, n.key) {
				spare = n
			}
		}
		if dead == nil || spare == nil {
			t.Fatalf("keepers %v are not two of the three nodes", before)
		}
		t.Logf("killing keeper %s; expecting %s to take over", dead.name, spare.name)
		dead.kill()
		// registry.DefaultKeeperTimeout, plus the time to notice the drop.
		eventually(t, registry.DefaultKeeperTimeout+90*time.Second, "keeper reassigned", func() error {
			k := folderKeepers(t, c, "photos")
			if slices.Contains(k, dead.key) || !slices.Contains(k, spare.key) || len(k) != 2 {
				return fmt.Errorf("keepers %v", k)
			}
			return nil
		})
		// The new keeper keeps.
		spare.write("photos", "after-reassign.jpg", "x")
		eventually(t, time.Minute, "new keeper keeps", func() error {
			return wantState(spare, "photos", "after-reassign.jpg", keeper.StateKept)
		})
	})
}

// addFolder creates folder on owner and joins it on the others.
func addFolder(t *testing.T, owner *testNode, folder string, others ...*testNode) {
	t.Helper()
	if err := owner.call(node.MethodFolderAdd, node.FolderParams{Name: folder, Path: owner.dir(folder)}, nil); err != nil {
		t.Fatalf("folder add %s on %s: %v", folder, owner.name, err)
	}
	for _, n := range others {
		if err := n.call(node.MethodFolderJoin, node.FolderParams{Name: folder, Path: n.dir(folder)}, nil); err != nil {
			t.Fatalf("folder join %s on %s: %v", folder, n.name, err)
		}
	}
	eventually(t, time.Minute, folder+" has every member", func() error {
		f, err := serverFolder(owner.c, folder)
		if err != nil {
			return err
		}
		if len(f.Members) != 1+len(others) {
			return fmt.Errorf("members %v", f.Members)
		}
		return nil
	})
}

func keep(t *testing.T, n *testNode, folder string, p node.KeepParams) {
	t.Helper()
	p.Folder = folder
	var res node.KeepResult
	if err := n.call(node.MethodKeep, p, &res); err != nil {
		t.Fatalf("keep %s: %v", folder, err)
	}
	if res.Permanode == "" {
		t.Fatalf("keep %s: no permanode", folder)
	}
	eventually(t, time.Minute, folder+" keeper running on "+n.name, func() error {
		_, err := n.keepStatus(folder)
		return err
	})
}

func serverFolder(c *cluster, name string) (api.Folder, error) {
	var st server.Status
	if err := c.serverCall(server.MethodStatus, nil, &st); err != nil {
		return api.Folder{}, err
	}
	for _, f := range st.Folders {
		if f.Name == name {
			return f, nil
		}
	}
	return api.Folder{}, fmt.Errorf("no folder %s on the server", name)
}

func folderKeepers(t *testing.T, c *cluster, name string) []api.NodeKey {
	t.Helper()
	f, err := serverFolder(c, name)
	if err != nil {
		t.Fatal(err)
	}
	if f.Keep == nil {
		t.Fatalf("%s is not kept", name)
	}
	return f.Keep.Keepers
}

func fileState(ks node.KeepStatus, rel string) (keeper.FileStatus, bool) {
	for _, f := range ks.Status.Files {
		if filepath.ToSlash(f.Path) == rel {
			return f, true
		}
	}
	return keeper.FileStatus{}, false
}

func wantState(n *testNode, folder, rel string, want keeper.State) error {
	ks, err := n.keepStatus(folder)
	if err != nil {
		return err
	}
	f, ok := fileState(ks, rel)
	if !ok {
		return fmt.Errorf("%s not in %s's keep status", rel, n.name)
	}
	if f.State != want {
		return fmt.Errorf("%s on %s is %s, want %s", rel, n.name, f.State, want)
	}
	return nil
}

// copies lists conflict copies matching pattern in folder's top level.
func copies(n *testNode, folder, pattern string) []string {
	m, _ := filepath.Glob(filepath.Join(n.dir(folder), pattern))
	return m
}
