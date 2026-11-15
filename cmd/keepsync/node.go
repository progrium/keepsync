package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/gateway"
	"github.com/progrium/keepsync/internal/node"
)

func isNotRunning(err error) bool { return errors.Is(err, ctl.ErrNotRunning) }

func (e *env) nodeInit(args []string) error {
	fs := newFlags(e, "node init")
	var o node.InitOptions
	fs.StringVar(&o.Server, "server", "", "server tailcat address (tc…) or a DNS name with a TXT record")
	fs.StringVar(&o.Name, "name", "", "node name (default: hostname)")
	netFlags(fs, &o.Network)
	if _, err := parse(fs, args, 0, 0, "--server <tc… or name> [--name <name>]"); err != nil {
		return err
	}
	o.Dir = e.nodeDir()
	ctx, cancel := signalContext()
	defer cancel()
	res, err := node.Init(ctx, o)
	if err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "node %q initialized in %s\n", res.Config.Name, o.Dir)
	fmt.Fprintf(e.stdout, "device:  %s\n", res.Device)
	fmt.Fprintf(e.stdout, "key:     %s\n\n", res.Key)
	fmt.Fprintf(e.stdout, "Next: on the server, approve this node:\n  keepsync server add %s --name %s\nthen here: keepsync node run\n", res.Key, res.Config.Name)
	return nil
}

func (e *env) nodeRun(args []string) error {
	fs := newFlags(e, "node run")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	cfg, err := config.LoadNode(e.nodeDir())
	if err != nil {
		return err
	}
	ctx, cancel := signalContext()
	defer cancel()
	l := log.New(e.stderr, "", log.LstdFlags)
	return node.Run(ctx, cfg, node.Options{Logf: l.Printf})
}

func (e *env) nodeCall(method string, params, out any) error {
	ctx, cancel := callContext()
	defer cancel()
	sock := (&config.Node{Dir: e.nodeDir()}).SocketPath()
	err := ctl.Call(ctx, sock, method, params, out)
	if err != nil && isNotRunning(err) {
		return fmt.Errorf("the node is not running (start it with `keepsync node run`): %w", err)
	}
	return err
}

func (e *env) status(args []string) error {
	fs := newFlags(e, "status")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	var st node.Status
	if err := e.nodeCall(node.MethodStatus, nil, &st); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "node %s, device %s\n", st.Name, short(string(st.Device)))
	switch {
	case st.Watching:
		fmt.Fprintf(e.stdout, "server: connected (registry version %d)\n", st.Version)
		printServerSummary(e, st.ServerSummary, st.SummaryErr)
	case st.ServerErr != "":
		since := ""
		if !st.ServerSeen.IsZero() {
			since = fmt.Sprintf(", last seen %s ago", time.Since(st.ServerSeen).Round(time.Second))
		}
		fmt.Fprintf(e.stdout, "server: UNREACHABLE%s: %s\n", since, st.ServerErr)
		if !st.Registered {
			fmt.Fprintf(e.stdout, "  if this node is new, approve it on the server: keepsync server add %s\n", st.Key)
		}
	default:
		fmt.Fprintln(e.stdout, "server: connecting")
	}
	fmt.Fprintln(e.stdout)
	if len(st.Peers) == 0 {
		fmt.Fprintln(e.stdout, "peers: none")
	} else {
		tw := tabwriter.NewWriter(e.stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "PEER\tDEVICE\tSTATE")
		for _, p := range st.Peers {
			state := "disconnected"
			if p.Connected {
				state = "connected"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", p.Name, short(string(p.Device)), state)
		}
		tw.Flush()
	}
	fmt.Fprintln(e.stdout)
	printFolders(e, st.Folders)
	return nil
}

func short(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

func printFolders(e *env, folders []node.FolderInfo) {
	if len(folders) == 0 {
		fmt.Fprintln(e.stdout, "folders: none (keepsync folder add <name> <path>)")
		return
	}
	sort.Slice(folders, func(i, j int) bool { return folders[i].Name < folders[j].Name })
	tw := tabwriter.NewWriter(e.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "FOLDER\tPATH\tMEMBERS\tKEEP")
	for _, f := range folders {
		path := f.Path
		if !f.Attached {
			path = "(not attached: folder join)"
		}
		keep := "-"
		if f.Keep != nil {
			keep = fmt.Sprintf("grace %s, max-defer %s, expire %s", fmtDur(f.Keep.Grace), fmtDur(f.Keep.MaxDefer), fmtDur(f.Keep.Expire))
			if f.Keeper {
				keep += " (keeper)"
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", f.Name, path, strings.Join(f.Members, ","), keep)
	}
	tw.Flush()
}

func (e *env) folderAttach(args []string, name string, add bool) error {
	fs := newFlags(e, name)
	var merge *bool
	if !add {
		merge = fs.Bool("merge", false, "join from a directory that already has files, syncing them both ways with the folder")
	}
	usage := "<name> <path>"
	if !add {
		usage += " [--merge]"
	}
	pos, err := parse(fs, args, 2, 2, usage)
	if err != nil {
		return err
	}
	method := node.MethodFolderJoin
	if add {
		method = node.MethodFolderAdd
	}
	var fi node.FolderInfo
	if err := e.nodeCall(method, node.FolderParams{Name: pos[0], Path: pos[1], Merge: merge != nil && *merge}, &fi); err != nil {
		return err
	}
	verb := "joined"
	if add {
		verb = "added"
	}
	fmt.Fprintf(e.stdout, "%s folder %s at %s\n", verb, fi.Name, fi.Path)
	if add {
		fmt.Fprintf(e.stdout, "on other nodes: keepsync folder join %s <path>\n", fi.Name)
	}
	return nil
}

func (e *env) folderLs(args []string) error {
	fs := newFlags(e, "folder ls")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	var folders []node.FolderInfo
	if err := e.nodeCall(node.MethodFolderLs, nil, &folders); err != nil {
		return err
	}
	printFolders(e, folders)
	return nil
}

func (e *env) folderRm(args []string) error {
	fs := newFlags(e, "folder rm")
	pos, err := parse(fs, args, 1, 1, "<name>")
	if err != nil {
		return err
	}
	var res node.RmResult
	if err := e.nodeCall(node.MethodFolderRm, node.FolderParams{Name: pos[0]}, &res); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "stopped syncing %s here; its files stay on disk\n", pos[0])
	if !res.Left {
		fmt.Fprintf(e.stdout, "warning: this node is still a member of %s on the server: %s\n", pos[0], res.Warning)
	}
	return nil
}

func (e *env) keep(args []string) error {
	fs := newFlags(e, "keep")
	var p node.KeepParams
	fs.Var(durationFlag{&p.Grace}, "grace", "unmodified this long before a file is kept (default 48h)")
	fs.Var(durationFlag{&p.MaxDefer}, "max-defer", "keep files that never settle after this long (default 7d)")
	fs.Var(durationFlag{&p.Expire}, "expire", "remove from sync once unmodified this long and durable (default: never)")
	pos, err := parse(fs, args, 1, 1, "<folder> [--grace 48h] [--max-defer 7d] [--expire 30d]")
	if err != nil {
		return err
	}
	p.Folder = pos[0]
	var res node.KeepResult
	if err := e.nodeCall(node.MethodKeep, p, &res); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "keeping %s in permanode %s\n", p.Folder, res.Permanode)
	exp := "never"
	if res.Policy.Expire > 0 {
		exp = fmtDur(res.Policy.Expire)
	}
	fmt.Fprintf(e.stdout, "grace %s, max-defer %s, expire %s\n", fmtDur(res.Policy.Grace), fmtDur(res.Policy.MaxDefer), exp)
	return nil
}

func (e *env) keepStatus(args []string) error {
	fs := newFlags(e, "keep status")
	verbose := fs.Bool("v", false, "list every file")
	pos, err := parse(fs, args, 1, 1, "<folder> [-v]")
	if err != nil {
		return err
	}
	var ks node.KeepStatus
	if err := e.nodeCall(node.MethodKeepStatus, node.FolderParams{Name: pos[0]}, &ks); err != nil {
		return err
	}
	role := "not a keeper (another node keeps it)"
	if ks.Keeper {
		role = "keeper"
	}
	fmt.Fprintf(e.stdout, "%s: %s\n", ks.Folder, role)
	if !ks.LastPass.IsZero() {
		fmt.Fprintf(e.stdout, "last pass %s ago", time.Since(ks.LastPass).Round(time.Second))
		if ks.LastError != "" {
			fmt.Fprintf(e.stdout, " (error: %s)", ks.LastError)
		}
		fmt.Fprintln(e.stdout)
	}
	if len(ks.Errors) > 0 {
		fmt.Fprintf(e.stdout, "%d files failed in the last pass:\n", len(ks.Errors))
		for _, msg := range ks.Errors {
			fmt.Fprintf(e.stdout, "  %s\n", msg)
		}
	}
	if ks.Status.DurabilityUnknown {
		fmt.Fprintln(e.stdout, "durability unknown (server unreachable): nothing is reported expirable")
	}
	keys := make([]string, 0, len(ks.Counts))
	for k := range ks.Counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(e.stdout, "  %-10s %d\n", k, ks.Counts[k])
	}
	if *verbose {
		tw := tabwriter.NewWriter(e.stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "STATE\tPATH\tMODIFIED\tNEXT")
		for _, f := range ks.Status.Files {
			next := ""
			switch {
			case !f.KeepAt.IsZero():
				next = "keep at " + f.KeepAt.Format("2006-01-02 15:04")
			case !f.ExpireAt.IsZero():
				next = "expire at " + f.ExpireAt.Format("2006-01-02 15:04")
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", f.State, f.Path, f.ModTime.Format("2006-01-02 15:04"), next)
		}
		tw.Flush()
	}
	return nil
}

func (e *env) restore(args []string) error {
	fs := newFlags(e, "restore")
	var p node.RestoreParams
	fs.StringVar(&p.To, "to", "", "restore into this directory instead of the folder")
	fs.BoolVar(&p.Force, "force", false, "overwrite local files that differ from the kept version")
	fs.BoolVar(&p.Deleted, "deleted", false, "also restore files that were deleted in sync (expired files are always restored)")
	at := fs.String("at", "", "restore the archive as the server had kept it at this time (when versions were kept, not file mtimes): RFC 3339, YYYY-MM-DD[ HH:MM] local time, or an age such as 3d or 12h")
	pos, err := parse(fs, args, 1, 2, "<folder> [<path>] [--at <time>] [--to <dir>] [--force] [--deleted]")
	if err != nil {
		return err
	}
	if *at != "" {
		if p.At, err = parseAt(*at, time.Now()); err != nil {
			return err
		}
	}
	p.Folder = pos[0]
	if len(pos) == 2 {
		p.Path = pos[1]
	}
	var res node.RestoreResult
	if err := e.nodeCall(node.MethodRestore, p, &res); err != nil {
		return err
	}
	for _, f := range res.Restored {
		fmt.Fprintf(e.stdout, "restored  %s\n", f)
	}
	for _, f := range res.Skipped {
		fmt.Fprintf(e.stdout, "skipped   %s (differs locally; --force to overwrite)\n", f)
	}
	for _, f := range res.Deleted {
		fmt.Fprintf(e.stdout, "skipped   %s (deleted in sync; --deleted to restore)\n", f)
	}
	for _, f := range res.Errors {
		fmt.Fprintf(e.stdout, "error     %s\n", f)
	}
	asOf := ""
	if !p.At.IsZero() {
		asOf = " (as kept at " + p.At.Format("2006-01-02 15:04:05 MST") + ")"
	}
	deleted := ""
	if len(res.Deleted) > 0 {
		deleted = fmt.Sprintf(", %d skipped (deleted in sync)", len(res.Deleted))
	}
	fmt.Fprintf(e.stdout, "%d restored, %d already present, %d skipped%s, %d errors into %s%s\n",
		len(res.Restored), len(res.Unchanged), len(res.Skipped), deleted, len(res.Errors), res.Dir, asOf)
	if res.Reexpire {
		fmt.Fprintln(e.stdout, "note: some restored files are past the folder's expire window and will be removed from sync again on the next keeper pass; use --to to restore outside the folder")
	}
	if len(res.Errors) > 0 {
		return errors.New("some files were not restored")
	}
	return nil
}

// parseAt reads restore --at: an RFC 3339 time, a local date or date and
// time, or an age before now ("3d", "12h").
func parseAt(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	if d, err := config.ParseDuration(s); err == nil && d > 0 {
		return now.Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("--at %q: want RFC 3339, YYYY-MM-DD[ HH:MM], or an age like 3d", s)
}

func (e *env) gatewayConfig(args []string) error {
	fs := newFlags(e, "gateway config")
	path := fs.String("path", "", "Perkeep client config to write (default: Perkeep's own client-config.json)")
	if _, err := parse(fs, args, 0, 0, "[--path <client-config.json>]"); err != nil {
		return err
	}
	var gi node.GatewayInfo
	if err := e.nodeCall(node.MethodGateway, nil, &gi); err != nil {
		return err
	}
	if !gi.Enabled {
		return errors.New("the gateway is off on this node (gateway.listen in node.toml is empty, or it failed to start; see the node log)")
	}
	p := *path
	if p == "" {
		var err error
		if p, err = gateway.DefaultClientConfigPath(); err != nil {
			return err
		}
	}
	isDefault, err := gateway.WriteClientConfig(p, gi.URL, gi.Token)
	if err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "wrote server %q (%s) to %s\n", gateway.ServerAlias, gi.URL, p)
	if isDefault {
		fmt.Fprintln(e.stdout, "it is the default server, so pk-get, pk-mount and the rest work without flags")
	} else {
		fmt.Fprintf(e.stdout, "another server is the default: pass -server=%s to Perkeep tools\n", gateway.ServerAlias)
	}
	fmt.Fprintln(e.stdout, "search and the web UI need read scope: on the server, keepsync server grant <this node> read")
	return nil
}

func (e *env) unkeep(args []string) error {
	fs := newFlags(e, "unkeep")
	pos, err := parse(fs, args, 1, 1, "<folder>")
	if err != nil {
		return err
	}
	var res node.UnkeepResult
	if err := e.nodeCall(node.MethodUnkeep, node.FolderParams{Name: pos[0]}, &res); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "stopped keeping %s on every node: no more uploads or expiry\n", pos[0])
	if res.Archive != "" {
		fmt.Fprintf(e.stdout, "the archive stays (%s): restore still reads it, and keepsync keep %s continues the same tree\n", res.Archive, pos[0])
	}
	return nil
}

func (e *env) conflicts(args []string) error {
	fs := newFlags(e, "conflicts")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	var list []node.ConflictInfo
	if err := e.nodeCall(node.MethodConflicts, nil, &list); err != nil {
		return err
	}
	if len(list) == 0 {
		fmt.Fprintln(e.stdout, "no conflicts")
		return nil
	}
	tw := tabwriter.NewWriter(e.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "FOLDER\tCONFLICT COPY\tORIGINAL\tNODE\tHOOK")
	for _, c := range list {
		state := c.State
		if c.Reason != "" {
			state += ": " + c.Reason
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", c.Folder, c.Path, c.Original, c.Node, state)
	}
	return tw.Flush()
}

func (e *env) conflictsResolve(args []string) error {
	fs := newFlags(e, "conflicts resolve")
	pos, err := parse(fs, args, 1, 1, "<conflict copy path>")
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(pos[0])
	if err != nil {
		return err
	}
	// The hook may run for its whole timeout, so wait longer than other calls.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var ci node.ConflictInfo
	sock := (&config.Node{Dir: e.nodeDir()}).SocketPath()
	if err := ctl.Call(ctx, sock, node.MethodResolve, node.FolderParams{Path: abs}, &ci); err != nil {
		if isNotRunning(err) {
			return fmt.Errorf("the node is not running (start it with `keepsync node run`): %w", err)
		}
		return err
	}
	if ci.State == node.HookResolved {
		fmt.Fprintf(e.stdout, "resolved %s\n", ci.Original)
		return nil
	}
	return fmt.Errorf("not resolved (%s): %s", ci.State, ci.Reason)
}
