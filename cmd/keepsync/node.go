package main

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
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
	pos, err := parse(fs, args, 2, 2, "<name> <path>")
	if err != nil {
		return err
	}
	method := node.MethodFolderJoin
	if add {
		method = node.MethodFolderAdd
	}
	var fi node.FolderInfo
	if err := e.nodeCall(method, node.FolderParams{Name: pos[0], Path: pos[1]}, &fi); err != nil {
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
