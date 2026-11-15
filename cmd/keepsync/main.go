// Command keepsync keeps folders synced across machines with embedded
// Syncthing and archives them in Perkeep on an S3-compatible bucket.
//
// See docs/design.md for the CLI reference.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/progrium/keepsync/internal/config"
)

const usage = `usage: keepsync [--dir <base>] <command> [args]

Server:
  server init --bucket <url> [flags]   create the server (key, perkeep, zerofs)
  server run                           run the server
  server add <nodekey> [--name <n>]    approve a node
  server remove <name>                 remove a node
  server nodes                         list members
  server status                        show the running server
  server kit [--out <path>]            regenerate the recovery kit
  server baseline <out.json>           flush and snapshot, for a recovery drill
  server grant <name> <blobs|read>     set a node's Perkeep gateway scope
                                       (read covers every folder's archive)

Node:
  node init --server <tc… or name>     create this node, print its key
  node run                             run the node
  status                               server connection, peers, folders
  folder add <name> <path>             create a synced folder
  folder join <name> <path> [--merge] attach an existing folder here;
                                       --merge if <path> already has files
  folder ls                            list folders
  folder rm <name>                     stop syncing a folder here (files stay)
  keep <folder> [--grace 48h] [--max-defer 7d] [--expire 30d]
                                       mark a folder kept
  keep status <folder>                 files in grace, kept, expirable
  unkeep <folder>                      stop keeping a folder; the archive stays
  conflicts                            list conflict copies and hook state
  conflicts resolve <path>             run the conflict hook now, on this node
  restore <folder> [<path>] [--at <time>] [--to <dir>] [--force]
                                       pull kept files back, including expired;
                                       --at is when the server kept them
  gateway config [--path <file>]       point Perkeep tools at this node's gateway

Any:
  recover <kit>                        rebuild the server from the bucket
  recover <kit> --drill --bucket <copy> [--baseline <file>]
                                       restore a bucket copy and check it

The base dir defaults to $KEEPSYNC_DIR, else $XDG_DATA_HOME/keepsync, else
~/.local/share/keepsync. The server lives in <base>/server and the node in
<base>/node, so one machine can run both.
`

type env struct {
	base   string
	stdout io.Writer
	stderr io.Writer
}

func (e *env) serverDir() string { return config.RoleDir(e.base, config.ServerRole) }
func (e *env) nodeDir() string   { return config.RoleDir(e.base, config.NodeRole) }

func main() {
	e := &env{stdout: os.Stdout, stderr: os.Stderr}
	if err := run(e, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "keepsync:", err)
		os.Exit(1)
	}
}

func run(e *env, args []string) error {
	fs := flag.NewFlagSet("keepsync", flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	fs.Usage = func() { fmt.Fprint(e.stderr, usage) }
	fs.StringVar(&e.base, "dir", config.BaseDir(), "base directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if len(args) == 0 {
		fs.Usage()
		return flag.ErrHelp
	}
	cmd, rest := args[0], args[1:]
	sub := func() (string, []string) {
		if len(rest) == 0 {
			return "", nil
		}
		return rest[0], rest[1:]
	}
	switch cmd {
	case "server":
		s, a := sub()
		switch s {
		case "init":
			return e.serverInit(a)
		case "run":
			return e.serverRun(a)
		case "add":
			return e.serverAdd(a)
		case "remove", "rm":
			return e.serverRemove(a)
		case "nodes":
			return e.serverNodes(a)
		case "status":
			return e.serverStatus(a)
		case "kit":
			return e.serverKit(a)
		case "baseline":
			return e.serverBaseline(a)
		case "grant":
			return e.serverGrant(a)
		}
	case "node":
		s, a := sub()
		switch s {
		case "init":
			return e.nodeInit(a)
		case "run":
			return e.nodeRun(a)
		}
	case "status":
		return e.status(rest)
	case "folder":
		s, a := sub()
		switch s {
		case "add":
			return e.folderAttach(a, "folder add", true)
		case "join":
			return e.folderAttach(a, "folder join", false)
		case "ls", "list":
			return e.folderLs(a)
		case "rm", "remove":
			return e.folderRm(a)
		}
	case "keep":
		if len(rest) > 0 && rest[0] == "status" {
			return e.keepStatus(rest[1:])
		}
		return e.keep(rest)
	case "gateway":
		s, a := sub()
		if s == "config" {
			return e.gatewayConfig(a)
		}
	case "unkeep":
		return e.unkeep(rest)
	case "restore":
		return e.restore(rest)
	case "conflicts":
		if len(rest) > 0 && rest[0] == "resolve" {
			return e.conflictsResolve(rest[1:])
		}
		return e.conflicts(rest)
	case "recover":
		return e.recover(rest)
	case "purge":
		return notYet("purge", 0)
	case "help", "-h", "--help":
		fmt.Fprint(e.stdout, usage)
		return nil
	}
	fs.Usage()
	return flag.ErrHelp
}

func notYet(cmd string, milestone int) error {
	if milestone == 0 {
		return fmt.Errorf("%s is not implemented yet", cmd)
	}
	return fmt.Errorf("%s is not implemented yet (milestone %d)", cmd, milestone)
}

// parse parses flags that may appear before, after or between positional
// arguments, and checks the number of positionals.
func parse(fs *flag.FlagSet, args []string, min, max int, argUsage string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			break
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
	if len(pos) < min || (max >= 0 && len(pos) > max) {
		return nil, fmt.Errorf("usage: keepsync %s %s", fs.Name(), argUsage)
	}
	return pos, nil
}

func newFlags(e *env, name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	return fs
}

// signalContext is cancelled on SIGINT or SIGTERM.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// callContext bounds one control-socket call.
func callContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

// durationFlag is a flag.Value accepting "7d" as well as Go durations.
type durationFlag struct{ d *time.Duration }

func (f durationFlag) String() string {
	if f.d == nil || *f.d == 0 {
		return ""
	}
	return f.d.String()
}

func (f durationFlag) Set(s string) error {
	v, err := config.ParseDuration(s)
	if err != nil {
		return err
	}
	*f.d = v
	return nil
}

// fmtDur prints durations the way the docs write them: whole days as "7d".
func fmtDur(d time.Duration) string {
	if d == 0 {
		return "0"
	}
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}
