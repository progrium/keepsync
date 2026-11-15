package main

import (
	"fmt"
	"log"
	"os"
	"text/tabwriter"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/ctl"
	"github.com/progrium/keepsync/internal/kit"
	"github.com/progrium/keepsync/internal/registry"
	"github.com/progrium/keepsync/internal/server"
)

func (e *env) serverInit(args []string) error {
	fs := newFlags(e, "server init")
	var o server.InitOptions
	fs.StringVar(&o.Storage.URL, "bucket", "", "bucket and prefix, such as s3://bucket/keepsync (file:///path for local testing)")
	fs.StringVar(&o.Storage.Endpoint, "endpoint", "", "S3 endpoint for non-AWS services (R2, MinIO)")
	fs.StringVar(&o.Storage.Region, "region", "", "S3 region")
	fs.StringVar(&o.Storage.AccessKeyID, "access-key", os.Getenv("AWS_ACCESS_KEY_ID"), "S3 access key ID (default $AWS_ACCESS_KEY_ID)")
	fs.StringVar(&o.Storage.SecretAccessKey, "secret-key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "S3 secret key (default $AWS_SECRET_ACCESS_KEY)")
	fs.BoolVar(&o.Storage.AllowHTTP, "allow-http", false, "allow a plain-http endpoint")
	fs.StringVar(&o.ZeroFS, "zerofs", "", "zerofs binary (default: PATH, then the server dir, else download the pinned release)")
	fs.StringVar(&o.PasswordFile, "zerofs-password-file", "", "adopt an existing bucket's ZeroFS password")
	fs.StringVar(&o.Mount, "mount", "", "ZeroFS mount mode: fuse (default), v9fs or external")
	fs.StringVar(&o.MountDir, "mount-dir", "", "ZeroFS mount point (default <server dir>/zerofs/mnt)")
	fs.BoolVar(&o.NoDownload, "no-download", false, "fail instead of downloading zerofs")
	kitOut := fs.String("kit", "", "where to write the recovery kit (default ./"+kit.FileName+")")
	netFlags(fs, &o.Network)
	if _, err := parse(fs, args, 0, 0, "--bucket <url> [flags]"); err != nil {
		return err
	}
	o.Dir = e.serverDir()
	o.Logf = func(f string, a ...any) { fmt.Fprintf(e.stderr, f+"\n", a...) }
	ctx, cancel := signalContext()
	defer cancel()
	res, err := server.Init(ctx, o)
	if err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "server initialized in %s\n", o.Dir)
	fmt.Fprintf(e.stdout, "zerofs:  %s\n", res.ZeroFS)
	fmt.Fprintf(e.stdout, "key:     %s\n", res.Key)
	fmt.Fprintf(e.stdout, "address: %s\n\n", res.Addr)
	fmt.Fprintf(e.stdout, "Next: keepsync server run, then on each node:\n  keepsync node init --server %s\n", res.Addr)
	return e.writeKit(ctx, res.Config, *kitOut, res.Addr)
}

func netFlags(fs interface {
	StringVar(*string, string, string, string)
	IntVar(*int, string, int, string)
}, n *config.Network) {
	fs.StringVar(&n.DERPMapURL, "derp-map", "", "DERP map URL or local JSON file, for a self-hosted derper")
	fs.IntVar(&n.RegionID, "derp-region", 0, "DERP region ID to pin")
}

func (e *env) serverRun(args []string) error {
	fs := newFlags(e, "server run")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	cfg, err := config.LoadServer(e.serverDir())
	if err != nil {
		return err
	}
	ctx, cancel := signalContext()
	defer cancel()
	l := log.New(e.stderr, "", log.LstdFlags)
	return server.Run(ctx, cfg, server.Options{Logf: l.Printf})
}

func (e *env) serverCall(method string, params, out any) error {
	ctx, cancel := callContext()
	defer cancel()
	sock := (&config.Server{Dir: e.serverDir()}).SocketPath()
	err := ctl.Call(ctx, sock, method, params, out)
	if err != nil && isNotRunning(err) {
		return fmt.Errorf("the server is not running (start it with `keepsync server run`): %w", err)
	}
	return err
}

func (e *env) serverAdd(args []string) error {
	fs := newFlags(e, "server add")
	name := fs.String("name", "", "node name (default: the name the node registers with)")
	pos, err := parse(fs, args, 1, 1, "<nodekey> [--name <name>]")
	if err != nil {
		return err
	}
	if err := e.serverCall(server.MethodAdd, server.AddParams{Key: api.NodeKey(pos[0]), Name: *name}, nil); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "approved %s\n", pos[0])
	return nil
}

func (e *env) serverRemove(args []string) error {
	fs := newFlags(e, "server remove")
	pos, err := parse(fs, args, 1, 1, "<name or nodekey>")
	if err != nil {
		return err
	}
	if err := e.serverCall(server.MethodRemove, server.RemoveParams{Name: pos[0]}, nil); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "removed %s\n", pos[0])
	return nil
}

func (e *env) serverNodes(args []string) error {
	fs := newFlags(e, "server nodes")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	var nodes []registry.NodeInfo
	if err := e.serverCall(server.MethodNodes, nil, &nodes); err != nil {
		return err
	}
	printNodes(e, nodes)
	return nil
}

func printNodes(e *env, nodes []registry.NodeInfo) {
	if len(nodes) == 0 {
		fmt.Fprintln(e.stdout, "no nodes yet: approve one with `keepsync server add <nodekey>`")
		return
	}
	tw := tabwriter.NewWriter(e.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSTATE\tKEY\tDEVICE")
	for _, n := range nodes {
		state := "approved"
		switch {
		case n.Online:
			state = "online"
		case n.Registered:
			state = "offline"
		}
		name := n.Name
		if name == "" {
			name = "-"
		}
		dev := string(n.Device)
		if len(dev) > 7 {
			dev = dev[:7]
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", name, state, n.Key, dev)
	}
	tw.Flush()
}

func (e *env) serverStatus(args []string) error {
	fs := newFlags(e, "server status")
	if _, err := parse(fs, args, 0, 0, ""); err != nil {
		return err
	}
	var st server.Status
	if err := e.serverCall(server.MethodStatus, nil, &st); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "address: %s\nkey:     %s\n", st.Addr, st.Key)
	if st.FlushErr != "" {
		fmt.Fprintf(e.stdout, "flush:   FAILING: %s\n", st.FlushErr)
	} else if !st.LastFlush.IsZero() {
		fmt.Fprintf(e.stdout, "flush:   last at %s\n", st.LastFlush.Format("2006-01-02 15:04:05"))
	} else {
		fmt.Fprintln(e.stdout, "flush:   none yet")
	}
	fmt.Fprintf(e.stdout, "registry version %d, %d folders\n\n", st.Version, len(st.Folders))
	printNodes(e, st.Nodes)
	return nil
}
