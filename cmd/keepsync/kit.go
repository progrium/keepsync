package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/term"

	"github.com/progrium/keepsync/internal/api"
	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/kit"
	"github.com/progrium/keepsync/internal/server"
	"github.com/progrium/keepsync/internal/zerofs"
)

// writeKit writes the recovery kit and prints the passphrase once.
func (e *env) writeKit(ctx context.Context, cfg *config.Server, out string, addr api.Addr) error {
	if out == "" {
		out = kit.FileName
	}
	if fi, err := os.Stat(out); err == nil && fi.IsDir() {
		out = filepath.Join(out, kit.FileName)
	}
	pass, err := kit.Write(ctx, cfg, out, addr)
	if err != nil {
		return fmt.Errorf("recovery kit: %w", err)
	}
	abs, _ := filepath.Abs(out)
	fmt.Fprintf(e.stdout, "\nRecovery kit: %s\nPassphrase:   %s\n\n", abs, pass)
	fmt.Fprintln(e.stdout, "The passphrase is shown only this once. Store the kit and the passphrase")
	fmt.Fprintln(e.stdout, "separately and offline, and never in the bucket: together they can read all")
	fmt.Fprintln(e.stdout, "data and impersonate the server.")
	return nil
}

func (e *env) serverKit(args []string) error {
	fs := newFlags(e, "server kit")
	out := fs.String("out", "", "where to write the kit (default ./"+kit.FileName+")")
	if _, err := parse(fs, args, 0, 0, "[--out <path>]"); err != nil {
		return err
	}
	cfg, err := config.LoadServer(e.serverDir())
	if err != nil {
		return err
	}
	// The address is in the saved key's region; ask the running server if
	// there is one, else let the kit derive what it can.
	var addr api.Addr
	var st server.Status
	if e.serverCall(server.MethodStatus, nil, &st) == nil {
		addr = st.Addr
	}
	ctx, cancel := signalContext()
	defer cancel()
	return e.writeKit(ctx, cfg, *out, addr)
}

func (e *env) serverBaseline(args []string) error {
	fs := newFlags(e, "server baseline")
	pos, err := parse(fs, args, 1, 1, "<out.json>")
	if err != nil {
		return err
	}
	out, err := filepath.Abs(pos[0])
	if err != nil {
		return err
	}
	if err := e.serverCall(server.MethodBaseline, server.BaselineParams{Path: out}, nil); err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "baseline written to %s\nNow copy the bucket (e.g. rclone copy) and run:\n  keepsync recover <kit> --drill --bucket <copy> --baseline %s\n", out, out)
	return nil
}

func (e *env) recover(args []string) error {
	fs := newFlags(e, "recover")
	drill := fs.Bool("drill", false, "restore a copy of the bucket and check it, without taking over the identity")
	bucket := fs.String("bucket", "", "the bucket copy to drill against (--drill)")
	baseline := fs.String("baseline", "", "baseline JSON from `keepsync server baseline` (--drill)")
	passFile := fs.String("passphrase-file", "", "read the kit passphrase from a file (default $KEEPSYNC_KIT_PASSPHRASE, else prompt)")
	accessKey := fs.String("access-key", "", "bucket write credentials, if the kit holds a read-only token")
	secretKey := fs.String("secret-key", "", "bucket secret key for --access-key")
	mount := fs.String("mount", "", "ZeroFS mount mode: fuse (default), v9fs or external")
	pos, err := parse(fs, args, 1, 1, "<kit> [--drill --bucket <copy> [--baseline <file>]]")
	if err != nil {
		return err
	}
	pass, err := e.passphrase(*passFile)
	if err != nil {
		return err
	}
	k, err := kit.Open(pos[0], pass)
	if err != nil {
		return err
	}
	defer k.Close()
	creds := kit.Credentials{AccessKeyID: *accessKey, SecretAccessKey: *secretKey}
	logf := func(f string, a ...any) { fmt.Fprintf(e.stderr, f+"\n", a...) }
	ctx, cancel := signalContext()
	defer cancel()

	if *drill {
		if *bucket == "" {
			return errors.New("--drill needs --bucket <copy of the bucket>")
		}
		o := kit.DrillOptions{Bucket: *bucket, Credentials: creds, Mount: zerofs.MountMode(*mount), Logf: logf}
		if *baseline != "" {
			b, err := os.ReadFile(*baseline)
			if err != nil {
				return err
			}
			o.Baseline = &kit.Baseline{}
			if err := json.Unmarshal(b, o.Baseline); err != nil {
				return fmt.Errorf("baseline %s: %w", *baseline, err)
			}
		}
		rep, err := kit.Drill(ctx, k, o)
		if err != nil {
			return err
		}
		fmt.Fprintln(e.stdout, rep.String())
		if !rep.OK() {
			return errors.New("drill failed")
		}
		return nil
	}

	dir := e.serverDir()
	stats, err := kit.RecoverDir(ctx, k, dir, kit.RecoverOptions{Credentials: creds, Mount: zerofs.MountMode(*mount), Logf: logf})
	if err != nil {
		return err
	}
	fmt.Fprintf(e.stdout, "recovered %d blobs (%d bytes) into %s in %s\n", stats.Blobs, stats.Bytes, dir, stats.Total.Round(1e9))
	if len(stats.BadBlobs) > 0 {
		fmt.Fprintf(e.stdout, "WARNING: %d blobs failed their hash check and were not restored\n", len(stats.BadBlobs))
	}
	fmt.Fprintf(e.stdout, "Now start it: keepsync server run (address %s)\n", k.Manifest.ServerAddr)
	return nil
}

func (e *env) passphrase(file string) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		return strings.TrimSpace(string(b)), err
	}
	if p := os.Getenv("KEEPSYNC_KIT_PASSPHRASE"); p != "" {
		return p, nil
	}
	fmt.Fprint(e.stderr, "Kit passphrase: ")
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(e.stderr)
		return strings.TrimSpace(string(b)), err
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
