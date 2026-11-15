package zerofs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/hugelgupf/p9/fsimpl/localfs"
	"github.com/hugelgupf/p9/p9"
)

// The test binary doubles as a stand-in zerofs: with KEEPSYNC_FAKE_ZEROFS set
// it implements just enough of "zerofs run" and "zerofs flush" for the
// supervisor tests. Events go to the file named by that variable.

const fakeEnv = "KEEPSYNC_FAKE_ZEROFS"

func TestMain(m *testing.M) {
	if path := os.Getenv(fakeEnv); path != "" {
		os.Exit(fakeMain(path, os.Args[1:]))
	}
	os.Exit(m.Run())
}

func fakeEvent(path, format string, args ...any) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	fmt.Fprintf(f, format+"\n", args...)
	f.Close()
}

func fakeSockets(config string) (ninep, rpc string, err error) {
	b, err := os.ReadFile(config)
	if err != nil {
		return "", "", err
	}
	section := ""
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			section = line
			continue
		}
		if v, ok := strings.CutPrefix(line, "unix_socket = "); ok {
			v, _ = strconv.Unquote(v)
			switch section {
			case "[servers.ninep]":
				ninep = v
			case "[servers.rpc]":
				rpc = v
			}
		}
	}
	return ninep, rpc, nil
}

func fakeMain(events string, args []string) int {
	if len(args) < 3 || args[1] != "--config" {
		fmt.Fprintln(os.Stderr, "fake zerofs: bad args", args)
		return 2
	}
	if os.Getenv(envPassword) == "" {
		fmt.Fprintln(os.Stderr, "fake zerofs: password not in environment")
		return 1
	}
	ninep, rpc, err := fakeSockets(args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	switch args[0] {
	case "run":
		fakeEvent(events, "run")
		if f := os.Getenv("FAKE_FAIL_START_FILE"); f != "" {
			if _, err := os.Stat(f); err == nil {
				fmt.Fprintln(os.Stderr, "fake zerofs: failing on purpose")
				return 1
			}
		}
		if os.Getenv("FAKE_FAIL_START") != "" {
			fmt.Fprintln(os.Stderr, "fake zerofs: failing on purpose")
			return 1
		}
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGTERM)
		l9, err := net.Listen("unix", ninep)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if root := os.Getenv("FAKE_NINEP_ROOT"); root != "" {
			// Serve root over 9P, for MountNineP tests.
			go p9.NewServer(localfs.Attacher(root)).Serve(l9)
		} else {
			go func() {
				for {
					c, err := l9.Accept()
					if err != nil {
						return
					}
					c.Close()
				}
			}()
		}
		lr, err := net.Listen("unix", rpc)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		die := make(chan struct{})
		go func() {
			for {
				c, err := lr.Accept()
				if err != nil {
					return
				}
				line, _ := bufio.NewReader(c).ReadString('\n')
				switch strings.TrimSpace(line) {
				case "flush":
					fakeEvent(events, "flush")
					fmt.Fprintln(c, "ok")
				case "die":
					close(die)
				}
				c.Close()
			}
		}()
		select {
		case <-term:
			fakeEvent(events, "term")
			l9.Close()
			lr.Close()
			return 0
		case <-die:
			fakeEvent(events, "died")
			return 3
		}
	case "flush":
		c, err := net.Dial("unix", rpc)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to connect to RPC server. Is the server running?")
			return 1
		}
		defer c.Close()
		fmt.Fprintln(c, "flush")
		reply, _ := bufio.NewReader(c).ReadString('\n')
		if strings.TrimSpace(reply) != "ok" {
			return 1
		}
		return 0
	}
	return 2
}

// fakeSupervisor returns a Supervisor running the test binary as zerofs,
// with a plain-directory mounter so no FUSE or root is needed.
func fakeSupervisor(t *testing.T, extraEnv ...string) (*Supervisor, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Short data dir: unix socket paths are limited to ~104 bytes.
	dir, err := os.MkdirTemp("", "zfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	events := filepath.Join(dir, "events")
	s, err := New(Config{
		Binary:   exe,
		DataDir:  filepath.Join(dir, "data"),
		Storage:  Storage{URL: "s3://bucket/prefix", AccessKeyID: "test-akid-value", SecretAccessKey: "test-secret-value"},
		Password: "0123456789abcdef0123456789abcdef",
		Env:      append([]string{fakeEnv + "=" + events}, extraEnv...),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	s.mount = &dirMount{}
	return s, events
}

// dirMount is a test mounter: the "mount" is the plain directory.
type dirMount struct {
	mounts, unmounts int
	err              error
}

func (d *dirMount) Mount(context.Context, mountSpec) error { d.mounts++; return nil }
func (d *dirMount) Unmount(context.Context) error          { d.unmounts++; return nil }
func (d *dirMount) Err() error                             { return d.err }

func dialUnix(path string) (net.Conn, error) { return net.Dial("unix", path) }
