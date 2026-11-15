// Package hook runs a conflict hook script for one Syncthing conflict copy.
//
// The script gets the conflict in its environment (see Job) and runs in the
// folder root. To resolve, it writes the result to the original path,
// deletes the conflict copy and exits 0. A non-zero exit, a timeout, or a
// conflict copy still in place leaves the conflict for a human.
package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// DefaultTimeout bounds a hook run unless Job.Timeout says otherwise.
const DefaultTimeout = 5 * time.Minute

// Environment variables the hook receives.
const (
	EnvFolder       = "KEEPSYNC_FOLDER"
	EnvOriginal     = "KEEPSYNC_ORIGINAL"
	EnvConflict     = "KEEPSYNC_CONFLICT"
	EnvConflictNode = "KEEPSYNC_CONFLICT_NODE"
	EnvBase         = "KEEPSYNC_BASE"
)

// Job is one hook run.
type Job struct {
	// Script is the hook executable.
	Script string
	// Folder is the folder name; Dir its root, which is the working dir.
	Folder string
	Dir    string
	// Original and Conflict are absolute paths.
	Original string
	Conflict string
	// ConflictNode names the node whose change the copy holds.
	ConflictNode string
	// Base is a file holding the last kept version of the original, or
	// "" if there is none.
	Base    string
	Timeout time.Duration
	// Logf receives the script's output, one line per call.
	Logf func(format string, args ...any)
}

// Result is the outcome of a run.
type Result struct {
	Resolved bool
	// Reason says why the conflict is not resolved.
	Reason   string
	ExitCode int
	Duration time.Duration
}

// Run runs the hook and reports whether it resolved the conflict.
func Run(ctx context.Context, j Job) Result {
	timeout := j.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	logf := j.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.Command(j.Script)
	cmd.Dir = j.Dir
	cmd.Env = append(os.Environ(),
		EnvFolder+"="+j.Folder,
		EnvOriginal+"="+j.Original,
		EnvConflict+"="+j.Conflict,
		EnvConflictNode+"="+j.ConflictNode,
		EnvBase+"="+j.Base,
	)
	out := &lineWriter{logf: logf, prefix: "hook: "}
	cmd.Stdout, cmd.Stderr = out, out
	setProcessGroup(cmd)

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return Result{Reason: fmt.Sprintf("start %s: %v", j.Script, err), ExitCode: -1}
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	timedOut := false
	select {
	case err = <-done:
	case <-ctx.Done():
		timedOut = true
		killProcessGroup(cmd)
		err = <-done
	}
	out.flush()
	res := Result{Duration: time.Since(start), ExitCode: cmd.ProcessState.ExitCode()}
	switch {
	case timedOut:
		res.Reason = fmt.Sprintf("timed out after %s", timeout)
	case err != nil:
		res.Reason = fmt.Sprintf("exited with %v", err)
	default:
		if _, err := os.Lstat(j.Conflict); err == nil {
			res.Reason = "exited 0 but left the conflict copy in place"
		} else if !errors.Is(err, fs.ErrNotExist) {
			res.Reason = fmt.Sprintf("check conflict copy: %v", err)
		} else {
			res.Resolved = true
		}
	}
	return res
}

// lineWriter turns output into log lines.
type lineWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	logf   func(string, ...any)
	prefix string
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			w.buf.WriteString(line)
			break
		}
		w.logf("%s%s", w.prefix, strings.TrimRight(line, "\r\n"))
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() > 0 {
		w.logf("%s%s", w.prefix, w.buf.String())
		w.buf.Reset()
	}
}
