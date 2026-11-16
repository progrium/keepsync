// Package zerofs supervises the ZeroFS process that backs the server's
// diskpacked replica (api.ZeroFS).
//
// ZeroFS is a separate, unmodified AGPL program run as a subprocess. keepsync
// pins one release (Pinned): FindBinary looks for it as --zerofs, then on
// PATH, then in keepsync's data dir, and Release.Install downloads it there,
// checking the SHA-256 of both the release tarball and the platform binary.
// A binary that does not match the pin produces a warning, never an upgrade.
//
// Supervisor writes zerofs.toml (9P and RPC on private unix sockets only, no
// NFS, telemetry off; secrets passed through the environment, never written
// to the file), starts "zerofs run", waits for both sockets, and mounts the
// filesystem over 9P at Dir. Flush syncs the mount and runs "zerofs flush",
// so a nil return means everything written so far is in the bucket. Stop
// flushes, unmounts, and sends zerofs SIGTERM.
//
// See docs/design.md, "ZeroFS, two modes".
package zerofs
