// Package kit builds the encrypted recovery kit and rebuilds a server from
// it (docs/design.md, "Recovery kit").
//
// A kit is a zip, encrypted with age under a scrypt passphrase
// (keepsync-recovery.zip.age). It holds everything needed to rebuild the
// server that is not in the bucket: the keepsync and pinned zerofs
// binaries for the current platform, the bucket's location and
// credentials, the ZeroFS password, the Perkeep identity, the server's
// tailcat key and config, RECOVER.md, and SHA256SUMS.
//
// Build or Write make a kit; Open decrypts and verifies one. Recover
// mounts ZeroFS on the bucket, starts perkeepd on a fresh local storage and
// index, and copies every blob of the bucket's diskpacked replica into it,
// which reindexes them; then it installs the tailcat key and config so
// `server run` brings the server back at the same address. Drill runs the
// same restore against a copy of the bucket, without installing the
// identity, and checks blobs, files, and the index against a Baseline the
// live server made with Snapshot.
//
// perkeepd runs once per process and holds its queue locks until exit, so
// a restored server, a drill, and a live server each need their own process.
package kit
