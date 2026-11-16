# keepsync

keepsync keeps a set of folders synced across your machines and archives them permanently. It is one Go binary that embeds Syncthing for sync and Perkeep for the archive, stores the archive in any S3-compatible bucket through ZeroFS, and connects everything over tailcat, so nothing needs a public IP, open ports or DNS.

A synced folder can hold just recent files (say the last 30 days) while the archive keeps every version forever. Files only leave sync once they are safely in the bucket, and you can restore any of them, as they are now or as they were at an earlier time.

For an overview of the pieces and why they were chosen, see the [architecture](docs/architecture.md). For how it works in detail, see the [spec](docs/spec.md).

## Getting started

### How it fits together

You run one **server** and any number of **nodes**. Every machine that syncs folders is a node. The server holds the Perkeep archive, ZeroFS and the list of approved nodes, and it is the only machine that talks to the bucket. One machine can be both a server and a node.

Nodes sync directly with each other, so sync keeps working while the server is down. Only archiving, expiry and membership changes wait for it.

### Prerequisites

- **Go 1.27.2** or newer, to build keepsync.
- **tailcat.** Nothing to install. keepsync embeds tailcat as a library, and `go.mod` pins a version from tailcat's `main` branch, which `go build` fetches. By default traffic is relayed through tailcat's public DERP relays when a direct connection can't be made. Those are rate-limited, so for real sync volumes run your own `derper` and pass `--derp-map` and `--derp-region` to `server init` and `node init`.
- **An S3-compatible bucket** for the archive, such as Cloudflare R2, AWS S3 or MinIO, with an access key that can read and write it.
- **ZeroFS v2.3.7** on the server. You don't need to install it: `server init` downloads the pinned release and checks its SHA-256 if it isn't on your `PATH`. Builds exist for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and freebsd/amd64.

### Build

keepsync vendors a patched Syncthing through a `replace` directive, so `go install …@version` won't work. Clone and build instead:

```sh
git clone https://github.com/progrium/keepsync
cd keepsync
go build -o keepsync ./cmd/keepsync
```

Put the `keepsync` binary on your `PATH` on the server and on every node.

Each machine keeps its state under a base directory: `--dir`, else `$KEEPSYNC_DIR`, else `$XDG_DATA_HOME/keepsync`, else `~/.local/share/keepsync`. The server uses `<base>/server` and the node uses `<base>/node`.

### 1. Set up the server

Create the server, pointing it at your bucket. The bucket URL is `s3://<bucket>/<prefix>`. For anything other than AWS, also give the endpoint. Credentials default to `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_ACCESS_KEY`.

```sh
# Cloudflare R2
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
keepsync server init \
  --bucket s3://my-bucket/keepsync \
  --endpoint https://<account-id>.r2.cloudflarestorage.com \
  --region auto
```

For AWS S3, drop `--endpoint` and pass your `--region`. For a local MinIO over plain HTTP, add `--allow-http`.

`server init` takes `--mode store` (the default) or `--mode cache`. A **store** server keeps every archived blob on its own disk and copies it to the bucket; reads are local. A **cache** server keeps no blob store on disk: Perkeep writes straight to the bucket through ZeroFS, and local disk holds only the index and ZeroFS's cache, which you size with `--cache-size <GB>` (default 10, minimum 2). Use cache mode when the archive is bigger than the server's disk; reading old files then fetches them from the bucket. The mode can't change after init, but a recovery can pick either (see [Recover a lost server](#recover-a-lost-server)).

`server init` creates the server's tailcat key, its Perkeep identity and the ZeroFS config, finds or downloads ZeroFS, and writes the **recovery kit** to `./keepsync-recovery.zip.age` (or `--kit <path>`). It prints:

- the server's address (`tc…`), which nodes need to join;
- the kit's passphrase, **shown only this once**.

Store the kit and the passphrase separately and offline, and never in the bucket. Together they can read all your data and impersonate the server. See [Recovery kit](#recovery-kit).

Then start the server and leave it running (under systemd, launchd or similar):

```sh
keepsync server run
```

It starts ZeroFS, starts Perkeep and the registry, and flushes to the bucket every minute. Stopping it with Ctrl-C, SIGTERM or SIGHUP drains and flushes everything to the bucket first; a second Ctrl-C quits at once. ZeroFS never outlives the server: on Linux it stops when keepsync dies for any reason, and on any platform `server run` stops a ZeroFS left running by an earlier run before starting its own.

Check on it with:

```sh
keepsync server status   # address, last flush (or why flushing fails), mode, disk and bucket use, nodes
```

### 2. Join a node

On each machine that will sync, create the node with the server's address:

```sh
keepsync node init --server tc... --name laptop
```

`--name` defaults to the hostname. `--server` also accepts a DNS name whose TXT record holds the address, bare or as `keepsync=tc…`. It is looked up once and saved. Publishing the address is safe, because the server only lets in nodes you approve.

`node init` works offline. It prints the node's key and the exact command to approve it. Run that on the server:

```sh
keepsync server add nodekey:... --name laptop
```

Then start the node and leave it running:

```sh
keepsync node run
```

Check it with `keepsync status`, which shows the server connection with a short server summary (healthy or not, last flush, disk and bucket use, nodes online), then peers and folders:

```
PEER   DEVICE   PATH                  TRAFFIC
h9dev  X3NOEHO  direct 54.149.165.58  ↓ 20 B/s  ↑ 3.3 MB/s

FOLDER  SIZE      FILES   SYNC                      KEEP
demo    4.2 GB    1,234   syncing 63%, 1.5 GB left  -
photos  812.0 MB  20,511  up to date                kept, keeper
```

PATH says whether traffic to a peer goes direct or through a DERP relay, so you can tell when sync is stuck on a rate-limited relay (only one node of each pair dials, so a pair shares one tunnel, and keepsync keeps pinging a relayed tunnel so it can upgrade to direct). TRAFFIC is the rate each way, averaged over the last 10 seconds. If the node hasn't been approved yet, `status` prints the `server add` command again. `keepsync server nodes` on the server lists every node and whether it is online.

Repeat for every machine. The approved nodes find and connect to each other on their own.

### 3. Add folders

Folders have a name that is the same on every node and a path that is local to each node. The path is created if it doesn't exist.

On the first node, create the folder:

```sh
keepsync folder add docs ~/Documents
```

On every other node that should sync it, join it:

```sh
keepsync folder join docs ~/Documents
```

`folder join` refuses a directory that already has files, because they would sync into every node's copy of the folder. To do that on purpose, add `--merge`:

```sh
keepsync folder join docs ~/Documents --merge
```

`keepsync folder ls` lists folders, their members and their sync state. `keepsync folder status docs` shows one folder in detail: its state, what this node still needs, and each peer's completion, path and traffic. Add `-v` to list the files still needed, the ones being pulled first. `keepsync folder rm docs` stops syncing a folder on this node. The files stay on disk. If the server can't be reached, the node stops syncing anyway and tells the server later.

At this point the folder syncs between nodes but isn't archived yet.

### 4. Keep a folder

Keeping a folder archives it in Perkeep, and in the bucket. Run this once, on any node that has the folder:

```sh
keepsync keep docs --grace 48h --max-defer 7d --expire 30d
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--grace` | `48h` | A file must be unmodified this long before it is kept. Edits and deletes inside this window never reach the archive. |
| `--max-defer` | `7d` | Files that never settle are kept anyway after this long, and again each time they hit it. |
| `--expire` | never | Once a file is unmodified this long and durable in the bucket, it is removed from sync on every node. It stays in the archive. Leave it out (or pass `0`) to never expire anything. |

Durations take Go syntax plus a `d` suffix. Grace must be shorter than max-defer, and max-defer shorter than expire.

`keep` creates the folder's archive root and writes a `.keepsync` file into the folder, which syncs to every node. Up to two connected nodes are picked as the folder's keepers. They upload files past grace every 5 minutes. A deleted file is never deleted from the archive; it is marked as deleted (or expired, if expiry removed it), so a plain restore can skip it.

Each kept version records Syncthing's version of the file, so the archive knows which edit came after which, whatever the mtimes say. A file copied in with an old mtime (`cp -p`, an unzip) is still kept as the newest version, and a node that was offline and is behind never rolls the archive back.

Expiry only removes a file after the server confirms the file, and everything needed to find it by name, is durable in the bucket. If the server can't answer, expiry waits and the folder simply grows.

To leave files out of the archive, list them in `.stignore` (which also stops them syncing) or in `.keepignore` (which only stops keeping them). Syncthing's temp files, `.stfolder`, `.stversions` and `.keepsync` are always left out.

See where each file stands:

```sh
keepsync keep status docs      # counts: in grace, kept, lagging, expirable
keepsync keep status docs -v   # every file, with when it will be kept or expire
```

#### Unkeep

```sh
keepsync unkeep docs
```

This stops keeping the folder on every node: no more uploads and no more expiry. The archive and `.keepsync` stay, so `restore` still works, and running `keep docs` again continues the same archive.

### 5. Restore files

`restore` reads kept files back from the server, including ones that have expired from sync:

```sh
keepsync restore docs                         # the whole folder
keepsync restore docs reports/2026            # one file or directory
keepsync restore docs --to /tmp/docs-copy     # somewhere other than the folder
keepsync restore docs reports/q3.txt --force  # overwrite a local file that differs
```

Files keep their relative paths and permissions. A local file with the same size and mtime as the kept version is left alone, and one that differs is skipped unless you pass `--force`.

Files that were deleted in sync on purpose (including old names of renamed files and conflict copies a hook resolved) are skipped and counted, so a full restore brings back what expired, not what you deleted. To get those back too, add `--deleted`:

```sh
keepsync restore docs --deleted
```

To restore the archive as it was at an earlier time, use `--at`:

```sh
keepsync restore docs --at 3d --to /tmp/docs-3-days-ago
keepsync restore docs --at "2026-11-01 09:00" --to /tmp/docs-nov-1
keepsync restore docs --at 2026-11-01T09:00:00Z
```

`--at` takes an RFC 3339 time, `YYYY-MM-DD[ HH:MM]` in local time, or an age such as `3d` or `12h`. It means when the server kept a version, not the file's mtime, so a file kept two days after its last edit shows up at the time it was kept.

If a restored file is already past the folder's expire window, `restore` says so, because the next keeper pass removes it from sync again. Use `--to` to restore outside the folder in that case.

### 6. Browse the archive with Perkeep tools

Every node serves a read-only Perkeep gateway on `127.0.0.1:3179` that tunnels to the server, so stock Perkeep tools (`pk-get`, `pk-mount`, the web UI) work against the archive with no extra ports.

Point Perkeep's tools at the gateway:

```sh
keepsync gateway config
```

This writes a server named `keepsync`, with the gateway's local token, into Perkeep's own `client-config.json` (or the file given with `--path`). If another server is already your default there, pass `-server=keepsync` to Perkeep tools.

By default a node can only fetch blobs by ref. To allow search, the web UI, `pk-get` and `pk-mount`, grant it `read` on the server:

```sh
keepsync server grant laptop read    # and `blobs` to take it back
```

Kept folders show up as ordinary directories, for example under `pk-mount`'s `roots/`, with older states under `at/`. No scope can sign or delete, so the gateway can't change the archive.

To move or turn off the gateway, set `listen` under `[gateway]` in the node's `node.toml` (an empty string disables it) and restart the node.

### 7. Resolve conflicts with a hook

When two nodes edit the same file, Syncthing keeps one version and saves the other as a `.sync-conflict-…` copy next to it. keepsync keeps conflict copies like any other file, so a losing version is never lost.

You can have a script resolve conflicts, whether a merge tool or an LLM. Set it in the node's `node.toml` (`<base>/node/node.toml`) and restart the node:

```toml
conflict_hook = "/home/me/bin/merge-conflict"
conflict_hook_timeout = "5m"

[[folder]]
name = "docs"
# ...
conflict_hook = "/home/me/bin/merge-docs"   # overrides the default for this folder
```

Only one node runs the hook per folder: the first keeper on a kept folder, or the first member on an unkept one. If several nodes make identical copies of the same conflict, the hook runs once and the duplicates are removed. Set the hook on every node so it runs wherever that role lands. Before it runs on a kept folder, both versions are kept right away, so anything the hook does can be undone with `restore`.

The script runs in the folder root with:

| Variable | Value |
| --- | --- |
| `KEEPSYNC_FOLDER` | Folder name |
| `KEEPSYNC_ORIGINAL` | Absolute path of the current file |
| `KEEPSYNC_CONFLICT` | Absolute path of the conflict copy |
| `KEEPSYNC_CONFLICT_NODE` | Name of the node whose change lost, else its short device ID |
| `KEEPSYNC_BASE` | A temp file with the common ancestor of both versions, for a three-way merge, or empty if there is none |
| `KEEPSYNC_BASE_EXACT` | `1` if the base is known to be the common ancestor; `0` if it is only the last kept version, a guess |

To resolve, write the result to `$KEEPSYNC_ORIGINAL`, delete `$KEEPSYNC_CONFLICT` and exit 0. A non-zero exit, a timeout, or a copy left in place leaves the conflict for you.

A minimal hook that uses `git merge-file`:

```sh
#!/bin/sh
set -e
[ "$KEEPSYNC_BASE_EXACT" = 1 ] || exit 1   # no reliable base: leave it for a human
git merge-file "$KEEPSYNC_ORIGINAL" "$KEEPSYNC_BASE" "$KEEPSYNC_CONFLICT"
rm "$KEEPSYNC_CONFLICT"
```

`git merge-file` exits non-zero when the merge has conflicts, so the copy stays for you to look at.

```sh
keepsync conflicts                                    # conflict copies and hook state
keepsync conflicts resolve ~/Documents/notes.sync-conflict-20261115-101500-ABCDEFG.md
                                                      # run the hook now, on this node
```

### Recovery kit

The bucket holds the archive, and the recovery kit holds everything else needed to rebuild the server: the Perkeep identity, the server's tailcat key (so it comes back at the same address), the bucket credentials, the ZeroFS password, the config, the registry, and the `keepsync` and `zerofs` binaries. It is an age-encrypted zip.

`server init` writes the first kit. Regenerate it after changing the server's config, and keep the latest one:

```sh
keepsync server kit --out /safe/place/keepsync-recovery.zip.age
```

#### Recover a lost server

On a new machine, with the kit and its passphrase:

```sh
keepsync recover keepsync-recovery.zip.age
keepsync server run
```

`recover` starts ZeroFS on the bucket and rebuilds the Perkeep index from it, then installs the key, config and registry. A store-mode server also gets every archived blob copied back to local disk; a cache-mode one reads them from the bucket as needed. `--mode store|cache` recovers into a different mode from the one the kit was made with. The passphrase comes from `--passphrase-file`, `$KEEPSYNC_KIT_PASSPHRASE` or a prompt. Nodes reconnect on their own, because the address is the same.

ZeroFS allows only one writer, so **never recover while the old server could still be running against the bucket.**

#### Drill a recovery

Check that recovery works without touching the live server:

```sh
# on the live server
keepsync server baseline baseline.json

# copy the bucket (on R2 or S3, rclone copies server-side with no egress)
rclone copy r2:my-bucket r2:my-bucket-drill

# anywhere, in its own process
keepsync recover keepsync-recovery.zip.age --drill --bucket s3://my-bucket-drill/keepsync --baseline baseline.json
```

The drill restores the copy without taking over the server's identity, then checks that every blob and kept file reads back with the right hash, that the index finds every kept file, and where the registry would come from. It also reports restore time and throughput.

### Mount modes

The server always talks to ZeroFS over 9P. By default it is the 9P client itself and mounts nothing, so it needs no FUSE or root. Pick another way with `server init --mount`:

| Mode | How | Needs |
| --- | --- | --- |
| `9p` (default) | keepsync talks 9P to ZeroFS directly; no mount | Nothing extra |
| `fuse` | ZeroFS's own 9P client through FUSE, as your user | Linux, `/dev/fuse`, `fusermount3` |
| `v9fs` | The Linux kernel's 9P client | Linux, root, the `9p` and `9pnet_fd` modules |
| `external` | Nothing. You mount the 9P export at `--mount-dir` yourself | Any platform |

With `9p`, blobs are stored one file per blob, which writes far faster than the `diskpacked` packs the mount modes use for a new bucket (see [Testing](docs/spec.md#testing)). A bucket keeps the layout it was created with, and one written as `diskpacked` needs a mount mode. Servers set up before `9p` keep `fuse`.

Stick with `9p` unless you want to look at the server's files with ordinary tools while it runs, or have a `diskpacked` bucket. To switch later, set `mount` under `[zerofs]` in `server.toml` and restart the server. If ZeroFS exits or stops answering, the server restarts and reconnects it on its own, retrying for 5 minutes; `server status` shows `zerofs: RECONNECTING` meanwhile. If it stays down past that, `server status` and every node's status show `zerofs: DOWN`, and you have to restart `keepsync server run`.

### Command reference

Run `keepsync help` for the full list. The [spec](docs/spec.md#cli-reference) has a table of every command and flag.

## License

MIT. ZeroFS, which keepsync runs as a separate program, is AGPL-3.0. See [Licensing](docs/spec.md#licensing).
