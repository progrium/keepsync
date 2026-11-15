# keepsync spec

Status: describes the code on `dev` as of 2026-11-15. Where the code differs from the original design, this document follows the code and says so. Work that is not built yet is listed under "Not built yet".

## Overview

keepsync is one Go binary. It keeps a set of folders synced across machines with embedded Syncthing, and archives them in Perkeep. Perkeep's blobs are replicated through ZeroFS to any S3-compatible bucket (R2, S3, MinIO and so on). Nodes and the server reach each other over tailcat, so nothing needs a public IP, open ports or DNS.

**Goals**

- A synced folder can be a recent subset (say the last 30 days) of a permanent archive, and expiry never loses a file.
- Everything needed to rebuild the server from the bucket fits in one encrypted recovery kit.
- Joining a node is one command plus approval on the server.
- Prove out embedding Syncthing as a library.

**Non-goals**

- Replacing Syncthing's sync engine or conflict detection.
- Physical deletion from the archive as a normal operation.

## Architecture

One binary plays two roles. Every host runs `keepsync node run`. One host runs `keepsync server run`, which holds Perkeep, ZeroFS and the registry. Nodes never touch ZeroFS or the bucket, and they reach the server only through tailcat. One machine can run both roles, because each role has its own directory.

```
 +---------------------------+                         +---------------------------+
 | Node A                    |  Syncthing over tailcat | Node B                    |
 |  embedded Syncthing       |<----------------------->|  embedded Syncthing       |
 |  keeper: grace/keep/expire|     direct or DERP      |  keeper: grace/keep/expire|
 |  gateway 127.0.0.1:3179   |                         |  gateway 127.0.0.1:3179   |
 +-------------+-------------+                         +-------------+-------------+
               |   register, upload blobs, request claims (tailcat)  |
               v                                                     v
        +--------------------------------------------------------------------+
        | keepsync server                                                    |
        |   +-------------+      +-----------------+      +----------------+ |
        |   | registry :1 | ---> | perkeepd :3179  | ---> | ZeroFS         | |
        |   | members     |      | signs claims    |      | diskpacked, 9P | |
        |   +-------------+      +-----------------+      +-------+--------+ |
        +-------------------------------------------------------- | --------+
                                                                  | flush
                                                                  v
                                                          +----------------+
                                                          |  S3 bucket     |
                                                          +----------------+
```

Syncthing traffic runs node to node. The server is only on the path for membership, keeping and recovery, so sync carries on while it is down.

**Code layout.** `cmd/keepsync` is the CLI. The packages under `internal/` each implement one or more interfaces from `internal/api`, and depend on each other only through those interfaces. `internal/api/fake` has test doubles for all of them.

| Package | Does |
| --- | --- |
| `api`, `api/fake` | Shared types and interfaces, and fakes for tests |
| `tcnet` | `api.Transport` on tailcat |
| `registry` | Registry server and client (members, folders, keepers, claims) |
| `pkserver` | Embedded perkeepd, the signer, durability watermark and node scopes |
| `zerofs` | ZeroFS pin, download and supervisor |
| `syncer` | Embedded Syncthing |
| `keeper` | Keep loop, expiry and `keep status` |
| `hook` | Conflict hook runner |
| `gateway` | Local read-only Perkeep gateway on a node |
| `kit` | Recovery kit, recover and drill |
| `config`, `ctl` | On-disk config and the control socket |
| `server`, `node` | Wiring for each role |

## Directories and config

The base directory is `--dir`, else `$KEEPSYNC_DIR`, else `$XDG_DATA_HOME/keepsync`, else `~/.local/share/keepsync`. The server lives in `<base>/server` and the node in `<base>/node`. Config files are TOML, owner-only, because `server.toml` holds the bucket credentials and the ZeroFS password.

**Server directory**

| Path | Holds |
| --- | --- |
| `server.toml` | `[network]` (DERP map, region), `[storage]` (bucket URL, endpoint, region, keys, `allow_http`), `[zerofs]` (binary, password, mount mode, mount dir, cache sizes), `flush_interval` (default 1m) |
| `tailcat.key.json` | The server's tailcat identity |
| `perkeep/` | Perkeep identity, local blobs (`packRelated`), `index.leveldb` |
| `zerofs/` | ZeroFS config, cache, sockets, default mount point `zerofs/mnt`, and keepsync's own copy of the binary |
| `registry.json` | Registry state |
| `ctl.sock` | Control socket |

**Node directory**

| Path | Holds |
| --- | --- |
| `node.toml` | `name`, `server` (tc address), `[network]`, `keep_interval` (default 5m), `[gateway] listen` (default `127.0.0.1:3179`, empty disables), `conflict_hook` and `conflict_hook_timeout` (default 5m), and `[[folder]]` entries (name, path, Syncthing folder ID, optional per-folder `conflict_hook`) |
| `tailcat.key.json` | The node's tailcat identity |
| `syncthing/` | Device certificate, `config.xml`, the `index-v2` SQLite index |
| `membership.json` | The last membership seen, so the node syncs with known peers when it starts while the server is unreachable |
| `gateway-token` | The gateway's local token |
| `ctl.sock` | Control socket |

**Control socket.** `init`, `run` and `recover` work standalone. Every other command talks to the running process over `ctl.sock`: JSON over HTTP on a Unix socket, one POST to `/<method>` per call.

## Networking over tailcat

Every keepsync process runs one tailcat server and dispatches incoming connections by virtual port. Nothing listens on a real port except the node's gateway on loopback and Syncthing's loopback bridge.

| Virtual port | Service | Where |
| --- | --- | --- |
| 1 | Registry API | Server |
| 3179 | perkeepd HTTP | Server |
| 22000 | Syncthing BEP | Every node |

**Identity and allow lists.** A process is identified by its server key (`nodekey:…`). Each transport keeps its own allow list, and only allowed keys can connect. The server allows approved nodes, and a node allows its current peers and the server. Handlers see the authenticated key of the remote end.

**Dial keys.** A process does not dial with its server key. Several tailcat clients sharing one key fight over DERP routing (in a three-peer test, concurrent dials took up to 7 s with a shared key against 1 s with distinct keys). So the dial key for each (dialer, target) pair is derived from an X25519 exchange between their server keys. The target can derive the same public key, so allowing an identity admits exactly the key that identity dials it with, and nobody else can compute it.

**Relays.** Traffic goes direct when NAT traversal works and falls back to DERP otherwise. tailcat's public DERP relays are rate-limited, so for real sync volumes the user may run their own `derper` and pass `--derp-map` (URL or local JSON file) and `--derp-region` to `server init` and `node init`.

**Bootstrap.** A node needs only the server's `tc…` address. `node init --server` also accepts a DNS name whose TXT record holds the address, bare or as `keepsync=tc…`. It is resolved once at init and the address is saved. Publishing the address is safe because the server allow-lists node keys.

**Join flow**

1. `keepsync server init --bucket <url>` creates the server and prints its address.
2. `keepsync node init --server tc…` writes the node's config, tailcat key and Syncthing device certificate. It prints the device ID, the node key and the `server add` command to run, then exits. It needs no network except for a DNS lookup.
3. `keepsync server add <nodekey> [--name]` approves the node. The approved nodes are the allow list.
4. `keepsync node run` starts tailcat and Syncthing, then registers `{name, key, address, device ID}` with the registry and watches the membership over a held-open connection.
5. For each peer, the node allows the peer's key and adds it as a Syncthing device.

## Registry

The registry runs on the server on port 1. It holds the approved nodes, folders, keep policies, keeper assignments and node scopes.

**Wire protocol.** Each side writes a stream of JSON values. Requests carry an ID and responses can arrive in any order, so one connection carries many concurrent calls. A `watch` request turns its connection into a stream: the current membership, then one response per change. The caller's identity is always the tailcat key of the connection, never a request field.

**Heartbeat.** The server pings every connection every 10 s, and the client answers with a pong. Either side drops a connection that has been silent for 30 s, because a tailcat connection to a vanished peer never resets. So a node stops counting as online within 30 s of going silent.

**Folders.** `folder add` creates a folder with a random Syncthing-style ID (`abcde-fghij`) and makes the caller its first member. `folder join` adds the caller to an existing folder. It refuses a local directory that already has files (Syncthing's and keepsync's own files aside), because those files would sync into every member's copy. `--merge` joins anyway and syncs both ways. Rejoining a folder at the path it is already attached at is allowed. `folder rm` calls LeaveFolder, which removes the node from the folder's members and keepers. Files on disk stay.

**Keepers.** Each kept folder gets up to two keepers, chosen from its connected members, with send-receive nodes ahead of receive-only ones. A keeper disconnected for 2 minutes is replaced if another connected member can take over. `Keepers[0]` stays first while it is healthy; it is the node meant to run the conflict hook.

**Claims and durability.** The registry forwards `ClaimPath`, `Paths`, `PathsAt` and `Durable` to the server's signer and durability watermark. It only allows them on permanodes of folders the calling node is a member of.

**Persistence.** The state is saved to `registry.json` before each change is published. Each save is also mirrored to `.keepsync/registry.json` on the ZeroFS mount, so the next flush carries it to the bucket for recovery. (The original design stored registry state in Perkeep. This JSON file plus bucket mirror replaces that.)

## Keep server

The server runs perkeepd, configured like the proof of concept: `packRelated` local storage, a `diskpacked` storage named `zerofs` on the ZeroFS mount, and `replicateTo` it, which is an asynchronous sync handler. It is the only machine that holds the Perkeep identity key and the only one that runs ZeroFS.

**Startup and shutdown.** `server run` starts ZeroFS, waits for its sockets, mounts it, then starts perkeepd, the registry and the tailcat listener. On stop it drains the replication queue, flushes, closes perkeepd, then stops ZeroFS, which flushes again. A clean stop means everything is in the bucket.

**ZeroFS.** keepsync pins ZeroFS v2.3.7 and checks SHA-256 for both the release tarball and the platform binary. Builds exist for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and freebsd/amd64. The binary is looked up as `--zerofs`, then on PATH, then in `zerofs/` in the server directory. If none is found, `server init` downloads the pin, unless `--no-download` is given. `run` never downloads. A binary that does not match the pin produces a warning, never an upgrade.

The supervisor writes `zerofs.toml` with 9P and RPC on private Unix sockets only, no NFS and telemetry off. Secrets go in the environment, never in the file. ZeroFS is always reached over 9P, because it reports NFS writes as stable while they are still buffered. The mount mode is set at init with `--mount`:

| Mode | How | Needs |
| --- | --- | --- |
| `fuse` (default) | ZeroFS's own 9P client (`zerofs mount`) through FUSE, as the current user. Reconnects if zerofs restarts. | Linux, `/dev/fuse`, `fusermount3`. No root. |
| `v9fs` | The Linux kernel 9P client (`mount -t 9p`) | Linux, root, the `9p` and `9pnet_fd` modules |
| `external` | Mounts nothing. The operator has mounted the 9P export at `--mount-dir`; keepsync checks it is a mount point, so it never writes to a plain local directory. | Any platform, including macOS and FreeBSD |

The original design said mount mode needs root. The FUSE mode removed that.

**ZeroFS password.** `server init` generates it from 32 random bytes and stores it in `server.toml` and the recovery kit. `--zerofs-password-file` adopts an existing bucket's password instead.

**Durability watermark.** Every `flush_interval` (default 1m) the server flushes ZeroFS. A blob is durable once it was seen in the diskpacked replica before a flush started, and that flush succeeded. Only blobs someone asked about are tracked: `Durable` queues the unseen ones, and each flush checks the queue against the replica first. A ref counts as durable only if its whole closure is: a file's parts, recursively; a claim's permanodes; and, for a claim or permanode, the server's public key, which a reindex needs to verify it. A ref the server doesn't have is not durable. `server status` shows the last flush, or that flushing is failing.

**Signing.** Nodes upload file blobs, which are content-addressed and unsigned. Then they ask the registry to keep a fileref at a path in a kept folder, and the server signs the claims (see "Kept tree layout"). Signing is serialised. The server writes nothing if the path already holds that ref (`ClaimUnchanged`), and rejects a ref whose mtime is not newer than the current one (`ClaimRejectedOlder`), so a lagging node cannot roll a file back. Claim dates strictly increase, so a later claim always wins, even within one clock tick.

**Node scopes.** Every tunnelled connection to perkeepd is checked against the node's scope.

| Scope | Allows | Granted by |
| --- | --- | --- |
| `blobs` (default) | Upload, stat and fetch blobs, plus discovery. Not sign, enumerate, search or remove. | Every node |
| `read` | `blobs` plus search, the web UI, `pk-get` and `pk-mount` | `keepsync server grant <name> read` |

The scope is stored on the node's registry record. No scope can sign or delete.

## Keeping and expiry

A kept folder maps to one root permanode whose tree only ever grows. Expiry removes files from the sync folder only after they are durable in the bucket. Each timer is measured from a file's mtime.

| Setting | Default | Meaning |
| --- | --- | --- |
| `--grace` | 48h | A file must be unmodified this long before it is kept. Edits and deletes inside this window never reach the archive. |
| `--max-defer` | 7d | Cap for files that never settle. Past it, the file is kept anyway, and again each time it hits the cap. |
| `--expire` | never | The file is removed from sync on every node once unmodified this long and durable. `0` or omitted means nothing expires. (The original design defaulted to 30d.) |

Durations accept Go syntax plus a `d` suffix. A policy is rejected unless grace < max-defer, and max-defer < expire when expire is set.

**Mapping.** `keepsync keep <folder>` asks the server to create the root permanode and writes its ref to a `.keepsync` file inside the folder. Syncthing carries that file to every node, so all nodes agree on the permanode. The registry also records it as the folder's `Archive`.

**Unkeep.** `keepsync unkeep <folder>` clears the folder's policy and keepers. This node stops its keeper at once, and every other node stops when the membership drops the policy: no more puts and no expiry. The archive, `Folder.Archive` and `.keepsync` all stay, so `restore` still reads the archive, and `keep` again continues the same tree. `keep status` on an unkept folder says it is not kept and names the archive.

**Kept tree layout.** The tree uses Perkeep's own conventions, so the web UI, search and `pk-mount` show kept folders as ordinary directories:

- The root permanode has `title` and `camliRoot` set to the folder name, so it appears under `pk-mount`'s `roots/`.
- Every directory below the root has its own permanode. Its parent names it with `camliPath:<name> → dir permanode`.
- Every file has its own permanode, with `camliContent → fileref` and `keepsyncMtime`. Its directory names it with `camliPath:<name> → file permanode`.

Each change is one signed multi claim:

- A changed file gets one claim on its permanode, which sets `camliContent` and `keepsyncMtime` together. Nothing above it changes.
- A new file gets a new permanode, and one claim sets its content and mtime and names it in its directory. A missing directory gets a permanode too, named in the same claim.

The mtime rule is checked per file permanode. A path is recoverable by name only when the fileref, the file's permanode and every claim and permanode from the root down to it are durable. The registry returns that set with each path entry (`PathEntry.Claims`), and expiry checks all of it. `PathsAt`, `restore --at` and `pk-mount`'s `at/` all fold the same claims as of a time.

**The keeper loop.** Each keeper node makes a pass right away and then every `keep_interval` (default 5m):

1. Walk the folder, skipping excluded files.
2. Put each file past grace, or past max-defer, through Perkeep. A put cache makes unchanged files free, and blobs the server already has are not re-sent.
3. Ask the server to keep the fileref at the file's path.
4. A missing file is never translated into a delete.

Running two keepers is harmless, because the server deduplicates claims. max-defer is tracked in memory, so after a restart a never-settling file's cap is measured from the first version seen after the restart.

**Expiry.** A pass then removes a file from sync once it is unmodified for the expire window, its current fileref and mtime are what the tree holds, and the server reports the whole recovery set durable. Before removing, the keeper re-checks the file's ref, so a same-mtime file with different content is not removed. If the server cannot answer, expiry pauses and sync folders grow past the window. That is the intended failure mode.

**Excluded from keeping:** Syncthing temp files (`.syncthing.*.tmp`), `.stfolder`, `.stversions`, `.keepsync`, the ignore files themselves, anything matched by `.stignore`, and an optional `.keepignore`. The built-in matcher covers common `.stignore` syntax. Conflict copies are kept like any other file.

**`keep status <folder>`** lists files as in grace (with when they will be kept), due, kept, lagging (the archive has a newer version), or expirable. It reports when durability is unknown.

**Restore.** `keepsync restore <folder> [<path>] [--at <time>] [--to <dir>] [--force]` reads kept files back from the server over tailcat, including expired ones:

- It reads the current tree, or the tree as of `--at`. `--at` is when the server kept a version, not a file mtime. It takes RFC 3339, `YYYY-MM-DD[ HH:MM]` in local time, or an age such as `3d` or `12h`.
- Files go back into the folder, or into `--to`, keeping their relative paths.
- A local file with the kept size and mtime is left alone.
- A local file that differs is skipped unless `--force` is given.
- If a restored file is already past the folder's expire window, the result says so, because the next pass expires it again.

## Syncthing

Syncthing v2.1.7 runs in-process through `lib/syncthing`. It is vendored and pruned in `third_party/syncthing`, wired in with a `go.mod` replace. Two reasons: the published module does not build as a library without generated GUI assets, and v2 has no importable module version. The only patch is a stub `gui.files.go`. `internal/syncer/README.md` lists the Syncthing internals touched and how stable each one is.

**Config.** Peers and folders come from the registry, never from introducers or a config file at startup. Global and local discovery, relays, NAT, usage and crash reporting, upgrades and the GUI are off.

**Transport bridge.** Syncthing listens only on `127.0.0.1:<free port>`.
- Inbound: the port 22000 handler checks the caller is a current peer, dials Syncthing's loopback listener and splices the two connections.
- Outbound: each peer gets its own loopback listener, which is that device's only address in Syncthing's config, and each connection to it is dialed to the peer over tailcat.

BEP's TLS runs end to end, so Syncthing still checks device IDs.

**Restarts.** On start, the syncer restores the previous run's folders and peers from `config.xml`. Without this, Syncthing would drop the index of any folder missing at startup, every file would be rescanned as a new version, and a delete on another node (such as expiry) would come back as a conflict copy.

**Folder types.** Every node defaults to send-receive. A folder's `ReceiveOnly` list marks nodes as receive-only.

**Conflicts.** Syncthing writes `.sync-conflict-<date>-<device>` copies. Conflict copies are kept in Perkeep under their own name, next to the original, and expire like any other file, so a losing version is never lost and unresolved conflicts do not pile up in sync.

**Conflict hook.** `conflict_hook = "/path/to/script"` in node.toml applies to every folder, and a `conflict_hook` on a `[[folder]]` overrides it. The hook lets an LLM or any other tool resolve conflicts.

- **Who runs it.** On a kept folder, only `Keepers[0]`. On an unkept folder, `Members[0]`. Other nodes only report.
- **When.** On each conflict event from Syncthing, and on a scan of the folder when the node starts or attaches it. Each copy is debounced for 5 s, and hooks run one at a time per folder.
- **Before it runs**, on a kept folder, the node reads the original's current kept version as the base. Then it keeps the original and the copy immediately, skipping grace, so anything the hook does is recoverable. If that fails, the hook does not run.
- **Timeout.** `conflict_hook_timeout` (default 5m). On timeout the hook's whole process group is killed.

The script runs in the folder root and its output is logged. It gets:

| Variable | Value |
| --- | --- |
| `KEEPSYNC_FOLDER` | Folder name |
| `KEEPSYNC_ORIGINAL` | Absolute path of the current file |
| `KEEPSYNC_CONFLICT` | Absolute path of the conflict copy |
| `KEEPSYNC_CONFLICT_NODE` | Name of the node whose losing change the copy holds, else its short device ID. This is the copy's `ModifiedBy` in Syncthing's index. The ID in the copy's name is the winner's, so it isn't used. |
| `KEEPSYNC_BASE` | The base in a temp file, for a three-way merge, or empty if there is none (always empty on an unkept folder). The file is deleted afterwards. |

**Contract.** To resolve, the script writes the result to the original path, deletes the conflict copy and exits 0. Syncthing syncs that like any other edit. A non-zero exit, a timeout, or a copy still in place leaves the conflict for a human. A failed copy is not retried until its size or mtime changes.

**`keepsync conflicts`** lists the conflict copies in attached folders, with the original, the node, and the hook state: none, pending, running, failed (with the reason), or not this node's job. **`keepsync conflicts resolve <path>`** runs the hook now on this node, whatever its role, keeping both versions first on a kept folder.

## Perkeep gateway

Each node serves the server's Perkeep on `127.0.0.1:3179`, so stock Perkeep tools work against the archive with no extra ports or DNS. The gateway proxies HTTP to the server's perkeepd over tailcat on port 3179. A gateway that cannot start is logged, and the node runs without it.

**Local auth.** The gateway requires the per-node token in `gateway-token` as the password of HTTP basic auth, the way Perkeep's userpass auth sends it. `keepsync gateway config [--path]` writes a Perkeep client config that points at the gateway with that token. By default it writes Perkeep's own `client-config.json`.

What a node can reach through the gateway is its scope (see "Node scopes"). With `blobs`, the tools can fetch blobs by ref and nothing more. `read` opens search, the web UI, `pk-get` and `pk-mount`.

## Recovery kit

`server init` writes an encrypted recovery kit holding everything needed to rebuild the server that does not live in the bucket. `server kit [--out]` regenerates it.

**Format.** `keepsync-recovery.zip.age`: a zip encrypted with age under a scrypt passphrase (work factor 2^18). The passphrase is generated, shown once and stored nowhere. Store the kit and the passphrase separately and offline, never in the same bucket.

**Contents**

| Entry | Holds |
| --- | --- |
| `kit.json` | Format, creation time, platform, server address, Perkeep key ID, bucket location and credentials, ZeroFS password, pinned ZeroFS version and whether the bundled binary matches it |
| `bin/keepsync`, `bin/zerofs` | The binaries for the current platform |
| `perkeep/identity-secring.gpg`, `perkeep/perkeepd.json` | The Perkeep identity and config |
| `tailcat/server.key` | The server's tailcat key, so a recovered server comes back at the same address |
| `server/config`, `server/registry` | `server.toml`, and the registry as of the kit's creation |
| `RECOVER.md`, `SHA256SUMS` | Steps and checksums |
| `licenses/ZEROFS-LICENSE`, `licenses/ZEROFS-SOURCE` | ZeroFS's license and a link to the pinned source |

**`keepsync recover <kit>`**

1. Mounts ZeroFS on the kit's bucket.
2. Starts perkeepd on fresh local storage and a fresh index.
3. Copies every blob of the bucket's diskpacked replica into it, which reindexes them.
4. Installs the tailcat key, config and registry. It prefers the registry mirrored in the bucket, and falls back to the kit's copy.

Then `server run` brings the server back at the same address and nodes reconnect on their own. The passphrase comes from `--passphrase-file`, `$KEEPSYNC_KIT_PASSPHRASE` or a prompt. `--access-key`/`--secret-key` supply write credentials when the kit holds a read-only token, and `--mount` picks the mount mode. ZeroFS is single-writer, so never recover while the old server may still be running against the bucket.

**Drill.** Run `keepsync server baseline <out.json>` on the live server first. It flushes and snapshots every blob, kept tree and file hash. Then copy the bucket (on R2 or S3, `rclone copy` copies server-side with no egress cost) and run `keepsync recover <kit> --drill --bucket <copy> --baseline <out.json>`. The drill runs the same restore without installing the identity, and checks:

1. Blobs: every restored blob reads back and matches its hash, and every baseline blob was restored.
2. Files: every kept file reads back with the baseline's SHA-256.
3. Index: the index finds every permanode, and a search on `camliContent` finds the permanode of sample kept files.
4. Registry: where recovery would get the registry from. A registry only in the kit, or nowhere, is a warning.
5. Timing: restore time, ZeroFS start time, and copy-and-reindex throughput.

perkeepd runs once per process, so a live server, a recovered server and a drill each need their own process.

## CLI reference

| Command | Role | What it does |
| --- | --- | --- |
| `server init --bucket <url> [flags]` | Server | Creates the tailcat key, Perkeep identity and ZeroFS config, finds or downloads ZeroFS, writes the kit, prints the address. Flags: `--endpoint`, `--region`, `--access-key`, `--secret-key` (default `$AWS_*`), `--allow-http`, `--zerofs`, `--zerofs-password-file`, `--mount`, `--mount-dir`, `--no-download`, `--kit`, `--derp-map`, `--derp-region`. |
| `server run` | Server | Supervises ZeroFS, perkeepd, the registry and tailcat. Drains and flushes on stop. |
| `server add <nodekey> [--name]` | Server | Approves a node. The name defaults to the one the node registers with. |
| `server remove <name>` | Server | Removes a node. |
| `server nodes` | Server | Lists members. |
| `server status` | Server | Address, key, last flush or flush failure, folders. |
| `server kit [--out]` | Server | Regenerates the recovery kit. |
| `server baseline <out.json>` | Server | Flushes and snapshots the server for a drill. |
| `server grant <name> <blobs\|read>` | Server | Sets a node's Perkeep scope. |
| `node init --server <tc… or DNS name> [--name]` | Node | Writes config and keys, prints the node key, exits. The name defaults to the hostname. |
| `node run` | Node | Runs tailcat, Syncthing, the keepers and the gateway. |
| `status` | Node | Server connection (or how long it has been unreachable), peers, folders. |
| `folder add <name> <path>` | Node | Creates a synced folder in the registry and attaches it here. |
| `folder join <name> <path> [--merge]` | Node | Attaches an existing folder here. A directory with files needs `--merge`. |
| `folder ls` / `folder rm <name>` | Node | Lists folders, or stops syncing one here. Files stay. |
| `keep <folder> [--grace] [--max-defer] [--expire]` | Node | Marks a folder kept. Creates the permanode and `.keepsync`. |
| `keep status <folder> [-v]` | Node | Files in grace, kept, lagging and expirable. |
| `unkeep <folder>` | Node | Stops keeping a folder on every node. The archive stays. |
| `restore <folder> [<path>] [--at] [--to] [--force]` | Node | Pulls kept files back, including expired ones, and from unkept folders. |
| `conflicts` / `conflicts resolve <path>` | Node | Lists conflict copies and hook state, or runs the hook now. |
| `gateway config [--path]` | Node | Writes a Perkeep client config for the local gateway. |
| `recover <kit> [--drill --bucket <copy> --baseline <file>]` | Any | Rebuilds the server from the bucket, or runs the drill. |

`purge` exists as a command but reports "not implemented yet".

## Failure modes

When the server is unreachable, sync keeps working, and only keeping, expiry and membership changes wait. No failure path can expire a file whose recovery set is not durable in the bucket.

| Failure | Behaviour |
| --- | --- |
| Server unreachable | Syncing continues with the cached peer list. The registry client reconnects with backoff. The keeper retries next interval; there is no queue to lose, because the walk is idempotent. Expiry pauses. `status` shows how long the server has been unreachable. |
| Node never approved | `status` shows the `server add` command to run. |
| Keeper node gone | After 2 minutes the server reassigns its keeper role. The second keeper keeps working meanwhile. |
| Lagging keeper reports an older file | The server rejects it. `keep status` shows the file as lagging. |
| Registry connection goes silent | Both sides drop it within 30 s, and the node reconnects and refetches the membership. |
| ZeroFS flush failing | Nothing new becomes durable, so expiry stops removing files. `server status` shows the failure. |
| Conflict hook fails or makes a bad merge | A failure leaves the copy for a human, shown in `conflicts`. Both versions were kept before the hook ran, so a bad merge can be undone with `restore`. |
| Server lost | `keepsync recover <kit>` from the bucket. The tailcat key is the same, so nodes reconnect on their own. |

## Edge cases

**A file recreated at an expired path.** The kept tree only grows, so the expired file's permanode still holds the path.

- **Normal case.** The new file is the path's next version. After grace it gets one new content claim on the same file permanode, and the old content stays in its history.
  - `PathsAt`, `restore --at` and `pk-mount`'s `at/` show the old file for times before the new one was kept, and the new file after that.
  - A plain `restore` brings back the new file. `restore --at <earlier> --to <dir>` gets the old one.
  - Restoring the old one into the folder skips it, because a different file is there. `--force` would overwrite the new file with the old one.
  - Grace and expiry are measured from the new file's mtime, so they start fresh.
- **New file with an older mtime than the expired version.** This happens when the file comes from `cp -p`, an unzip or `rsync -a`. The server rejects the claim as older, because that rule stops a lagging node rolling a file back.
  - The keeper reports the file as lagging in `keep status`, and never keeps or expires it, so it just stays in sync.
  - Workaround: `touch` the file, so its mtime is newer.
  - A possible fix, not built: accept an older mtime when the path's current version has already expired from sync.
- **New file with exactly the expired version's mtime but different content.** The keeper decides "already kept" by mtime alone, so the new file is never put. Expiry checks the content ref, so it does not remove the file either, and it stays in sync unarchived. `touch` fixes this too.

## Testing

Every package has unit tests against the fakes in `internal/api/fake`.

`test/e2e` starts a server and three nodes in one process, with tailcat on an in-process derper. Its steps cover:
- join and sync
- keeping after grace
- expiry gated on durability
- a delete after a node restart propagating, not coming back as a conflict copy
- leaving a folder
- `recover --drill`
- keeper reassignment

With `KEEPSYNC_ZEROFS_BIN` and `KEEPSYNC_MINIO_BIN` set, it runs against a real ZeroFS and MinIO; without them those steps skip. All steps pass against ZeroFS 2.3.7 and MinIO.

Not yet measured: keep throughput, bucket write operations and restore time on a real 30 GB set.

## Not built yet

- **Deleting from keep**, as a `del-attribute` on `camliPath:<name>` in the parent directory's permanode, or a `delete` claim. Both are reversible. **`purge`**, the rare manual physical delete, since Perkeep has no garbage collector.
- **ZeroFS without a mount**: a Perkeep blobserver that writes diskpacked packs through a Go 9P client straight to ZeroFS.
- **A native tailcat transport inside Syncthing**, replacing the loopback bridge. It needs a patch to the vendored `lib/connections`.
- **Signed writes through the gateway** (a `write` scope). The server keeps the only identity. Web UI edits already sign through the server's signing handler, but CLI tools such as `pk put` sign with their own key, and Perkeep does not honour a second signer by default. Either CLI signing is proxied to the server, or the server trusts a second signer. One identity is preferred, so this is still an open question.
- **Persisting max-defer state** across keeper restarts.
- **Release packaging**: the third-party notices file described under Licensing.

## Licensing

keepsync itself is MIT. Every embedded dependency is compatible with that, and ZeroFS (AGPL) stays a separate, unmodified program.

| Component | License | How keepsync uses it |
| --- | --- | --- |
| Syncthing | MPL-2.0 | Embedded library, vendored in `third_party/syncthing` |
| Perkeep | Apache-2.0 | Embedded (perkeepd and client) |
| tailcat | BSD-3-Clause | Embedded library |
| ZeroFS | AGPL-3.0 or commercial | Separate binary, run as a subprocess, spoken to over 9P |

**Obligations**

- Release binaries ship a third-party notices file: Apache-2.0 NOTICE text, BSD copyright lines, and a pointer to the Syncthing source (MPL).
- Patched Syncthing files (the `gui.files.go` stub) stay MPL-2.0 and are published.
- The recovery kit carries ZeroFS's license text and a link to the exact source for the pinned version.
- ZeroFS is never linked or modified. A patched ZeroFS would fall under the AGPL network clause.

This is a reading of the licenses, not legal advice.
