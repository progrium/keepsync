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
        |   | members     |      | signs claims    |      | blobs over 9P  | |
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
| `server.toml` | `mode` (store or cache), `[network]` (DERP map, region), `[storage]` (bucket URL, endpoint, region, keys, `allow_http`), `[zerofs]` (binary, password, mount mode (`9p` for new servers), mount dir, cache sizes), `flush_interval` (default 1m) |
| `tailcat.key.json` | The server's tailcat identity |
| `perkeep/` | Perkeep identity, local blobs (`packRelated`, store mode only), `index.leveldb` |
| `zerofs/` | ZeroFS config, cache, sockets, default mount point `zerofs/mnt` (unused with `9p`), and keepsync's own copy of the binary |
| `mode` | The mode the directory was initialized with |
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
| `pending-leave.json` | Folders removed here with `folder rm` while the server was unreachable, still to be left on the server |
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

**One tunnel per pair.** Left alone, both nodes of a pair dial each other, so they have two tailcat tunnels, one each way, which hole-punch separately: one can go direct while the other stays on a rate-limited DERP relay, and Syncthing (which opens three connections per peer by default) spreads its connections over both. A sync from a laptop to a server once ran at KB/s that way while the other tunnel was direct. So only the node with the lower node key dials its peers' Syncthing; the other refuses Syncthing's own dials and waits to be dialed. If it has had no inbound connection from the peer for 30 s, for example because the dialer cannot reach it, it dials too.

**Nudging tunnels to direct.** tailcat has no control plane to hand out endpoints, so direct path discovery only runs when something pings. Every 15 s the transport sends a disco ping over each outbound tunnel with open connections that is not known to be direct. A tunnel that upgrades stops being pinged.

**Path in status.** `keepsync status` shows each connected peer as `connected (direct <ip:port>)` or `connected (relay <region>)`. With a tunnel each way (normally only during the dial fallback) both are shown, as `in:` (the peer dialed us) and `out:` (we dialed), each with its rate when busy or `idle`. For an inbound tunnel the path is read from tailcat's own status, which costs nothing. tailcat reports a direct address only while it trusts it, which lapses about 6.5 s after the last pong, so an idle inbound tunnel without a current direct address shows as `idle` rather than as relayed. For an outbound tunnel the path is the last disco ping's answer (2 s timeout), refreshed when it is older than 20 s; a pong is a live measurement, so it holds for an idle tunnel too.

**Traffic in status.** The transport also counts the bytes each connection reads and writes, per peer identity across both tunnels, and samples the counters every second. `status` shows each peer's receive and send rates over the last 10 seconds. The counts cover everything sent over tailcat to that peer, which between nodes is Syncthing. Syncthing has no per-folder throughput, so rates are per peer, not per folder.

**Sync progress.** `status` and `folder ls` add a SYNC column per attached folder, from Syncthing's own model: the folder state, and this node's completion (bytes and items still needed, less the bytes of partly pulled files already fetched). It reads `up to date`, `scanning`, `syncing 63% (1.2 GB, 340 files left)`, `behind ...` (needs files but is not pulling, for example because no connected peer has them), or `error: ...`. A connected peer that is behind is appended as `laptop at 80%`; that is Syncthing's view of the peer, from the index the peer sent and what it has downloaded since. `folder status <name>` shows the same per peer, with path and traffic, and `-v` lists the needed files in pull order (pulling, queued, waiting), 100 by default (`-n`).

**Bootstrap.** A node needs only the server's `tc…` address. `node init --server` also accepts a DNS name whose TXT record holds the address, bare or as `keepsync=tc…`. It is resolved once at init and the address is saved. Publishing the address is safe because the server allow-lists node keys.

**Join flow**

1. `keepsync server init --bucket <url>` creates the server and prints its address.
2. `keepsync node init --server tc…` writes the node's config, tailcat key and Syncthing device certificate. It prints the device ID, the node key and the `server add` command to run, then exits. It needs no network except for a DNS lookup.
3. `keepsync server add <nodekey> [--name]` approves the node. The approved nodes are the allow list.
4. `keepsync node run` starts tailcat and Syncthing, then registers `{name, key, address, device ID}` with the registry and watches the membership over a held-open connection.
5. For each peer, the node allows the peer's key and adds it as a Syncthing device.

## Registry

The registry runs on the server on port 1. It holds the approved nodes, folders, keep policies, keeper assignments and node scopes.

**Wire protocol.** Each side writes a stream of JSON values. Requests carry an ID and responses can arrive in any order, so one connection carries many concurrent calls. A `watch` request turns its connection into a stream: the current membership, then one response per change. A membership carries a version and an epoch, a random value the registry picks each time it starts. Nodes ignore a membership with a lower version only when its epoch is the same, so a restarted or recovered registry always wins, even if it restored an older state. The caller's identity is always the tailcat key of the connection, never a request field.

**Heartbeat.** The server pings every connection every 10 s, and the client answers with a pong. Either side drops a connection that has been silent for 30 s, because a tailcat connection to a vanished peer never resets. So a node stops counting as online within 30 s of going silent.

**Folders.** `folder add` creates a folder with a random Syncthing-style ID (`abcde-fghij`) and makes the caller its first member. `folder join` adds the caller to an existing folder. It refuses a local directory that already has files (`.stfolder`, `.stversions` and the ignore files aside), because those files would sync into every member's copy. `--merge` joins anyway and syncs both ways. Rejoining a folder at the path it is already attached at is allowed. Joining it at a different path while it is attached is refused: Syncthing would restart the folder at the new path on the old directory's index, read it as everything deleted, and stop it with "folder marker missing" (`folder rm` first, then join). The CLI resolves a relative path against its own working directory before sending it, and the node refuses relative paths, since its working directory is not the caller's. `folder rm` calls LeaveFolder, which removes the node from the folder's members and keepers. Files on disk stay. If the server cannot be told, the node stops syncing the folder anyway, saves the leave in `pending-leave.json`, and retries it each time the server answers, unless the folder is joined again first.

**Keepers.** Each kept folder gets up to two keepers, chosen from its connected members, with send-receive nodes ahead of receive-only ones. A keeper disconnected for 2 minutes is replaced if another connected member can take over. `Keepers[0]` stays first while it is healthy; it is the node meant to run the conflict hook.

**Claims and durability.** The registry forwards `ClaimPath`, `Paths`, `PathsAt` and `Durable` to the server's signer and durability watermark. It only allows them on permanodes of folders the calling node is a member of.

**Persistence.** The state is saved to `registry.json` before each change is published. Each save is also mirrored to `.keepsync/registry.json` on ZeroFS (through the supervisor, so over 9P or the mount), so the next flush carries it to the bucket for recovery. (The original design stored registry state in Perkeep. This JSON file plus bucket mirror replaces that.)

## Keep server

The server runs perkeepd. It is the only machine that holds the Perkeep identity key and the only one that runs ZeroFS. Where perkeepd keeps blobs depends on the server's mode, chosen at `server init --mode` and fixed after that:

| Mode | Blobs | Local disk | Reads |
| --- | --- | --- | --- |
| `store` (default) | `packRelated` local storage, replicated asynchronously (`replicateTo`) to the blob store on ZeroFS, a storage named `zerofs` | Every blob, the index, and a minimal ZeroFS cache (2 GB) | Local |
| `cache` | The blob store on ZeroFS is perkeepd's primary storage. There is no local blob store and no replication | The index and the ZeroFS cache, sized with `--cache-size` (default 10 GB, minimum 2 GB) | From the ZeroFS cache, else from the bucket |

In both modes the Perkeep index (`index.leveldb`), the identity and perkeepd's small queues and caches stay local, and the bucket holds the same blob layout, so a server recovers into either mode (see Recovery kit). The mode is recorded in the server directory's `mode` file, so a hand edit of `server.toml` cannot switch it under existing data.

Cache mode suits a server with little disk: the archive can be far larger than the machine. The cost is that reading old blobs (restores, the gateway, search on file contents) fetches them from the bucket, and nothing is readable while the bucket is unreachable. Writes are unaffected: ZeroFS buffers them in RAM and PUTs them to the bucket, and the durability watermark works the same way, so expiry still waits until blobs are flushed to the bucket.

**Blob store on ZeroFS.** It has one of two layouts, and the layout belongs to the bucket: the server finds it from what is in the filesystem, so a recovery, a mode change or a different mount mode keeps whatever wrote the bucket.

| Layout | On ZeroFS | Reached | Written by |
| --- | --- | --- | --- |
| `files` | One file per blob under `blobs/` (Perkeep's `files` storage, as `localdisk` lays it out) | Over 9P with no mount, or through a mount | New buckets with mount mode `9p` (the default) |
| `diskpacked` | Perkeep's `diskpacked` packs and index at the root (`pack-00000.blobs`, ...) | Only through a mount | New buckets with a mount, and servers from before `9p` |

Over 9P the store is `files.Storage` on keepsync's own 9P client (`zerofs.NineP`, built on `hugelgupf/p9`), registered with perkeepd as storage type `keepsync-9p`. A `diskpacked` bucket with mount mode `9p` is refused at start with a hint to mount it. A per-file fsync over 9P is a full ZeroFS flush (about 55 ms), so the 9P store does not fsync blobs or directories: a 9P write is in ZeroFS when it returns, and the periodic flush is the durability point, as the watermark already assumes. Through a mount, Perkeep fsyncs every blob, and that fsync is the same full flush, which is why `diskpacked` on FUSE writes at about 0.5 s per blob and leaves thousands of small objects in the bucket (see Testing).

**Startup and shutdown.** `server run` starts ZeroFS, waits for its sockets, connects to it over 9P or mounts it, then starts perkeepd, the registry and the tailcat listener. On stop it drains the replication queue (store mode; cache mode has none), flushes, closes perkeepd, then stops ZeroFS, which flushes again. A clean stop means everything is in the bucket.

**ZeroFS.** keepsync pins ZeroFS v2.3.7 and checks SHA-256 for both the release tarball and the platform binary. Builds exist for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and freebsd/amd64. The binary is looked up as `--zerofs`, then on PATH, then in `zerofs/` in the server directory. If none is found, `server init` downloads the pin, unless `--no-download` is given. `run` never downloads. A binary that does not match the pin produces a warning, never an upgrade.

The supervisor writes `zerofs.toml` with 9P and RPC on private Unix sockets only, no NFS and telemetry off. Secrets go in the environment, never in the file. ZeroFS is always reached over 9P, because it reports NFS writes as stable while they are still buffered. The mount mode is set at init with `--mount`, and `server init` writes it to `server.toml`; a `server.toml` without one (from before `9p`) means `fuse`:

| Mode | How | Needs |
| --- | --- | --- |
| `9p` (default) | Mounts nothing. keepsync is the 9P client, on ZeroFS's private socket, and nothing else on the machine sees the filesystem. Uses the `files` layout. | Any platform ZeroFS runs on. No FUSE, no root. |
| `fuse` | ZeroFS's own 9P client (`zerofs mount`) through FUSE, as the current user. Reconnects if zerofs restarts. | Linux, `/dev/fuse`, `fusermount3`. No root. |
| `v9fs` | The Linux kernel 9P client (`mount -t 9p`) | Linux, root, the `9p` and `9pnet_fd` modules |
| `external` | Mounts nothing. The operator has mounted the 9P export at `--mount-dir`; keepsync checks it is a mount point, so it never writes to a plain local directory. | Any platform, including macOS and FreeBSD |

**Choosing a mount mode.** Use `9p` unless something else needs to see the filesystem: it needs nothing installed, works on every platform ZeroFS runs on, and writes far faster (see Testing). Use `fuse`, `v9fs` or `external` to inspect the bucket's files with ordinary tools while the server runs, or for a bucket in the `diskpacked` layout, which only a mount can reach. The mode can be changed after init by editing `mount` (and `mount_dir`) under `[zerofs]` in `server.toml` and restarting the server, because the layout comes from the bucket: a `files` bucket works in any mode, and a `diskpacked` bucket needs a mount mode. `recover --mount` picks the mode for a recovered server.

The original design said mount mode needs root. The FUSE mode removed that, and `9p` removed the mount.

**ZeroFS restarts.** The supervisor checks ZeroFS every 2 seconds: that the process is alive and, over 9P, that it answers a walk within 5 seconds (through a mount, that the mount client is alive). When either fails it reconnects on its own: if the process still runs it only reconnects (or remounts), otherwise it restarts ZeroFS on the same config and cache and then reconnects. It retries with backoff (1 s doubling to 30 s) for a reconnect window of 5 minutes. Over 9P the client is redialled in place, so perkeepd's blob store keeps the same object and carries on; an operation in flight at the moment of the drop fails and is retried by its caller (a keeper's next pass, a node's next upload). While reconnecting, flushes fail with the outage in the error, so nothing new becomes durable and expiry waits; sync is unaffected. ZeroFS buffers writes in RAM until a flush, so a ZeroFS that dies loses what it had not flushed. The supervisor counts each restart of a dead (or unanswering, and so killed) process, and the first flush after one repairs for it: blobs seen in the blob store since the last successful flush are no longer counted as seen, so `Durable` reports them only once a flush after the restart covers them, and in store mode every local blob the replica lost is copied to it again (perkeepd's replication already counted them as copied and would not retry). A flush that a restart cuts across fails and makes nothing durable. In cache mode the blob store is on ZeroFS, so the server keeps a local copy of every claim and permanode it signs (in `unflushed/` in the server directory) until a flush covers it, and the first flush after a restart puts back any the restart lost. They are the same signed blobs, so the index and the kept trees stay right; signing again would make different ones. Content the restart lost is not on the server at all, but it was never reported durable, so the nodes still have it, and keepers put it back: from the pass after a keeper keeps a file, it asks the server whether the file is durable, and while it is not, puts the file again (which uploads only what the server lacks), backing off from one keep interval to an hour between puts so a long outage doesn't rehash every recent file each pass. With the default 5-minute keep interval and 1-minute flush interval a file is normally durable by its first check, so this costs one `Durable` call per kept file. The tracking is in memory, so a node restarted meanwhile falls back on the put a keeper makes before expiring a file. Directories are lost the same way, and perkeepd's `files` store remembers the ones it made, so over 9P a write into a missing directory makes it again. A reconnect to a ZeroFS that kept running (a dropped 9P connection) loses nothing. A SIGKILLed ZeroFS on a local MinIO came back in about 0.3 s (`TestRealZeroFSReconnect`, and the e2e `zerofs-restart` step).

If ZeroFS is not back within the window the supervisor gives up and stops trying: the server stays up, so sync, the registry and status keep working, but nothing reaches the bucket until `keepsync server run` is restarted. `server status` has a `zerofs:` line (`up since ...`, `RECONNECTING: ...` with the time it went away, the failed attempts and the last error, or `DOWN: ... restart keepsync server run`), every node's server summary shows the same state and calls the server degraded or down, and the log records the loss, each failed attempt, the reconnect and the giving up. Reconnecting is tested end to end over 9P; the mount modes remount the same way, which for `fuse` needs `fusermount3` to clear the dead mount.

**ZeroFS cache.** In store mode the replica is written far more than it is read: reads are the durability check's stats (and `diskpacked`'s index lookups) and recovery. So store mode defaults to `cache_size_gb = 2` and `memory_cache_gb = 0.25`, which are ZeroFS 2.3.7's floors (it gives each half of the disk cache at least 1 GiB and each half of the memory cache at least 32 MiB, whatever is configured). In cache mode the disk cache is every read's local copy, so it defaults to 10 GB and the user sets it with `--cache-size` (`cache_size_gb`); below 2 GB is rejected, because ZeroFS would not use less. ZeroFS gives a tenth of it (at least 1 GiB, at most 16 GiB) to metadata and the rest to data. The cache is a read cache in effect: writes are buffered in RAM (up to about 1.25 GiB while a burst is being uploaded) and PUT straight to the bucket, so the cache size changes neither write buffering nor what a flush has to do. A read that misses the cache is a GET from the bucket, so recovery on a fresh machine with an empty cache reads everything from the bucket, and a small cache only means older data is fetched again rather than served locally.

**ZeroFS password.** `server init` generates it from 32 random bytes and stores it in `server.toml` and the recovery kit. `--zerofs-password-file` adopts an existing bucket's password instead.

**Durability watermark.** Every `flush_interval` (default 1m) the server flushes ZeroFS. A blob is durable once it was seen in the blob store on ZeroFS before a flush started, and that flush succeeded. Only blobs someone asked about are tracked: `Durable` queues the unseen ones, and each flush checks the queue against the replica first. A ref counts as durable only if its whole closure is: a file's parts, recursively; a claim's permanodes; and, for a claim or permanode, the server's public key, which a reindex needs to verify it. A ref the server doesn't have is not durable. `server status` shows the last flush, or that flushing is failing.

**Disk use.** `server status` shows the mode and what it uses. In store mode: the local blob store, the index and the ZeroFS cache. In cache mode: the ZeroFS cache against its limit, and the index. Both show the whole server directory, the volume's free space, and the bucket's size and object count. Local sizes are measured by walking the directories (at most every 15 seconds, never into the mount). The bucket is listed with S3 ListObjectsV2 at startup and every 10 minutes, one request per 1000 objects, so its figure can be up to 10 minutes old. ZeroFS packs data into large segments, so the listing stays short. Every member node can fetch a short summary over the registry: health (flushes working or failing), the last flush, local disk use for the mode, the volume's free space, the bucket size and how many nodes are online. `keepsync status` on a node prints it under the server line. Node keys, scopes and the node table stay in `server status`.

**Signing.** Nodes upload file blobs, which are content-addressed and unsigned. Then they ask the registry to keep a fileref at a path in a kept folder, with the file's Syncthing version, and the server signs the claims (see "Kept tree layout"). Signing is serialised. The server writes nothing if the path already holds that ref (`ClaimUnchanged`). Otherwise it decides with version vectors (`api.Supersedes`):

- A claim whose version descends from the kept one is accepted, whatever its mtime. That covers a file recreated after expiry, a `restore --force`, a swap, and edits after a fast-clock node set a future mtime.
- A claim the kept version descends from is rejected (`ClaimRejectedOlder`), whatever its mtime, so a lagging node cannot roll a file back.
- Concurrent versions, or a missing version on either side (files kept before versions were recorded), fall back to the old rule: the mtime must be newer.

Claim dates strictly increase, so a later claim always wins, even within one clock tick. A claim is also dated after the newest claim already on each permanode it changes, read from the index, so a clock that went back across a restart or a recovery cannot date a new version before the one it replaces.

**Node scopes.** Every tunnelled connection to perkeepd is checked against the node's scope.

| Scope | Allows | Granted by |
| --- | --- | --- |
| `blobs` (default) | Upload, stat and fetch blobs, plus discovery. Not sign, enumerate, search or remove. | Every node |
| `read` | `blobs` plus search, the web UI, `pk-get` and `pk-mount`, over the whole archive | `keepsync server grant <name> read` |

The scope is stored on the node's registry record. No scope can sign or delete.

**`read` covers every folder.** A node with `read` can search and browse the archive of every folder, including folders it is not a member of. keepsync assumes all of a server's nodes are trusted, so this is deliberate; `server grant` says so when it grants `read`. Per-folder scoping is not built. If a server is ever shared between people, the plan is Perkeep share claims on each folder's root, which let a node browse its folders' trees (but not search). Filtering search and blob access by folder was rejected: blobs are shared across folders, and checking what each node can reach is expensive.

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
- Every file has its own permanode, with `camliContent → fileref`, `keepsyncMtime`, `keepsyncVersion` (the Syncthing version vector, as `ID:counter,…`) and `keepsyncMode` (its permission bits, in octal; a chmod with the same content sets it on its own). Its directory names it with `camliPath:<name> → file permanode`.
- A file that left sync also has `keepsyncGone` (`expired` or `deleted`) and `keepsyncGoneAt`. Keeping the path again clears both, in the same claim as the content; keeping identical content again (such as restoring an expired file) clears them on their own.

Each change is one signed multi claim:

- A changed file gets one claim on its permanode, which sets `camliContent`, `keepsyncMtime` and `keepsyncVersion` together. Nothing above it changes.
- A new file gets a new permanode, and one claim sets its content, mtime and version and names it in its directory. A missing directory gets a permanode too, named in the same claim.
- A path that changes type (a kept file `notes` is now a directory, or the reverse) gets a new permanode of the right type, and one claim on the parent points the name at it. The old permanode stays in history, so `PathsAt` and `at/` still show the old type. File to directory is always accepted. Directory to file needs a newer version, or without one a newer mtime than the directory's newest file.

The version rule (see "Signing") is checked per file permanode. `History` returns every content version kept at a path, oldest first, with its version and when it was kept. A path is recoverable by name only when the fileref, the file's permanode and every claim and permanode from the root down to it are durable. The registry returns that set with each path entry (`PathEntry.Claims`), and expiry checks all of it. `PathsAt`, `restore --at` and `pk-mount`'s `at/` all fold the same claims as of a time.

**The keeper loop.** Each keeper node makes a pass right away and then every `keep_interval` (default 5m):

1. Walk the folder, skipping excluded files.
2. Ask Syncthing for each file's state. A file this node does not hold the current version of (not `Current`, unknown, deleted or a directory) is reported as lagging, and is not put or claimed. A node that is behind therefore never claims.
3. Put each remaining file past grace, or past max-defer, through Perkeep. A put cache makes unchanged files free, and blobs the server already has are not re-sent.
4. Ask the server to keep the fileref at the file's path, with its Syncthing version.
5. A missing file is never translated into a delete. Instead, for each kept path with no file on disk, no gone mark yet, and a state that Syncthing reports as known, current and deleted, the keeper asks the server to mark it gone: `expired` only if a keeper could have expired it: the kept version is past the expire window, its recovery set is known, and the server reports it durable. Otherwise it is `deleted`, so a file deleted by hand before its flush is not brought back by a plain restore. If the server cannot answer the durability check, the marks wait for a later pass. Unknown or not-current state marks nothing. A file this node expired itself is always marked `expired`, and if that mark fails, it is retried as `expired` on later passes rather than judged again.

Whether a file is "already kept" or "lagging" is decided by versions when both the file and the tree have one and they are not concurrent; otherwise by mtimes compared to the second, so filesystems with coarse mtimes agree with fine ones. Running two keepers is harmless, because the server deduplicates claims. Grace and max-defer are measured from when the keeper first saw a version, if the mtime is in the future. That state is in memory, so after a restart it is measured from the first version seen after the restart.

**Expiry.** A pass then removes a file from sync once it is unmodified for the expire window, this node holds Syncthing's current version of it, its content and version are what the tree holds, and the server reports the whole recovery set durable. Before removing, the keeper hashes the file again, never trusting its put cache, so a file with the kept mtime but different content is not removed. After removing, it marks the path `expired`. If the server cannot answer, expiry pauses and sync folders grow past the window. That is the intended failure mode.

**Excluded from keeping:** Syncthing temp files (`.syncthing.*.tmp`), `.stfolder`, `.stversions`, `.keepsync`, the ignore files themselves, anything matched by `.stignore`, and an optional `.keepignore`. The built-in matcher covers common `.stignore` syntax. Conflict copies are kept like any other file.

**`keep status <folder>`** lists files as in grace (with when they will be kept), due, kept, lagging (the archive has a newer version), changed (the kept version's mtime but different content, so never expired), or expirable. Like expiry, it checks the content of the files it would call expirable. It reports when durability is unknown, and lists the files the last pass failed on, with the error; the node also logs each such error once.

**Restore.** `keepsync restore <folder> [<path>] [--at <time>] [--to <dir>] [--force] [--deleted]` reads kept files back from the server over tailcat, including expired ones:

- It reads the current tree, or the tree as of `--at`. `--at` is when the server kept a version, not a file mtime. It takes RFC 3339, `YYYY-MM-DD[ HH:MM]` in local time, or an age such as `3d` or `12h`.
- Files go back into the folder, or into `--to`, keeping their relative paths.
- Restored files get the kept permission bits, or 0644 for files kept before modes were recorded.
- A local file with the kept size and mtime is left alone.
- A local file that differs is skipped unless `--force` is given.
- A file that was deleted in sync (as of `--at`, if given) is skipped unless `--deleted` is given, and listed as "skipped (deleted in sync)" with a count. Expired files are always restored.
- If a restored file is already past the folder's expire window, the result says so, because the next pass expires it again.

## Syncthing

Syncthing v2.1.7 runs in-process through `lib/syncthing`. It is vendored and pruned in `third_party/syncthing`, wired in with a `go.mod` replace. Two reasons: the published module does not build as a library without generated GUI assets, and v2 has no importable module version. It carries three small patches (see `third_party/syncthing/KEEPSYNC.md`): a stub `gui.files.go`; an `Internals.LocalFileInfo` accessor that `FileState` uses; and conflict copies named after the losing version's device instead of the winner's (see "Conflicts"). `internal/syncer/README.md` lists the Syncthing internals touched and how stable each one is.

**Config.** Peers and folders come from the registry, never from introducers or a config file at startup. Global and local discovery, relays, NAT, usage and crash reporting, upgrades and the GUI are off.

**Transport bridge.** Syncthing listens only on `127.0.0.1:<free port>`.
- Inbound: the port 22000 handler checks the caller is a current peer, dials Syncthing's loopback listener and splices the two connections.
- Outbound: each peer gets its own loopback listener, which is that device's only address in Syncthing's config, and each connection to it is dialed to the peer over tailcat.

BEP's TLS runs end to end, so Syncthing still checks device IDs.

**Restarts.** On start, the syncer restores the previous run's folders and peers from `config.xml`. Without this, Syncthing would drop the index of any folder missing at startup, every file would be rescanned as a new version, and a delete on another node (such as expiry) would come back as a conflict copy.

**Folder types.** Every node defaults to send-receive. A folder's `ReceiveOnly` list marks nodes as receive-only.

**Conflicts.** Syncthing writes `.sync-conflict-<date>-<device>` copies, where `<device>` is the short ID of the device whose losing change the copy holds (a keepsync patch; upstream names the winner). Conflict copies are kept in Perkeep under their own name, next to the original, and expire like any other file, so a losing version is never lost and unresolved conflicts do not pile up in sync.

**Conflict hook.** `conflict_hook = "/path/to/script"` in node.toml applies to every folder, and a `conflict_hook` on a `[[folder]]` overrides it. The hook lets an LLM or any other tool resolve conflicts.

- **Who runs it.** On a kept folder, only `Keepers[0]`. On an unkept folder, `Members[0]`. Other nodes only report.
- **When.** On each conflict event from Syncthing, and on a scan of the folder when the node starts, attaches it, or becomes its hook runner (for example by keeper failover). Each copy is debounced for 5 s, and hooks run one at a time per folder.
- **The base**, on a kept folder, is read before the hook runs. Let W be the original's current Syncthing version. The node walks the path's kept history, newest first, and takes the first version whose content differs from both the original and the copy and that W descends from (a version kept without a Syncthing version counts as an ancestor). The base is exact if that version has a Syncthing version. With no such version, the base is the last kept version, as a guess. A version kept after the copies diverged is one of the sides, so it is never the base.
- **Before it runs**, on a kept folder, after reading the base, the node keeps the original and the copy immediately, skipping grace, so anything the hook does is recoverable. If that fails, the hook does not run.
- **Duplicates.** Every node that held the losing version can make a copy of it, a bystander that received both versions included, so one conflict can arrive as several identical copies, at once or one after another. The hook runs once. A copy with the same content as another copy of the same original that is still present and sorts first by name, or that the hook resolved within the last hour, is removed as a duplicate.
- **Timeout.** `conflict_hook_timeout` (default 5m). On timeout the hook's whole process group is killed.

The script runs in the folder root and its output is logged. It gets:

| Variable | Value |
| --- | --- |
| `KEEPSYNC_FOLDER` | Folder name |
| `KEEPSYNC_ORIGINAL` | Absolute path of the current file |
| `KEEPSYNC_CONFLICT` | Absolute path of the conflict copy |
| `KEEPSYNC_CONFLICT_NODE` | Name of the node whose losing change the copy holds, else its short device ID. It comes from the copy's name: keepsync's Syncthing names copies after the losing version's device (upstream names the winner). |
| `KEEPSYNC_BASE` | The base in a temp file, for a three-way merge, or empty if there is none (always empty on an unkept folder). The file is deleted afterwards. |
| `KEEPSYNC_BASE_EXACT` | `1` if the base is known to be the common ancestor, else `0` (a guess, or no base). |

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

1. Starts ZeroFS on the kit's bucket, reached the way the old server reached it (the kit records the mount mode) unless `--mount` says otherwise.
2. Starts perkeepd on a fresh index, in the kit's mode or the one given with `--mode`.
3. In store mode, copies every blob of the bucket's blob store into fresh local storage, which reindexes them. In cache mode it copies nothing: it reads every blob from the bucket, checks its hash and feeds it to the index.
4. Records the mode it recovered in, and installs the tailcat key, config and registry. It prefers the registry mirrored in the bucket, and falls back to the kit's copy.

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
| `server init --bucket <url> [flags]` | Server | Creates the tailcat key, Perkeep identity and ZeroFS config, finds or downloads ZeroFS, writes the kit, prints the address. Flags: `--endpoint`, `--region`, `--access-key`, `--secret-key` (default `$AWS_*`), `--allow-http`, `--zerofs`, `--zerofs-password-file`, `--mode store|cache`, `--cache-size <GB>`, `--mount`, `--mount-dir`, `--no-download`, `--kit`, `--derp-map`, `--derp-region`. |
| `server run` | Server | Supervises ZeroFS, perkeepd, the registry and tailcat. Drains and flushes on stop. |
| `server add <nodekey> [--name]` | Server | Approves a node. The name defaults to the one the node registers with. |
| `server remove <name>` | Server | Removes a node. |
| `server nodes` | Server | Lists members. |
| `server status` | Server | Address, key, last flush or flush failure, mode, local disk and bucket use, volume free space, folders, nodes. |
| `server kit [--out]` | Server | Regenerates the recovery kit. |
| `server baseline <out.json>` | Server | Flushes and snapshots the server for a drill. |
| `server grant <name> <blobs\|read>` | Server | Sets a node's Perkeep scope. |
| `node init --server <tc… or DNS name> [--name]` | Node | Writes config and keys, prints the node key, exits. The name defaults to the hostname. |
| `node run` | Node | Runs tailcat, Syncthing, the keepers and the gateway. |
| `status` | Node | Server connection (or how long it has been unreachable) with a short server summary (health, last flush, disk use, nodes online), peers (with direct or relay path and traffic rates), folders (with sync progress). |
| `folder add <name> <path>` | Node | Creates a synced folder in the registry and attaches it here. |
| `folder join <name> <path> [--merge]` | Node | Attaches an existing folder here. A directory with files needs `--merge`. |
| `folder ls` / `folder rm <name>` | Node | Lists folders with their sync state, or stops syncing one here. Files stay. |
| `folder status <name> [-v] [-n 100]` | Node | One folder's sync state, what this node needs, each peer's completion, path and traffic; `-v` lists needed files. |
| `keep <folder> [--grace] [--max-defer] [--expire]` | Node | Marks a folder kept. Creates the permanode and `.keepsync`. |
| `keep status <folder> [-v]` | Node | Files in grace, kept, lagging and expirable. |
| `unkeep <folder>` | Node | Stops keeping a folder on every node. The archive stays. |
| `restore <folder> [<path>] [--at] [--to] [--force] [--deleted]` | Node | Pulls kept files back, including expired ones, and from unkept folders. |
| `conflicts` / `conflicts resolve <path>` | Node | Lists conflict copies and hook state, or runs the hook now. |
| `gateway config [--path]` | Node | Writes a Perkeep client config for the local gateway. |
| `recover <kit> [--mode store\|cache] [--drill --bucket <copy> --baseline <file>]` | Any | Rebuilds the server from the bucket, in the kit's mode or `--mode`, or runs the drill. |

`purge` exists as a command but reports "not implemented yet".

## Failure modes

When the server is unreachable, sync keeps working, and only keeping, expiry and membership changes wait. No failure path can expire a file whose recovery set is not durable in the bucket.

| Failure | Behaviour |
| --- | --- |
| Server unreachable | Syncing continues with the cached peer list. The registry client reconnects with backoff. The keeper retries next interval; there is no queue to lose, because the walk is idempotent. Expiry pauses. `status` shows how long the server has been unreachable. |
| Node never approved | `status` shows the `server add` command to run. |
| Keeper node gone | After 2 minutes the server reassigns its keeper role. The second keeper keeps working meanwhile. |
| A keeper is behind | It does not claim files it does not hold Syncthing's current version of, and the server rejects a claim whose version is older than the kept one. `keep status` shows such files as lagging. |
| Registry connection goes silent | Both sides drop it within 30 s, and the node reconnects and refetches the membership. |
| ZeroFS flush failing | Nothing new becomes durable, so expiry stops removing files. `server status` shows the failure. |
| ZeroFS exits or stops answering | The supervisor restarts and reconnects it within 5 minutes. Meanwhile flushes fail as above, and `server status` and the nodes' server summary show it reconnecting. |
| Conflict hook fails or makes a bad merge | A failure leaves the copy for a human, shown in `conflicts`. Both versions were kept before the hook ran, so a bad merge can be undone with `restore`. |
| Server lost | `keepsync recover <kit>` from the bucket. The tailcat key is the same, so nodes reconnect on their own. |

## Edge cases

Things operators still need to watch for, and what to do about each. Behaviour that used to be a problem and is now handled is described in the sections above, not here.

**An in-place edit that keeps both size and mtime.** Some tag editors have a "preserve modification time" option, and tags often fit in existing padding. Syncthing compares size and mtime, so it never sees the edit, and neither does the keeper. Expiry hashes a file before removing it, so the editing node does not remove its edited copy. But the edit never syncs, and on any other node the old content expires as usual; that delete then reaches the editing node, where Syncthing thinks the file is unchanged and deletes it too, losing the edit. **Do:** touch the file after such an edit, or turn the editor's option off.

**An old file arriving in an expiring folder.** Expiry is measured from the mtime, so a file copied in with an mtime already past the window (an unzip, `cp -p`, a photo import) is kept on the first pass and removed from every node a pass or two later, once it is durable. It is safe in the archive, but to a user it vanishes within minutes of being added. A path recreated after it expired behaves the same way if the new file has an old mtime. **Do:** touch files you import into an expiring folder, or `restore --to` them somewhere outside it.

**A file with an mtime in the future.** It is kept after grace like any other, counted from when the keeper first saw it. But it does not expire until the clock passes its mtime plus the window, so a file stamped years ahead (a bad camera clock, a zip with bogus dates) stays in sync for years. **Do:** fix the mtime with `touch`.

**What the archive does not hold.** The keeper walks regular files only. Symlinks and empty directories, which Syncthing does sync, are never kept, so a restore cannot bring them back. **Do:** keep anything that matters as a regular file, or recreate links after a restore.

**Archives kept before versions, gone marks and modes.** Files kept by earlier builds have no Syncthing version, gone mark or permission bits. For such a path's next version only, the old mtime rule applies, so an older mtime is rejected as a rollback; paths that left sync before marks existed are still brought back by a plain `restore`; and files restore as 0644 until their next edit. **Do:** `touch` a file whose older-mtime version is shown as lagging, and check the result of a full restore into a live folder (or use `--to`).

**`.stignore` is per node.** Syncthing never syncs `.stignore`, and each keeper reads its own. A path one keeper ignores is still kept by the other, but if both keepers ignore a path that other nodes sync, it is never kept. Keepers are chosen without looking at their ignore files. **Do:** use `.keepignore`, which syncs and is the same on every node, for anything meant to stay out of the archive, and keep `.stignore` the same on nodes that can be keepers.

**Leaving and rejoining a folder at the same path.** `folder rm` removes the folder from Syncthing, which drops its index and its `.stfolder` marker. (The syncer assigns Syncthing's folder and device lists rather than using its `SetFolders`/`SetDevices`, which only add and replace entries, so a removed folder used to keep syncing.) Rejoining at the same path rescans every file as a new local version. A file deleted elsewhere in the meantime, including by expiry, conflicts with that rescanned version, and Syncthing resolves a delete against a change in favour of the change, so it comes back on every node. Expired files are removed again on the next pass; files deleted by hand stay back. **Do:** rejoin into an empty directory, or delete the returned files again.

**Leaving a folder while the server is unreachable.** The node stops syncing at once and retries the leave whenever the server answers. Until then it is still a member and can still hold a keeper slot that keeps nothing, so with both keepers in that state the folder is neither kept nor expired for the length of the outage. **Do:** nothing, unless the outage is long; `keep status` on a remaining member shows whether its keepers are working.

**The conflict hook runs only where it is configured.** The hook runs on `Keepers[0]` (`Members[0]` for an unkept folder), and only with the hook set in that node's `node.toml`. A hook set on another node shows "not-this-node" in `keepsync conflicts`, and a runner without one shows "none". An unkept folder's runner is its creator, with no failover. **Do:** set the same hook on every node that can become a keeper, and run `keepsync conflicts resolve` by hand for copies left behind. A base with `KEEPSYNC_BASE_EXACT=0` is a guess: a hook should leave those conflicts for a human.

**Recovery loses recent registry changes.** A recovered registry has the state mirrored at the last flush, or the kit's copy if the mirror is missing. Nodes take the recovered membership at once, but changes after that point are lost: a node approved, a folder added or a policy changed in that window has to be done again. **Do:** after a recovery, check `server nodes` and `folder ls`, and regenerate the kit after membership changes so its fallback copy stays recent.

**ZeroFS gone for more than 5 minutes.** The supervisor restarts and reconnects ZeroFS on its own (see ZeroFS restarts), but gives up after the 5-minute window, for example when the bucket is unreachable for that long or ZeroFS fails at every start. The server then stays up with ZeroFS down: flushes fail, so nothing new becomes durable and expiry stops, and blob reads and writes fail. Sync is unaffected. **Do:** when `server status` (or a node's server summary) shows `zerofs: DOWN`, fix the cause and restart `keepsync server run`.

**The `read` scope covers the whole server.** `server grant <name> read` opens search, the web UI and `pk-mount` for every kept folder on the server, including folders the node never joined or has left. The registry's own calls (`Paths`, `PathsAt`, `History`, claims, gone marks) check folder membership, but the Perkeep scope does not, and the `blobs` scope can fetch any blob whose ref it knows. This is by design (see "`read` covers every folder"). **Do:** grant `read` only to nodes trusted with every folder, and don't share a server between people who should not see each other's folders.

## Testing

Every package has unit tests against the fakes in `internal/api/fake`.

`test/e2e` starts a server and three nodes in one process, with tailcat on an in-process derper. Its steps cover:
- join and sync, and `folder join --merge`
- keeping after grace
- expiry gated on durability
- a delete after a node restart propagating, not coming back as a conflict copy
- leaving a folder, and `unkeep`
- the conflict hook: one run per conflict, the losing node's name, both versions kept first, and an exact base
- keep rules: a file recreated with an old mtime, two kept files swapped, file to directory checked against `restore --at`, a lagging node that never rolls the archive back, and delete versus expiry with `restore --deleted` (including a hand delete past the window before a flush)
- disk and bucket use in `server status` and the node's server summary
- `recover --drill`
- keeper reassignment

With `KEEPSYNC_ZEROFS_BIN` and `KEEPSYNC_MINIO_BIN` set, it runs against a real ZeroFS and MinIO; without them those steps skip. `KEEPSYNC_E2E_MODE` picks the server's mode (`store` or `cache`). All steps pass, with no known bugs, against ZeroFS 2.3.7 and MinIO.

**Blob store benchmark.** `TestBlobStoreBench` in `internal/zerofs` (set `KEEPSYNC_ZEROFS_BENCH=all` and the real-ZeroFS variables) writes the same blobs to each way of putting a blob store on ZeroFS, flushes, reads them back, then restarts ZeroFS with an empty cache and reads them again from the bucket. 500 random blobs (26.7 MiB, one in ten 256 KiB, the rest 0.5 to 64 KiB), 8 workers, ZeroFS 2.3.7 and MinIO on one machine, 2026-11-15:

| Store | Write | Flush | Warm read | Cold read | Bucket objects |
| --- | --- | --- | --- | --- | --- |
| `files` over 9P | 0.52 s (51 MiB/s, 1.05 ms/blob) | 0.19 s | 0.21 s (127 MiB/s) | 0.27 s (100 MiB/s) | 24 |
| `files` through FUSE | 70.6 s (0.4 MiB/s, 141 ms/blob) | 0.03 s | 0.40 s (66 MiB/s) | 0.93 s (29 MiB/s) | 1468 |
| `diskpacked` through FUSE | 266 s (0.1 MiB/s, 532 ms/blob) | 0.03 s | 0.12 s (214 MiB/s) | 0.34 s (79 MiB/s) | 3483 |

Writes through the mount are bound by Perkeep's per-blob fsync, which ZeroFS turns into a full flush: each makes new small objects, hence the object count. Warm reads of `diskpacked` through FUSE are faster (the kernel caches the pack file); cold reads, the ones that matter in cache mode and recovery, are fastest over 9P. 9P is the default for new servers on these numbers. Not yet measured: keep throughput, bucket write operations and restore time on a real 30 GB set.

## Not built yet

- **Deleting from keep**, as a `del-attribute` on `camliPath:<name>` in the parent directory's permanode, or a `delete` claim. Both are reversible. **`purge`**, the rare manual physical delete, since Perkeep has no garbage collector.
- **A native tailcat transport inside Syncthing**, replacing the loopback bridge. It needs a patch to the vendored `lib/connections`.
- **Signed writes through the gateway** (a `write` scope). The server keeps the only identity. Web UI edits already sign through the server's signing handler, but CLI tools such as `pk put` sign with their own key, and Perkeep does not honour a second signer by default. Either CLI signing is proxied to the server, or the server trusts a second signer. One identity is preferred, so this is still an open question.
- **A replica server mode.** A second keepsync server that runs no ZeroFS. It connects to the main server over tailcat and serves as a Perkeep blob store replica, so the main perkeepd replicates blobs to it with Perkeep's own blob replication (`replicateTo`). It gives a second full copy of the archive on another machine, alongside the bucket.
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
- Patched Syncthing files (the `gui.files.go` stub, the `LocalFileInfo` accessor and the conflict-copy naming in `folder_sendrecv.go`) stay MPL-2.0 and are published.
- The recovery kit carries ZeroFS's license text and a link to the exact source for the pinned version.
- ZeroFS is never linked or modified. A patched ZeroFS would fall under the AGPL network clause.

This is a reading of the licenses, not legal advice.
