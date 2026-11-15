# keepsync design

Status: draft, 2026-11-14

## Overview

keepsync is one Go binary that keeps a set of folders synced across machines with embedded Syncthing, and archives them for the long term in Perkeep, replicated through ZeroFS to any S3-compatible bucket (R2, S3, MinIO and so on). Nodes and the server reach each other over tailcat, so nothing needs a public IP, open ports or DNS.

**Goals**

- A synced folder can be a recent subset (say the last 30 days) of a permanent archive, with no file ever lost by expiry.
- Everything needed to rebuild the server from the bucket fits in one encrypted recovery kit.
- Joining a node is one command plus approval on the server.
- Prove out embedding Syncthing as a library.

**Non-goals**

- Replacing Syncthing's sync engine or conflict detection.
- Physical deletion from the archive as a normal operation (purge is a rare manual admin step).

## Architecture

One binary plays two roles. Every host runs `keepsync node`. One host runs `keepsync server`, which holds Perkeep, ZeroFS and the registry. Nodes never touch ZeroFS or the bucket, and they reach the server only through tailcat.

```
 +---------------------------+                         +---------------------------+
 | Node A                    |  Syncthing over tailcat | Node B                    |
 |  embedded Syncthing       |<----------------------->|  embedded Syncthing       |
 |  keeper: grace/keep/expire|     direct or DERP      |  keeper: grace/keep/expire|
 |  tailcat listener :22000  |                         |  tailcat listener :22000  |
 +-------------+-------------+                         +-------------+-------------+
               |   register, upload blobs, request claims (tailcat)  |
               v                                                     v
        +--------------------------------------------------------------------+
        | keepsync server (Linux, macOS, FreeBSD)                            |
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

Syncthing traffic runs node to node. The server is only on the path for membership, keeping and recovery, so sync carries on when it is down. The code is a Go library package (`keepsync`) plus a thin `cmd/keepsync`.

## Networking over tailcat

Every keepsync process runs one `tailcat.Server` and dispatches incoming connections by virtual port with `OnTCP(port)`. Clients reach a service with `DialTCPPort(port)`, which returns a `net.Conn`. No service listens on a real port.

| Virtual port | Service | Where |
| --- | --- | --- |
| 1 | Registry API | Server |
| 3179 | perkeepd HTTP | Server |
| 22000 | Syncthing BEP | Every node |

**Bootstrap.** A node needs only the server's `tc…` address. DNS TXT is an optional convenience: `node init` resolves it once and saves the real address. Publishing the address is safe only because the server allow-lists node keys.

**Join flow**

1. `keepsync server init` creates a saved tailcat key (region pinned to the user's derper if they run one) and prints the server address.
2. `keepsync node init --server tc…` writes the node's config, its tailcat key and its Syncthing device cert, prints the node key, and exits.
3. `keepsync server add <nodekey>` puts the key on the server's allow list. The allow list is the membership list.
4. `keepsync node run` starts the node's tailcat listener (port 22000 to Syncthing) and embedded Syncthing with global discovery, local discovery and relays off. It registers `{nodekey, tc address, device ID}` on port 1 and receives the member list and its folder assignments over a held-open connection.
5. For each peer, the node adds the peer's key to its own allow list, opens a `tailcat.Client` to the peer, and adds the peer as a Syncthing device. If two nodes dial each other at once, Syncthing drops the duplicate connection.

**Syncthing transport.** First version: Syncthing binds to localhost and a small bridge connects it to tailcat on both sides. Target: a `tailcat` dialer and listener registered inside Syncthing's connection layer. That is an internal API, so it is part of proving out embedding.

**Relays.** Traffic goes direct when NAT traversal works and falls back to DERP when it does not. tailcat's free DERP relays are rate-limited, so the docs recommend running your own `derper` for real sync volumes. That choice is the user's.

## Keep server

The server runs perkeepd with local blob storage and a `diskpacked` replica on ZeroFS, which persists to an S3-compatible bucket. It is the only machine that holds the Perkeep identity key, and the only one that runs ZeroFS. It runs on Linux, macOS or FreeBSD, because ZeroFS ships no Windows build. Nodes can run anywhere.

**Perkeep config** follows the current proof of concept: `packRelated: true` for local storage, a `diskpacked` storage at the ZeroFS directory, and `replicateTo` that storage, which sets up an asynchronous sync handler.

**ZeroFS, two modes.** Both write the same `diskpacked` on-disk format, so a bucket written by either mode can be read or restored by either.

1. **Mount mode (first).** `keepsync server run` generates the ZeroFS config, starts `zerofs`, mounts it **over 9P, not NFS** (ZeroFS reports NFS writes as stable while they are still buffered), waits for health, then starts perkeepd. On stop it drains the sync queue and flushes ZeroFS, so a clean stop means everything is in the bucket. Needs root to mount.
2. **9P mode (later).** No mount and no root. A Perkeep blobserver writes `diskpacked` packs through a Go 9P client straight to ZeroFS's 9P listener. Most of the work is reimplementing diskpacked's writer on a 9P file handle instead of `os.File`.

**Getting ZeroFS.** Lookup order: `--zerofs` (flag or config), then `zerofs` on PATH, then keepsync's own copy in its data dir. If none is found, `server init` downloads the version pinned in keepsync's source and checks its SHA-256. `run` never downloads. There are no auto-upgrades, because the bucket's format is tied to the ZeroFS version. A binary that does not match the pin produces a warning.

**ZeroFS encryption password.** `server init` generates it (32 random bytes) and stores it in the server's config with owner-only permissions and in the recovery kit. Nobody types it. To adopt an existing bucket, such as the current proof of concept, `server init --zerofs-password-file <path>` imports the existing one instead.

**Signing.** Nodes upload file blobs, which are content-addressed and unsigned. They then ask the server over the registry API to keep a fileref at a path in a kept folder, and the server signs the claims (see "Kept tree layout"). The server writes nothing if the path already holds that ref, and accepts a new ref for a path only if its mtime is newer than the current one, so a lagging node cannot roll a file back.

**Durability watermark.** After each ZeroFS flush, the server records which blobs are now in the bucket. Nodes ask "are these refs durable?" before expiring anything. This is the blob verification tooling, built early because expiry depends on it.

**Registry state** (members, folders, assignments) is stored in Perkeep itself, so it comes back from the bucket on recovery.

## Keeping and expiry

A kept folder maps to one permanode whose tree only ever grows. Expiry removes files from the sync folder only after they are durable in the bucket. Each timer is measured from a file's mtime.

| Setting | Default | Meaning |
| --- | --- | --- |
| `--grace` | 48h | A file must be unmodified this long before it is kept. Deletes and edits inside this window never reach the archive. |
| `--max-defer` | 7d | Cap for files that never settle. Past it, the file is kept anyway, and again each time it hits the cap. |
| `--expire` | 30d | The file is removed from sync on every node once unmodified this long and durable in the bucket. `0` or omitted means nothing expires. |

The config is rejected unless grace < max-defer < expire.

**Mapping.** `keepsync keep <folder>` asks the server to create the permanode and writes its ref into a `.keepsync` file inside the folder. Syncthing carries that file to every node, so all nodes agree on the permanode.

**Kept tree layout.** The tree uses Perkeep's own conventions, so the web UI, search and `pk-mount` show kept folders as ordinary directories:

- The folder's root permanode has `title` and `camliRoot` set to the folder name, so it appears under `pk-mount`'s `roots/`.
- Every directory below the root has its own permanode. Its parent names it with `camliPath:<name> → dir permanode`.
- Every file has its own permanode, with `camliContent → fileref` and `keepsyncMtime`. Its directory names it with `camliPath:<name> → file permanode`.

Each change is one signed multi claim:

- A changed file gets one claim on its permanode, setting `camliContent` and `keepsyncMtime` together. Nothing above it changes.
- A new file gets a new permanode, and one claim sets its content and mtime and names it in its directory. A missing directory gets a permanode too, named in the same claim.

The server keeps the mtime rule per file permanode. A path is recoverable by name only when the fileref, the file's permanode and every claim and permanode from the root down to it are durable. The registry returns that set with each path entry, and expiry checks all of it. `PathsAt`, `restore --at` and `pk-mount`'s `at/` all fold the same claims as of a time.

**The keeper loop**, every interval:

1. Walk the folder, skipping excluded files.
2. For each file past grace (or past max-defer), `pk-put` it. The stat cache makes unchanged files free, and blobs the server already has are not re-uploaded.
3. Ask the server to keep the fileref at the file's path.
4. A missing file is never translated into a delete.

**Two keepers per folder.** The server assigns two keeper nodes per folder when available, for redundancy, and reassigns if one goes away. Running both is harmless, because the server deduplicates claims.

**Expiry.** A file is removed from sync only if its current fileref is already in the tree and the server reports those blobs durable. If the server is unreachable, expiry pauses and sync folders grow past the window. That is the intended failure mode.

**Excluded from keeping:** Syncthing temp files (`.syncthing.*.tmp`), `.stfolder`, `.stversions`, anything matched by `.stignore`, and an optional `.keepignore`. Conflict copies *are* kept (see Conflicts).

**Deleting from keep** means a claim: `del-attribute` on `camliPath:<name>` in the parent directory's permanode, or a `delete` claim on a permanode. Both are reversible and history stays. Perkeep has no garbage collector, so physical deletion is `keepsync purge`, a rare manual admin step.

## Syncthing, conflicts and the conflict hook

Syncthing is embedded as a library, with folder membership driven by the registry instead of introducers. Conflicts stay Syncthing's job, and keepsync adds archiving and an optional script hook.

**Folder types.** Every node defaults to send-receive. A folder can mark specific nodes receive-only, for mirrors that should never push changes. Syncthing file versioning (trash can or staggered) is recommended on kept folders, to cover files that have not passed grace yet.

**Conflicts.** Syncthing writes `.sync-conflict-<date>-<device>` copies. keepsync reports them in `keepsync status` and `keepsync conflicts`. Conflict copies are kept in Perkeep under their own name, next to the original. After that they expire under the normal rule, so a losing version is never lost and unresolved conflicts do not pile up in sync.

**Conflict hook.** Configured as `conflict_hook = "/path/to/script"`, per folder or globally, so an LLM or any tool can resolve conflicts. The script receives:

| Variable | Value |
| --- | --- |
| `KEEPSYNC_FOLDER` | Folder name |
| `KEEPSYNC_ORIGINAL` | Path of the current file |
| `KEEPSYNC_CONFLICT` | Path of the conflict copy |
| `KEEPSYNC_CONFLICT_NODE` | Name of the node that made the conflicting change |
| `KEEPSYNC_BASE` | Last kept version from Perkeep in a temp file (the common ancestor, for a three-way merge) |

**Contract.** To resolve, the script writes the result to the original path, deletes the conflict copy, and exits 0. Syncthing syncs that like any other edit. A non-zero exit, or a conflict copy still in place, leaves it for a human. `keepsync conflicts resolve <path>` reruns the hook by hand.

**Hook safety**

- Before the hook runs, keepsync keeps both versions immediately, skipping grace, so anything the hook does is recoverable.
- Only one node runs the hook: the folder's first assigned keeper.
- Hooks run one at a time per folder, with a timeout and a short debounce.

## Recovery kit

`keepsync server init` writes an encrypted recovery kit holding everything needed to rebuild the server from the bucket that does not live in the bucket. `keepsync recover` uses it, and its drill mode is the recovery check.

**Contents**

- `keepsync` and the pinned `zerofs` binaries for the current platform.
- S3 endpoint, region, bucket and credentials, ideally a read-only token. `recover` asks for write credentials only when actually standing the server back up.
- ZeroFS encryption password, Perkeep identity secret key, perkeepd config.
- The server's tailcat key, so a recovered server comes back at the same address and nodes reconnect without re-init.
- `RECOVER.md` with the steps, plus checksums.

**Encryption.** The kit can read all data and impersonate the server. It is written as `keepsync-recovery.zip.age` (age with a scrypt passphrase). The passphrase is shown once at init. The docs say to store the kit and the passphrase separately and offline, and never in the same bucket.

**Commands**

- `keepsync recover <kit>` mounts ZeroFS on the bucket, starts perkeepd with a fresh index and reindexes, then brings the server up with the same tailcat key.
- `keepsync recover <kit> --drill --bucket <copy>` runs the same path against a copy of the bucket without taking over the identity. On R2 or S3, `rclone copy` makes the copy server-side with no egress cost. ZeroFS is single-writer, so the drill must never mount the live bucket while the server runs.
- `keepsync server kit` regenerates the kit. It only goes stale when keys rotate or keepsync pins a new ZeroFS, because registry state lives in Perkeep.

**What the drill checks**

1. Blob check: diff the blobref lists of the live and restored servers, and read every restored blob so its hash is verified.
2. File check: extract files from the restored server and compare sha256 against the originals.
3. Index check: compare permanode counts and a few searches.
4. Record restore time and bucket operation counts.

## Perkeep gateway

Every node can expose the server's Perkeep on localhost, so stock Perkeep tools (web UI, `pk`, `pk-get`, `pk-mount`, the search API) work against the archive with no extra ports or DNS. Only read-only access is planned now. Signed writes are future work.

**How it works.** `node run` listens on `127.0.0.1:3179` (`gateway.listen` in node.toml, empty to disable) and proxies HTTP to the server's perkeepd over tailcat on `api.PortPerkeep`, reusing the tcnet round tripper. It binds to localhost only.

**Local auth.** The gateway requires a random per-node token (Perkeep userpass auth). `keepsync gateway config` writes a Perkeep client config pointing at the gateway with that token, so stock tools work without flags.

**Access scopes.** The server grants each node a scope, and pkserver enforces it on every tunnelled connection, extending the per-connection grant it already applies to nodes.

| Scope | Allows | How it is granted |
| --- | --- | --- |
| `blobs` | Upload and fetch blobs (what the keeper needs) | Every node, by default |
| `read` | `blobs` plus search, web UI, `pk-get`, `pk-mount` | `keepsync server grant <name> read` |
| `write` (future) | `read` plus signed claims through the server's signer | `keepsync server grant <name> write` |

**Future: signed writes.** The server keeps the only identity. Web UI edits already sign through the server's signing handler, so `write` scope just opens that handler. CLI tools such as `pk put` sign locally with their own key, and Perkeep does not honour a second signer's claims on the server's permanodes by default. A future design therefore either proxies CLI signing to the server through the gateway, or has the server trust a second signer. One identity is preferred; this is an open question.

**Limits.** No scope can delete blobs. Purge stays a server-side admin step.

## CLI reference

`init` and `run` commands work standalone. Every other command talks to the local running process over a control socket.

| Command | Role | What it does |
| --- | --- | --- |
| `server init` | Server | Creates the tailcat key, perkeepd config and identity, and ZeroFS config. Fetches pinned ZeroFS if needed. Writes the recovery kit. Prints the server address. |
| `server run` | Server | Supervises ZeroFS, perkeepd and the tailcat listener. Drains and flushes on stop. |
| `server add <nodekey> [--name]` | Server | Approves a node (adds it to the allow list). |
| `server remove <name>` | Server | Removes a node. |
| `server nodes` | Server | Lists members. |
| `server kit` | Server | Regenerates the recovery kit. |
| `node init --server <tc… or name>` | Node | Writes config and keys, prints the node key, exits. |
| `node run` | Node | Runs Syncthing, the keeper and expiry. |
| `status` | Node | Server connection, peers (direct or DERP), per-folder sync state. |
| `folder add <name> <path>` | Node | Creates a synced folder in the registry. |
| `folder join <name> <path>` | Node | Attaches an existing folder on this node. |
| `folder ls` / `folder rm <name>` | Node | Lists folders, or stops syncing one here (files stay). |
| `keep <folder> [--grace] [--max-defer] [--expire]` | Node | Marks a folder kept. Creates the permanode and `.keepsync`. |
| `unkeep <folder>` | Node | Stops keeping. The archive stays. |
| `keep status <folder>` | Node | Files in grace, kept, and eligible to expire. |
| `restore <folder> <path> [--at <time>]` | Node | Pulls files back out of Perkeep, including expired ones. |
| `conflicts` / `conflicts resolve <path>` | Node | Lists conflicts, or reruns the hook. |
| `recover <kit> [--drill --bucket <copy>]` | Any | Rebuilds the server from the bucket, or runs the drill. |
| `server grant <name> <blobs\|read\|write>` | Server | Sets a node's Perkeep gateway scope. |
| `gateway config` | Node | Writes a Perkeep client config pointing at the local gateway. |
| `purge` (later) | Server | Rare manual physical delete. |

## Failure modes, testing and milestones

When the server is unreachable, sync keeps working and only keeping, expiry and membership changes wait. No failure path can expire a file that is not durable in the bucket.

| Failure | Behaviour |
| --- | --- |
| Server unreachable | Syncing continues with the cached peer list. The keeper retries next interval (no queue to lose, the walk is idempotent). Expiry pauses. `status` flags it after a threshold. |
| Keeper node gone | The server reassigns the folder's keeper. The second keeper keeps working meanwhile. |
| Lagging keeper reports an older file | The server rejects refs whose mtime is not newer than the current one. |
| tailcat or registry stream drops | The node reconnects and refetches the member list. |
| Server lost | `keepsync recover <kit>` from the bucket. Same tailcat key, so nodes reconnect on their own. |
| Conflict hook makes a bad merge | Both versions were kept before it ran, so restore from Perkeep. |

**Testing.** tailcat runs in-process, so one test binary can start a server and three nodes against a local derper and a MinIO or local bucket. That gives end-to-end tests for join, sync, keep, expiry, conflicts and recover without touching a cloud bucket. Measure keep throughput, bucket write operations and restore time on a real 30 GB set.

**Milestones**

1. `server run` in mount mode, `node init`/`run` with embedded Syncthing over tailcat (localhost bridge), registry, `folder add`/`join`.
2. `keep` with grace and max-defer, server-side signing, two keepers.
3. Durability watermark and expiry.
4. Recovery kit, `recover` and `--drill`.
5. Conflict reporting and the conflict hook.
6. ZeroFS 9P mode, and a native tailcat transport inside Syncthing.
7. Perkeep gateway, read-only. Signed writes are future work, not scheduled.

**Open questions**

- Does the tailcat Go library expose the CLI's `--allow` check as a field, or does each `OnTCP` handler check the peer key itself?
- Do `folder add` and `folder join` both earn their place, or should folders appear on every node automatically?
- Which Syncthing internals does embedding need, and how stable are they across Syncthing releases?

## Licensing

keepsync itself is MIT. Every embedded dependency is compatible with that, and ZeroFS (AGPL) stays a separate, unmodified program.

| Component | License | How keepsync uses it |
| --- | --- | --- |
| Syncthing | MPL-2.0 | Embedded library |
| Perkeep | Apache-2.0 | Embedded (perkeepd and client) |
| tailcat | BSD-3-Clause | Embedded library |
| ZeroFS | AGPL-3.0 or commercial | Separate binary, run as a subprocess, spoken to over 9P |

**Obligations**

- Release binaries ship a third-party notices file: Apache-2.0 NOTICE text, BSD copyright lines, and a pointer to the Syncthing source (MPL).
- Any patched Syncthing source files stay MPL-2.0 and are published.
- The recovery kit and the ZeroFS download carry ZeroFS's license text and a link to the exact source for the pinned version.
- ZeroFS is never linked or modified. A patched ZeroFS would fall under the AGPL network clause.

This is a reading of the licenses, not legal advice.
