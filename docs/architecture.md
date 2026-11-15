# keepsync architecture

This is an overview of how keepsync is put together and why. For the exact behaviour of every piece, see the [spec](design.md).

## Overview

keepsync is built on a two-tier storage system:

1. **Sync tier: active working data** that you want synchronized across your devices.
2. **Keep tier: long-term, durable storage**, or archiving.

You can use either tier on its own, or you can mark certain synchronized folders as **kept**, which links the two: files in a kept folder are archived once they settle, and can optionally expire from sync after a while, because they are safe in the archive. A synced folder can hold just the last 30 days while the archive holds every version forever.

Each tier is implemented with an existing technology that is responsible for it:

| Tier | Powered by | Role |
| --- | --- | --- |
| Sync | [Syncthing](#syncthing) | Peer-to-peer file sync between your devices |
| Keep | [Perkeep](#perkeep) | Content-addressed, permanent archive |

Two supporting technologies round out the stack:

| Component | Role |
| --- | --- |
| [ZeroFS](#zerofs) | Puts the archive in an S3-compatible bucket, compressed and encrypted |
| [tailcat](#tailcat) | Peer-to-peer WireGuard networking, so nothing needs a public IP, open ports or DNS |

[keepsync](#keepsync) itself wraps all of this up in a single Go binary with one simple command line interface.

```
  laptop (node)            desktop (node)            NAS (node)
  Syncthing  <------------>  Syncthing  <------------>  Syncthing
      \                         |                          /
       \______________ tailcat (direct or DERP) __________/
                                |
                         keepsync server
                    Perkeep  ->  ZeroFS  ->  S3 bucket
```

Nodes sync directly with each other. The server only sits on the path for membership, archiving and recovery, so sync carries on while it is down.

## Syncthing

[Syncthing](https://syncthing.net) handles the sync tier: your synchronized, active working data.

**If you know Dropbox:** Syncthing works like Dropbox, but peer to peer. There is no central copy in someone's cloud; each device syncs directly with the others that share a folder.

Files are written directly to disk, in whatever filesystem the node's host uses (APFS on your Mac, ext4 on Linux, and so on), so you get full native speed for reads and writes. Syncthing watches for file changes and synchronizes each changed file to the other nodes subscribed to that folder.

Syncthing has fairly robust conflict detection. When two devices change the same file, it doesn't try to merge them: it keeps one version and saves the other next to it as a `.sync-conflict-…` copy, leaving it to you to resolve. keepsync adds a **conflict hook**, a script you configure that resolves conflicts automatically, potentially using an LLM or agent (see [Conflict hook](#conflict-hook)).

Syncthing is well loved and relied upon for synchronizing files across devices. keepsync embeds it as a Go library (v2, vendored with three small patches) rather than running it as a separate program, and turns off its own discovery and relays in favour of tailcat (see [tailcat](#tailcat)).

## Perkeep

The keep tier is powered by [Perkeep](https://perkeep.org) (formerly Camlistore). It's less well known, but it was created by Brad Fitzpatrick, who created LiveJournal and memcached, spent years on the Go team (where he wrote much of the Go standard library's `net/http`), and is now at Tailscale, where he wrote tailcat. So while Perkeep is trusted by a smaller audience and is less documented than Syncthing, its architectural design is quite sound and rather simple.

Perkeep has its own layered architecture. It's really a toolkit for building this kind of system, which allows lots of different configurations and setups. The core concepts come in layers:

### Blobs

The bottom layer is **blob storage**: a content-addressable store, where each blob is identified by a hash of its content. So identity is also integrity: if the bytes don't match the name, the blob is corrupt. It also gives you deduplication for free, because the same content always has the same name and is stored once.

**If you know git:** blobs are like git objects. Git names every file version and tree by its hash; Perkeep does the same, and a large file is split into chunks plus a small "file" schema blob that lists them, much like git's trees list blobs. Other content-addressed stores (IPFS, restic's repository, Docker image layers) work the same way.

A blob store can be replicated to other storage, typically with eventual consistency, so you easily get redundancy of the actual data, including to the cloud. keepsync doesn't use Perkeep's own cloud storage backends. Instead it replicates to (or, in cache mode, writes directly to) a `diskpacked` store on a ZeroFS mount, which gets the blobs into a bucket compressed, encrypted, and packed into a few large objects (see [ZeroFS](#zerofs)).

### Index

The next layer is **indexing**. The index can be rebuilt entirely from the blob store, so it can be treated like a local cache for looking data up. keepsync relies on this for recovery: a rebuilt server starts from an empty index and reindexes the blobs from the bucket.

**If you know databases:** the blobs are the log, and the index is a materialized view you can always throw away and regenerate.

### Permanodes

Blobs are immutable, so to give something a name that lasts while its content changes, Perkeep has **permanodes**. A permanode is a small signed blob whose only job is to be a stable, unique identity. Its state comes from **claims**: signed blobs that set attributes on it, such as "my content is now this file" or "my child named `notes.md` is that permanode". The index folds a permanode's claims, in date order, into its current state, and can fold them as of any earlier time too.

**If you know git:** a permanode is like a branch name, and claims are like commits that move it. Nothing is overwritten; history is just the claims.

Permanodes that point to other permanodes form **filesystem trees**. keepsync maps each kept folder to one root permanode, with a permanode for each directory and each file under it, using Perkeep's own conventions, so Perkeep's tools show kept folders as ordinary directories.

### Access

The last layer is more of an access interface. There are several ways to work with Perkeep data: command line tools (`pk-put`, `pk-get`), a web interface, a search API, and a filesystem interface (`pk-mount`, over FUSE).

This decoupling means you can choose how you want to use it. You can dump raw blobs into Perkeep without worrying about where they live in any named hierarchy, and associate them with permanodes later: for example, a "folder" permanode that refers to many blobs or files. Or you can build a structure of permanodes and expose it through the filesystem for traditional filesystem semantics, which is what keepsync does.

The filesystem also lets you see other views of the data, such as historical versions (`pk-mount`'s `at/` shows a tree as of a given time). Through the index, you can run queries across all of the data, whether or not it is in a filesystem tree.

There is no automatic garbage collection. Perkeep is about writing data in to last indefinitely. keepsync follows that: deleting or expiring a file from sync marks it in the archive, but never removes it.

### keepsync's Perkeep

keepsync is an opinionated Perkeep setup, but it is compatible with the Perkeep ecosystem: the archive is an ordinary Perkeep, with ordinary permanodes and claims, so you can use stock Perkeep tools on it however you like (see [Gateway](#gateway)). The server holds the only signing key, and nodes ask it to sign claims for them.

## ZeroFS

[ZeroFS](https://www.zerofs.net) is the first supporting technology. It provides a filesystem on top of an object store like S3.

**If you know s3fs or `rclone mount`:** those map each file to one object in the bucket, so the bucket mirrors your files one to one, every small write rewrites an object, and anyone with the bucket can read the files. ZeroFS doesn't write files directly as objects. It splits file contents into small extents (32 KiB), compresses each one (zstd or lz4), encrypts it with authenticated encryption (XChaCha20-Poly1305, so tampering is detected), and packs them into large immutable segments in the bucket, with the metadata in a database stored in the same bucket. This lets the data in the bucket be encrypted, take less space, and hide how many files it represents. It's closer to a block device or a log-structured filesystem that happens to live in S3, like JuiceFS but without a separate metadata server.

You talk to ZeroFS through a network filesystem protocol: it serves 9P, NFS and NBD. keepsync uses only 9P, because ZeroFS's NFS reports writes as stable while they are still buffered, and over 9P an fsync returns only once the data is durable. keepsync mounts it with FUSE (no root needed) or the Linux kernel's 9P client, or uses a mount you set up yourself.

**Caching.** ZeroFS keeps a cache in memory and on local disk, but it's a read cache: data read from the bucket is kept there and served locally the next time. Writes don't go through the disk cache. They are buffered in memory and PUT straight to the bucket, so the cache size doesn't change how much a flush has to upload. A read that misses the cache is a GET from the bucket. This makes it much faster to use than mapping directly to objects in the store.

**Durability.** keepsync makes data durable by syncing the mount and then running `zerofs flush`. Once that returns, everything written before it is in the bucket. The server does this every minute and tracks which blobs were written before a successful flush (its "durability watermark"), and keepsync never expires a file from sync until the server says the file, and everything needed to find it by name, is durable in the bucket.

**Single writer.** ZeroFS is a single-writer system: one process writes to a bucket at a time, and ZeroFS fences out others. It has its own leader/standby design for failover, but keepsync keeps this simple and doesn't use it. Instead, recovery onto a new machine is done from the [recovery kit](design.md#recovery-kit).

keepsync uses ZeroFS as the replication target (or primary store) for the Perkeep server running on the same machine. ZeroFS is a separate AGPL program: keepsync runs an unmodified, pinned release as a subprocess and never links it.

### Server modes: store and cache

You can run the keepsync server in two modes, chosen when you create it:

- **Store mode** (the default). Perkeep uses the local disk on that machine as authoritative storage, which can grow indefinitely, so it's up to you to make sure there's enough disk space. Every blob is also replicated asynchronously to the bucket through ZeroFS. All reads are local, so the ZeroFS cache is kept very small (2 GB, ZeroFS's minimum).
- **Cache mode.** You specify the ZeroFS cache size, and Perkeep uses ZeroFS as its blob store, with no local blob store at all. That makes the cloud copy the authoritative data. This is for when the server has limited or fixed disk and the archive may be far bigger than the machine. The cost is that reading old files fetches them from the bucket.

In both modes the index and Perkeep's identity stay local, and the bucket holds the same layout, so a server can be recovered into either mode.

## tailcat

The last component of the architecture is Tailscale's [tailcat](https://github.com/tailscale/tailcat), which is both a standalone utility and a Go library. It gives you Tailscale-like peer-to-peer WireGuard networking in a userspace network stack, much like Tailscale's own architecture, but without Tailscale's control plane or an account. Peers are identified by their keys, and it uses Tailscale's public DERP servers, or your own, for rendezvous and as a relay when a direct connection can't be made.

**If you know Tailscale:** tailcat is the data plane without the coordination server. There's no tailnet, admin console or login; each program holds a key, and who may connect is decided by the program itself. keepsync's server plays the coordination role for its own nodes by keeping the list of approved node keys.

keepsync uses tailcat to connect the server and nodes, and nodes to each other, in place of Syncthing's own discovery, relay and peer-to-peer stack. It serves the same function there, and it also makes the Perkeep server on the keepsync server reachable from keepsync nodes. So nodes can reach the server, and each other, behind double NATs and similar, with no public IP, open ports or DNS anywhere. If public DERP is too slow for your sync volume (it's rate-limited), run your own `derper`.

## keepsync

keepsync wraps all of this up in a single binary, giving you both peer-to-peer file synchronization and long-term archiving, with the ability to connect the two, in one simple command line interface. The same binary plays two roles.

### Server

One machine runs `keepsync server run`. It holds:

- **Perkeep** (`perkeepd`, embedded), the archive, and the only key that signs claims.
- **ZeroFS**, supervised as a subprocess and flushed every minute.
- **The registry**: the approved nodes, the folders and their members, keep policies, and which nodes are each folder's keepers.

The server is the only machine that touches the bucket. Everything needed to rebuild it from the bucket fits in one encrypted **recovery kit**, and a recovered server comes back at the same tailcat address, so nodes reconnect on their own.

### Nodes

Every machine that syncs folders runs `keepsync node run`, with embedded Syncthing. Joining is one command on the node plus approval on the server. Folders have a name that is the same everywhere and a path that is local to each node.

When a folder is kept, up to two nodes are its **keepers**. Every few minutes a keeper walks the folder, uploads files that have been unmodified for the grace period (48 hours by default) to Perkeep, and asks the server to sign claims putting them in the folder's tree. Each kept version records Syncthing's version vector, so the archive knows which edit came after which, whatever the mtimes say, and a node that is behind can never roll the archive back. If the folder has an expiry window, the keeper removes files past it from sync, but only once they are durable in the bucket. You can `restore` any file, as it is now or as it was at an earlier time.

### Gateway

Each node also provides a gateway to the server's Perkeep on `127.0.0.1:3179`, tunnelled over tailcat. `keepsync gateway config` writes a Perkeep client config for it, so you can use stock Perkeep tools (`pk-get`, `pk-mount`, the web UI) without having to configure them yourself. By default a node can only fetch blobs; `keepsync server grant <node> read` opens search, the web UI and `pk-mount`. Nothing through the gateway can sign or delete.

### Conflict hook

Syncthing leaves conflicts as `.sync-conflict-…` copies. keepsync lets you set a `conflict_hook` script per node or per folder, which can resolve them with a merge tool or an LLM. One node per folder runs it, and gets the original, the conflict copy, the losing node's name and, on a kept folder, the common ancestor from the archive for a three-way merge. On a kept folder both versions are archived before the hook runs, so a bad merge can always be undone with `restore`. If the hook fails, the conflict is left for you, as Syncthing would.

## Further reading

- [README](../README.md): getting started and command reference.
- [Spec](design.md): the exact behaviour of each piece, failure modes and edge cases.
