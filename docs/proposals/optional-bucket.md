# Optional cloud replication in keepsync

Nov 18, 2026 · Jeff Lindsay, h.d.a.

## Summary

Making the bucket optional is very doable, because nodes never touch it: the whole change sits on the server. The real question is not plumbing but what "durable" means without a second copy, since that word gates expiry, which is the one place keepsync can lose data.

**Recommendation:** add a bucketless server that is store mode only, with expiry refused until the server has a replica. Build it by turning today's ZeroFS dependency into a general "replica" slot (bucket, none, and later the replica server or a local directory), so a bucketless server can attach a bucket later and backfill it with code that already exists (`repairReplica`). Sync, keeping, history, restore, the gateway, and the conflict hook all work unchanged without a bucket; recovery shrinks to "identity plus your own backup of the blob directory".

Everything below is from reading `keepsync` at `ecc2111` (spec, architecture, `internal/server`, `internal/pkserver`, `internal/keeper`, and `internal/kit`). Nothing was built or run.

## What depends on the bucket today

The bucket is reached only by the server, through ZeroFS. In store mode it is a pure replica (`replicateTo`); in cache mode it is the primary blob store.

| Piece | How it uses the bucket | Where |
| --- | --- | --- |
| Durability watermark | `Durable` means "seen in the replica before a ZeroFS flush that succeeded". Flush runs every `flush_interval` (1m). | `pkserver/durability.go` |
| Expiry | A file leaves sync only once its content plus every claim and permanode naming it are `Durable`. | `keeper/keeper.go` (`allDurable`) |
| Gone marks | A missing file past the window is marked `expired` only if durable, else `deleted`, which changes what a plain `restore` brings back. | `keeper/keeper.go` |
| Keeper re-put loop | After keeping, a keeper re-puts a file until the server says it is durable, to cover ZeroFS restarts. | `keeper` |
| Cache mode | The bucket is the only blob store; the index and an `unflushed/` stash of signed blobs are local. | `pkserver.loadConfig` |
| ZeroFS restart repair | After a restart, `repairReplica` copies every local blob the replica lost; cache mode replays the stash. | `pkserver/durability.go` |
| Clean shutdown | Drain replication, flush, stop ZeroFS: "everything is in the bucket". | `server.shutdown` |
| Registry persistence | Every save is mirrored to `.keepsync/registry.json` on ZeroFS, so recovery has a fresh registry. | `server/store.go` |
| Recovery kit | `kit.json` holds the bucket URL, credentials, and ZeroFS password; `bin/zerofs` is bundled. `Storage.URL` is required. | `kit/kit.go` |
| `recover` and drill | Start ZeroFS on the bucket, copy and reindex every blob, prefer the bucket's registry. The drill needs a bucket copy. | `kit/recover.go` |
| Status and summaries | `zerofs:` health line, last flush or flush error, bucket bytes and object count (ListObjectsV2 every 10 min), all also in each node's server summary. | `server/usage.go`, `server/ctl.go` |
| Init and config | `server init` requires `--bucket`, generates the ZeroFS password, and downloads the pinned ZeroFS. `server.toml` has `[storage]` and `[zerofs]`. | `server.Init`, `config` |
| Licensing | ZeroFS is AGPL, kept as an unmodified subprocess; the kit carries its license. | spec, Licensing |

**What does not touch the bucket:** nodes, Syncthing, tailcat, signing, kept trees, `History` and `restore` (served from perkeepd), the gateway and scopes, the conflict hook, and keeper assignment. Multi-node operation is therefore unaffected, apart from what status shows and when expiry fires. Node health even pings with `Durable(nil)`, which has nothing to do with the bucket.

## What breaks or changes without it

1. **Expiry loses its safety argument.** Today a file leaves every node only after a second, off-site copy exists. Without a bucket the best "durable" can mean is "fsynced on the server's one disk". Expire with that rule and the server's disk becomes the only copy of everything past the window. That breaks the spirit of the spec's first goal: "expiry never loses a file".
2. **Cache mode has no meaning.** With no bucket there is nothing to cache; a bucketless server is store mode by definition.
3. **Recovery shrinks.** The kit can still restore the identity, tailcat key (same address), config, and registry. The blobs must come from something the user runs: a copy of `server/perkeep/blobs` (rsync, restic, ZFS snapshots, a NAS). `recover` needs a "blobs from this directory" source in place of "blobs from this bucket", and the drill needs the same.
4. **Registry freshness on recovery gets worse.** No bucket mirror means the fallback is the kit's copy, which is as old as the last `server kit`. A cheap fix: write the mirror into the blob directory (or next to it) so a backup of that directory carries it.
5. **Server disk becomes the archive's ceiling.** Store mode already grows without bound locally; it just now has no escape hatch to cache mode.
6. **Status needs an honest "no replica" state.** The `zerofs:` line, last flush, bucket size, and the nodes' server summary should say "replication off" rather than show a failing or empty bucket.
7. **Things that simply go away:** the flush ticker, ZeroFS supervision and restart repair, the stash, drain on stop, the single-writer caveat, the ZeroFS download, and the AGPL binary in the kit.
8. **Nodes are unchanged.** Sync, keeping, history, `restore --at`, the gateway, and the conflict hook all behave the same. The keeper's re-put loop becomes a no-op once `Durable` answers immediately.

## Design options

The options differ mainly in what `Durable` means without a bucket, and so whether expiry is allowed.

| Option | `Durable` means | Expiry | Effort | Main risk |
| --- | --- | --- | --- | --- |
| A. Local-only, durable on local fsync | Blob is in the server's local store | Allowed | Small | The server's one disk is the only copy of expired files; silent weakening of the core promise |
| B. Local-only, no replica (recommended first step) | "No replica": an explicit answer, never true | Refused at `keep --expire` while no replica; existing policies with expiry block startup or pause with a clear message | Small to medium | Users who want expiry must add a replica; that is the point |
| C. Pluggable replica slot | "In the replica, after a replica flush", for any replica: bucket (ZeroFS), local or NAS directory, or the planned replica server over tailcat | Allowed whenever a replica exists | Medium; mostly a refactor of `api.ZeroFS` into `api.Replica` | More configurations to test |
| D. Opt-in expiry without replica (`--expire-unsafe`) | Same as A, but only for folders that ask | Per folder, explicit | Small on top of B | A footgun with a name on it |

**Why not A:** it is the easiest and the most dangerous, because nothing in the UI would tell a user that `--expire 30d` now deletes the only off-server copy.

**Why C is the right shape:** almost every bucket touch point already goes through three calls, `Flush`, `Dir`/9P access, and the restart counter, and `repairReplica` already backfills a replica from the local store. A "none" replica, a "local directory" replica (Perkeep `localdisk` with fsync, so flush is a no-op), and the "replica server mode" in the spec's Not built yet list all fit the same slot. That also gives a clean answer to "add a bucket later": attach it, let `repairReplica` copy everything, and `Durable` starts answering true after the first flush.

## Recommended design and touch points

Ship B on the shape of C: a `replica` setting that is `bucket` (today, the default) or `none`, with expiry refused while it is `none`.

**Requirement (Jeff, Nov 18): adding a bucket later must work.** This design meets it because a bucketless server keeps exactly the store-mode local layout, so attaching a bucket is store mode's normal setup plus a one-time backfill: start ZeroFS, add `replicateTo`, run `repairReplica` to copy every existing blob, flush, mirror the registry, and regenerate the kit. Two rules keep that path open: the bucketless server must never use a layout or blob store that store mode can't replicate from, and `server replica add` gets an e2e test from day one.

**User-facing**

- `server init` without `--bucket` (or with `--no-bucket`, if an explicit flag reads better) creates a store-mode server with `replica = "none"`. `--mode cache` without a bucket is rejected.
- `keep --expire` on a server with no replica fails with: no replica, so expiry would leave the server's disk as the only copy; add one with `server replica add`.
- `server replica add --bucket ...` later writes `[storage]` and `[zerofs]`, starts ZeroFS, and backfills with `repairReplica`. Expiry is allowed from then on.
- `server status` and node summaries show `replica: none` in place of the `zerofs:`, flush, and bucket lines.
- The README and spec say plainly that a bucketless server's archive is as safe as the backups of its directory.

**Code**

1. `api.ZeroFS` becomes `api.Replica` (Start, Flush, Stop, Health, Restarts, access for the blob store). `zerofs.Supervisor` implements it; a `noReplica` type does too.
2. `pkserver.Start`: allow no replica. `HighLevelConfig` drops the `zerofs` storage and `replicateTo`; `Flush` and `Drain` become no-ops; `Durable` returns a distinct `ErrNoReplica` (or all false with a reason) rather than lying.
3. `keeper`: treat `ErrNoReplica` as "expiry off", not "server unreachable", so `keep status` says why and gone marks fall back to `deleted` only where today's rules would.
4. `registry`: reject a policy with an expire window when the server has no replica; on startup, a stored policy with expiry on a bucketless server logs and pauses expiry.
5. `server`: skip the flush ticker, ZeroFS lookup and download, the bucket mirror (write it into the server dir's backup area instead), and bucket usage polling.
6. `config` and `server.Init`: `[storage]` optional; `Validate` requires store mode when it is absent.
7. `kit`: `Storage` optional; no `bin/zerofs` or ZeroFS license when there is none. `recover --blobs <dir>` copies and reindexes from a directory, reusing the store-mode copy path. The drill takes the same source.
8. Tests: fakes already have a directory-backed `fake.ZeroFS`; add a no-replica fake, unit tests for the refusal paths, and an e2e step running a bucketless server through keep and restore, then `replica add`.

Rough size: most of the work is items 1, 2, and 7; the rest is guards and status text. Docs (README, architecture, and spec) need a short "Running without a bucket" section, with the Oxford comma per the repo's CLAUDE.md.

## Open questions

- Should D (`--expire-unsafe`) exist at all? I lean no; it is easy to add later if someone asks.
- Removing a bucket from an existing server (bucket to none) is harder than adding one: expired files may live only in the bucket in cache mode. Out of scope unless wanted; store mode could allow it after a warning.
- Does perkeepd's sync handler backfill on its own when `replicateTo` is added to a server that already has blobs? If not, `repairReplica` covers it, but this needs a test against real Perkeep.
- Where should the registry mirror go without a bucket, so a user's backup of the server directory always carries a fresh copy?
