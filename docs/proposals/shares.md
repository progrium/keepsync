# Shares: short-term access to a folder over HTTP

Nov 20, 2026 · Jeff Lindsay

## Summary

A **share** gives someone outside your nodes, a person or an agent, access to one synced folder or a subpath of it, read-only or read-write, for a limited time. Guests need only an HTTPS URL and a bearer token: `curl` is enough, with nothing to install and no tailcat key.

**Recommendation:** the keepsync server becomes an optional public front door for shares only. It checks each request and forwards it over tailcat to the node that made the share, and that node serves straight from its Syncthing folder. The API is plain HTTP, but the tree listing is dotfs's `.fs/index` format unchanged, and versions follow dotfs's rules, so dotfs clients and 9P over WebSocket can be added later on the same URLs without changing anything a guest already relies on.

This deliberately sits outside Syncthing's model: a guest is not a Syncthing device, and its writes become ordinary local edits on the serving node. That is what makes it short-term sharing rather than a fourth way to sync.

Everything below is from reading `keepsync` at `cf11ed8` and `dotfs` at `219122b`. Nothing was built or run.

## Use cases

1. **An agent works on a project for an afternoon.** `keepsync share docs/acme-proposal --rw --for 4h --label claude`. The agent reads the index, edits files with conditional `PUT`s, and its changes reach your other nodes like any edit. When the share expires its access stops; its writes stay in the folder and, on a kept folder, in the archive.
2. **An agent gets read-only context.** The same, without `--rw`. Read-only is the default.
3. **A person gets a folder for a while.** Same as either of the above. Expiry ends access, but it cannot take back bytes already downloaded; the proposal does not pretend otherwise.
4. **A snapshot as of a time** (later step). `keepsync share docs/taxes --at 2026-04-15`, served by the server from the archive. It can never conflict, and it can include files that have expired from sync.

The guiding constraint is the most locked-down guest we expect: an agent in a cloud sandbox whose only way out is an HTTPS proxy with a host allowlist, with no UDP, no raw TCP, and no tailcat. Such a guest can use a share once the share hostname is on its allowlist, and a path-per-share design means one allowlist entry covers every share.

## User experience

```
$ keepsync share docs/acme-proposal --rw --for 4h --label claude
share 7f3k  docs/acme-proposal  read-write  until 22:50
url:   https://shares.example.com/s/7f3k
token: ks_share_…

$ keepsync share ls
ID    PATH                MODE  LABEL   EXPIRES  CONNECTED  WRITES
7f3k  docs/acme-proposal  rw    claude  22:50    yes        14

$ keepsync share rm 7f3k
```

- `keepsync share <folder>[/<subpath>] [--rw] [--for <duration>] [--label <name>]` runs on a node that has the folder attached. It prints the URL and token once.
- `--for` defaults to a short window (say 1h) and is capped by a server setting.
- `share ls` lists this node's shares (on the server, every share). `share rm <id>` revokes a share at once and drops live requests.
- A share lives in the registry and is served while the node that made it runs `node run`. It survives restarts of either process until it expires or is removed.

## Architecture

```
guest ──HTTPS──▶ keepsync server ──tailcat (port 4480)──▶ node ──▶ Syncthing folder
                 checks token,                            checks again,
                 expiry, mode                             reads and writes files
```

**Server.** Holds share records in the registry: id, folder, subpath, mode, expiry, label, owning node, and a hash of the token. It serves `/s/<id>/...`, rejects anything invalid at the edge, and forwards the request to the owning node over tailcat on a new virtual port (4480 here, any free number will do). A share whose node is offline answers `503`.

**Node.** Accepts share requests only from the server's key. It checks the share again against its copy of the membership, resolves the path inside the share's subtree (never escaping it), and serves from the folder on disk. There is no dotfs server and no second copy of the data.

**Why the server and not the node.** Nodes deliberately have no public address. The server already holds the registry and reaches every node over tailcat, so it can give every share one stable URL without exposing any node.

### Exposure

Off unless configured. One mode per server, in `server.toml`:

```toml
[shares]
expose = "cloudflared"   # or "bind" or "tailscale"
max_duration = "7d"
```

| Mode | How | Notes |
| --- | --- | --- |
| `bind` | Listen on `addr` with a certificate and key you supply | For a server with a public IP or a reverse proxy in front. ACME could come later. |
| `cloudflared` | Run `cloudflared` as a supervised subprocess with a named tunnel token, the way the server already supervises ZeroFS | Stable hostname. Quick tunnels (`trycloudflare.com`) change name on every restart, which breaks links and guests' allowlists, so they are for demos only. |
| `tailscale` | Embed `tsnet` and serve with `ListenFunnel` | In-process Go, no subprocess. Needs a Tailscale account and auth key; Funnel allows only a few ports. |

`server status` shows the mode, the public URL, and whether the front door is up.

## HTTP API

All paths are relative to the share's subtree. Every request carries `Authorization: Bearer <token>`.

| Request | Meaning |
| --- | --- |
| `GET /s/<id>/.fs/index` | The subtree's metadata in dotfs index format: `fsindex 1 seq N`, then one line per path. With `Accept-Encoding: zstd` the body may be zstd, as dotfs's `accept zstd` allows. |
| `GET /s/<id>/<path>` | A file's content. `ETag` is its version. Supports `Range`. |
| `PUT /s/<id>/<path>` with `If-Match: <version>` | Replace a file only if it is still at that version. The reply carries the new version in `ETag`. |
| `PUT /s/<id>/<path>` with `If-None-Match: *` | Create a file only if the path does not exist. |
| `DELETE /s/<id>/<path>` with `If-Match: <version>` | Remove a file only if it is still at that version. |

Outcomes map onto dotfs's ctl replies: success is `200` or `201`, `conflict <version>` is `409` with the current version in `ETag` (or `0` when the path is gone), `missing` is `404`, `denied` is `403`, and `invalid` is `400`. A `PUT` or `DELETE` without a precondition is refused with `428`, so a guest can never overwrite blindly.

A read-only share answers `403` to every write. Parent directories are not created implicitly, matching dotfs's `commit`.

### Versions

dotfs needs versions that only go up and a seq that orders the whole scope. Syncthing already has both:

- A file's **version** is its **local sequence number** in Syncthing's index for that folder (`FileInfo.Sequence`), which Syncthing raises every time this node's copy of the file changes, whether the change was local or pulled from a peer.
- The index's **seq** is the highest local sequence in the folder. Seq is folder-wide even when a share covers a subpath; it still only goes up, which is all dotfs requires.

keepsync already reads Syncthing's local file info through its `Internals.LocalFileInfo` patch (`internal/syncer/filestate.go`), so this needs a small accessor for the sequence and a walk of the local index for the listing, not new Syncthing patches.

Hashes are `-` at first. Syncthing's block hashes are SHA-256 per block, not a BLAKE3 hash of the whole file, so dotfs's `b3:` hashes would mean hashing on demand. The index format allows `-`.

### How a write lands

1. Look up the path's local sequence. If it differs from `If-Match`, answer `409`.
2. Compare the file on disk with what Syncthing last scanned (size and mtime). If they differ, an edit has not been scanned yet, so answer `409` rather than overwrite it.
3. Write the body to a temp file in the folder that Syncthing ignores, fsync, and rename it over the path.
4. Rescan that one path (`Syncer.Scan`, which already exists and waits for the scan), read the new sequence, and return it.

There is a short window between steps 1 and 3 where Syncthing could pull a remote version. Syncthing's own puller checks the file on disk before replacing it, so the race ends as a normal `.sync-conflict-…` copy, which the conflict hook handles as usual. The guest's `PUT` succeeds and the conflict shows up in the index afterwards. That is acceptable for short-term sharing and is called out in the docs.

### Undo and attribution

- On a **kept** folder, creating a read-write share first keeps the shared subtree immediately, skipping grace, the way the conflict hook keeps both sides before it runs. Every file a guest can touch then has a version in the archive from before the share, so `restore --at` can always undo a guest.
- Syncthing's version vectors credit a guest's writes to the serving node. Each share therefore keeps a write log (time, path, operation, old and new version) on that node, shown by `share log <id>` and in `share ls`'s WRITES column.

### Hidden from guests

`.stfolder`, `.stignore`, `.stversions`, `.keepsync`, `.keepignore`, and Syncthing temp files never appear in the index and cannot be read or written. Paths ignored by `.stignore` are hidden too, since Syncthing has no version for them.

## Security

- **Tokens** are random, shown once, and stored only as hashes in the registry. They are scoped to one share; there is no account-wide token.
- **Checked twice.** The server checks the token, expiry, and mode; the node checks the share again, so a bug or stale cache in the front door alone cannot widen access.
- **Expiry is enforced on every request,** so long requests are the only thing that outlives it, and `share rm` cancels those.
- **Subtree escape.** Paths are cleaned and resolved within the subtree; `..`, absolute paths, and symlinks that point outside it are refused.
- **Metadata exposure.** The index shows every name in the subtree at once. That is inherent in a share and is what the guest asked for, but users should share subpaths, not whole folders, when names matter.
- **Blast radius.** A read-write guest's deletes and edits reach every node within seconds. Read-only is the default, `--for` is short by default, and kept folders give an undo.
- **Limits.** Per-share caps on request size, writes per minute, and concurrent requests, with `429` when exceeded.

## Later steps

In rough order:

1. **Events.** `GET /s/<id>/.fs/events?since=N` as server-sent events, using dotfs's event record format, driven by Syncthing's local index updates.
2. **Moves and directories.** `POST /s/<id>/.fs/ctl` with dotfs's one-line `mv`, `mkdir`, and `rm` commands.
3. **Snapshot shares** served by the server from the archive (`--at`), using the same index format with version `0`, as dotfs allows for read-only trees.
4. **9P over WebSocket** at the same URL, for `dotfs sync` and `dotfs mount`.
5. **An MCP endpoint**, a thin layer over the HTTP API, so a share can be added to an agent as a connector by URL.

## Open questions

- Can a read-write guest delete, or should there be a mode that allows only creating and editing?
- What are the default and maximum durations?
- Should keepsync download and pin `cloudflared` the way it does ZeroFS, or expect it on `PATH`?
- Should snapshot shares come before events, since they need no node online and cannot conflict?
- Does a share need its own subdomain ever, for browser use with cookies, or are paths always enough?
