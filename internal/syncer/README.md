# internal/syncer: embedding Syncthing

`syncer.Syncer` implements `api.Syncer` by running Syncthing v2 in-process
through `lib/syncthing`, the same entry point `cmd/syncthing` uses. Peers and
folders come from code (`SetPeers`, `SetFolders`); nothing is read from a
Syncthing config file at startup.

## How it is wired

- **Identity.** `cert.pem`/`key.pem` in `Config.Dir`, created on first use
  (`syncthing.LoadOrGenerateCertificate`). The device ID is stable across
  restarts and known as soon as `New` returns.
- **Config.** Built fresh on every `Start` from `config.New` plus fixed
  options: global and local discovery, relays, NAT, usage reporting, crash
  reporting, upgrades and the GUI are off; the only listen address is
  `tcp://127.0.0.1:<free port>`. Every `SetPeers`/`SetFolders` rebuilds the
  device and folder lists and applies them with `config.Wrapper.Modify`,
  waiting until Syncthing has taken the change, then saves `config.xml` in
  `Dir`.
- **Restarts.** `Start` restores the previous run's folders and peer device
  IDs from `config.xml`, unless `SetFolders`/`SetPeers` were already called.
  This matters: Syncthing drops the index of any folder missing from its
  config at startup, and a dropped index means every file is rescanned as a
  new version, so a delete on another node (expiry) comes back as a conflict
  copy. With the index kept (`Dir/index-v2`), version vectors survive.
  Restored peers are not dialed until `SetPeers` gives their addresses. The
  device ID comes from `cert.pem`; the folder ID is whatever the caller
  passes (the registry's `Folder.ID`), so it must be stable too.
- **Database.** `syncthing.OpenDatabase(Dir/index-v2)`, SQLite. With cgo it
  uses mattn/go-sqlite3, without cgo modernc.org/sqlite; both pass the tests.
- **Transport bridge.** Inbound: the `api.PortSyncthing` handler checks the
  caller is a current peer's key, dials Syncthing's loopback listener and
  splices. Outbound: each peer gets its own loopback listener, which is that
  device's only address in Syncthing's config; an accepted connection is
  `Transport.Dial`ed to the peer's tailcat address. BEP's TLS runs end to end
  over the splice, so Syncthing still checks device IDs.
- **Allow list.** `SetPeers` calls `Transport.Allow` for peer keys and
  `Disallow` for keys it allowed earlier and that are no longer peers. It
  never touches keys it did not add (such as the server's).
- **Events.** One subscription to `ItemFinished`, `LocalChangeDetected`,
  `DeviceConnected` and `DeviceDisconnected`, translated to `api.SyncEvent`.
  Only files produce item events (not directories or symlinks); deletes are
  included, so consumers should stat. A path that is a conflict copy also
  produces a `SyncConflict` event, whose `Device` is the copy's `ModifiedBy`:
  the device whose losing version the copy holds, since that node creates
  the copy. The short ID in the copy's name is the winner's (Syncthing names
  it after the incoming version's `ModifiedBy`). For
  `SyncItemChanged`, `Device` is the global file's `ModifiedBy`.
  Events are hints: Syncthing drops events for slow subscribers.
- **Scan.** `Internals.ScanFolderSubdirs`, which returns when the scan is done.

## Syncthing internals touched

| What | Package | Stability |
| --- | --- | --- |
| `syncthing.New`, `App.Start/Stop`, `OpenDatabase`, `LoadOrGenerateCertificate` | `lib/syncthing` | Public entry point used by `cmd/syncthing`; changed signature between v1 and v2 (LevelDB backend replaced by `db.DB`). |
| `App.Internals.ScanFolderSubdirs`, `GlobalFileInfo` | `lib/syncthing` | Documented as unstable ("the interface in Internals is unstable"). |
| `config.New`, `config.Wrap`, `Wrapper.Modify`, `Configuration` fields | `lib/config` | Exported and used by the REST API; option fields come and go between releases. |
| `events.NewLogger`, `Subscribe`, event data maps | `lib/events` | Event payloads are untyped maps; keys match the documented REST event API. |
| `protocol.DeviceID`, `ShortID` | `lib/protocol` | Stable. |
| `svcutil.ExitSuccess` | `lib/svcutil` | Minor. |

Not touched yet: `lib/connections`. The native tailcat dialer/listener
(milestone 6) needs it, and its dialer and listener registries are
unexported, so that step will need a patch to the vendored copy.

## Why a vendored Syncthing

`go.mod` replaces `github.com/syncthing/syncthing` with
`third_party/syncthing`, a pruned copy of Syncthing at a fixed commit. Two
reasons:

1. **The published module does not build as a library.** `lib/syncthing`
   imports `lib/api`, which needs `lib/api/auto.Assets()`. Upstream generates
   that file (`gui.files.go`) from `gui/` with `go generate`, and the
   generated file is not in the module, so a plain `go build` fails unless
   every build passes `-tags noassets`. Build tags cannot be set in
   `go.mod`, so the vendored copy adds a stub `gui.files.go` instead (the GUI
   is disabled anyway). That is the only patch; see
   `third_party/syncthing/KEEPSYNC.md`.
2. **v2 has no importable module version.** v2 tags keep the module path
   `github.com/syncthing/syncthing` without `/v2`, so the Go proxy only
   offers v2 as a pseudo-version (`v1.30.0-rc.1.0.<date>-<hash>`).

The v2 code requires Go 1.26.2 or newer (keepsync's `go.mod` is already
past that).

## Things for the wiring (workstream G)

- Syncthing logs through the process-wide `slog` default at INFO, so set the
  level there.
- On startup Syncthing runs a short hashing benchmark (~0.5s CPU) when INFO
  logging is enabled.
- Two `Syncer`s in one process work (the tests do it), but Syncthing keeps
  some process-wide state: Prometheus metrics labelled by folder ID, and
  `lib/locations` defaults (only used for a protected-file list and a
  free-disk check, which is disabled here).
