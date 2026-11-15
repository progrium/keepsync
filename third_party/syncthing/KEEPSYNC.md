# Vendored Syncthing

A pruned copy of [Syncthing](https://github.com/syncthing/syncthing)
(MPL-2.0, see LICENSE and AUTHORS) used through a `replace` in keepsync's
`go.mod`. See `internal/syncer/README.md` for why.

- Upstream: tag v2.1.7, commit d6d0181f7d466381c4b39ec8fd944c364f939b32
  (module version `v1.30.0-rc.1.0.20261020195438-d6d0181f7d46`).
- Contents: only the packages `internal/syncer` imports (`go list -deps`),
  without tests or testdata, plus `go.mod`, `go.sum`, `LICENSE`, `AUTHORS`.

## Patches

1. `lib/api/auto/gui.files.go` (new file): stub assets for builds without
   `-tags noassets`. Upstream generates this file and does not publish it.

Patched files stay under MPL-2.0.

## Updating

```sh
V=<commit>
go mod download -json github.com/syncthing/syncthing@$V   # note Dir
rm -rf third_party/syncthing && mkdir -p third_party/syncthing
# copy each package dir from `go list -deps ./internal/syncer`
# (non-test files, plus internal/db/sqlite/sql), go.mod, go.sum, LICENSE,
# AUTHORS, then reapply the patches above and this file.
go mod tidy && go test ./internal/syncer/
```
