package kit

import (
	"bytes"
	"text/template"
)

var recoverTmpl = template.Must(template.New("RECOVER.md").Parse(`# keepsync recovery kit

Created {{.Created.Format "2006-01-02 15:04 MST"}} for the server
{{- if .ServerAddr}} at ` + "`{{.ServerAddr}}`" + `{{end}}.
Binaries are for {{.Platform}}. ZeroFS is pinned at {{.ZeroFSVersion}}
{{- if not .ZeroFSPinned}} (the bundled zerofs binary did NOT match the pin when this kit was made){{end}}.

This kit can read all your data and impersonate your server. Keep it and its
passphrase apart, offline, and never in the bucket it protects.

## What is in it

- ` + "`bin/keepsync`" + `, ` + "`bin/zerofs`" + `: the programs, for {{.Platform}}.
- ` + "`kit.json`" + `: the bucket ({{.Storage.URL}}{{if .Storage.Endpoint}} at {{.Storage.Endpoint}}{{end}}),
  its credentials and the ZeroFS encryption password.
- ` + "`perkeep/`" + `: the Perkeep identity secret key and the perkeepd config shape.
- ` + "`tailcat/server.key`" + `: the server's tailcat key, so a recovered server
  keeps its address and nodes reconnect without re-init.
- ` + "`server/`" + `: the server's config{{if .HasRegistry}} and registry state{{end}}.
- ` + "`licenses/`" + `: ZeroFS's license (AGPL-3.0) and where its source is.
- ` + "`SHA256SUMS`" + `: checksums of everything above.

## Rebuild the server

1. Make sure the old server is stopped for good. ZeroFS is single-writer:
   two servers on one bucket corrupt it.
2. On the new host, get keepsync. If you have none, decrypt this kit with
   age and unzip it:

       age -d -o kit.zip keepsync-recovery.zip.age
       unzip kit.zip bin/keepsync && sha256sum -c --ignore-missing SHA256SUMS

3. Run, as a user that can mount filesystems:

       bin/keepsync recover keepsync-recovery.zip.age

   It asks for the passphrase and, if the kit holds a read-only token,
   for write credentials to the bucket. It mounts ZeroFS on the bucket,
   starts perkeepd on a fresh index, copies and reindexes every blob, and
   puts the server's keys and config in place.
4. Start the server: ` + "`keepsync server run`" + `. It comes back at the same
   tailcat address, and nodes reconnect on their own.

## Drill

Check the kit works without touching the live server. Flush the live
server, copy the bucket (on R2 or S3, ` + "`rclone copy`" + ` does it server-side
with no egress), then:

    keepsync recover keepsync-recovery.zip.age --drill --bucket s3://<copy>/<prefix>

The drill restores from the copy without taking over the server's
identity, and checks every blob's hash, every kept file's sha256 against
the live server, and the index. Never point --bucket at the live bucket.
`))

func recoverMD(m Manifest) []byte {
	var b bytes.Buffer
	if err := recoverTmpl.Execute(&b, m); err != nil {
		panic(err)
	}
	return b.Bytes()
}
