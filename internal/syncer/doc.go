// Package syncer embeds Syncthing (api.Syncer) as a library, in-process.
//
// Syncthing listens for BEP on a loopback port, and a small bridge carries
// connections between that port and the tailcat Transport on
// api.PortSyncthing (the "localhost bridge" from docs/design.md). See
// README.md in this directory for which Syncthing internals this touches.
package syncer
