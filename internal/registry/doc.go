// Package registry implements the registry server and api.RegistryClient.
//
// Server holds the approved nodes (the allow list), folders, keep policies
// and keeper assignments, persisted through a Store before every change is
// published. It serves nodes on api.PortRegistry and forwards path claims
// and durability questions to the api.Signer and api.Durability it is given.
// Client is the node side, speaking the wire protocol described in wire.go.
//
// Keepers: each kept folder gets up to two keepers, chosen from its
// connected members (send-receive before receive-only). A keeper that has
// been disconnected for Config.KeeperTimeout is replaced when another
// connected member can take over. Keepers[0] runs the conflict hook and
// stays first while it is healthy.
//
// See docs/design.md.
package registry
