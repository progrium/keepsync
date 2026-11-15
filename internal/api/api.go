// Package api holds the types and interfaces shared by keepsync's
// components. Each component package implements one or more of these
// interfaces and depends on the others only through them, so components can
// be built and tested independently (see package fake for test doubles).
//
// See docs/design.md for the behaviour behind each interface.
package api

import (
	"context"
	"net"
	"time"
)

// NodeKey is a tailcat node public key ("nodekey:<hex>").
type NodeKey string

// Addr is a tailcat address ("tc…").
type Addr string

// DeviceID is a Syncthing device ID.
type DeviceID string

// BlobRef is a Perkeep blobref ("sha224-…").
type BlobRef string

// Virtual tailcat ports. Nothing listens on a real port.
const (
	PortRegistry  uint16 = 1
	PortPerkeep   uint16 = 3179
	PortSyncthing uint16 = 22000
)

// Clock is injected wherever timing rules apply (grace, max-defer, expiry).
type Clock interface {
	Now() time.Time
}

// SystemClock is the real clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// Transport is the single tailcat endpoint of a keepsync process. Incoming
// connections are dispatched by virtual port; only allowed keys may connect.
type Transport interface {
	Addr() Addr
	Key() NodeKey
	// Handle registers the handler for a virtual port. peer is the
	// authenticated key of the remote end.
	Handle(port uint16, h func(conn net.Conn, peer NodeKey))
	Dial(ctx context.Context, to Addr, port uint16) (net.Conn, error)
	Allow(keys ...NodeKey)
	Disallow(keys ...NodeKey)
	Close() error
}

// Node is one member of a keepsync network.
type Node struct {
	Name   string
	Key    NodeKey
	Addr   Addr
	Device DeviceID
}

// KeepPolicy marks a folder as kept. Durations are measured from mtime.
// Invariant: Grace < MaxDefer < Expire, with Expire == 0 meaning never expire.
type KeepPolicy struct {
	Permanode BlobRef
	Grace     time.Duration
	MaxDefer  time.Duration
	Expire    time.Duration
	// Keepers are the nodes assigned to keep this folder (up to two).
	// Keepers[0] also runs the conflict hook.
	Keepers []NodeKey
}

// Folder is a synced folder as the registry knows it.
type Folder struct {
	Name        string
	ID          string // Syncthing folder ID, identical on every node
	Members     []NodeKey
	ReceiveOnly []NodeKey
	Keep        *KeepPolicy // nil if not kept
}

// Membership is the registry's view of the network, versioned so watchers
// can tell updates apart.
type Membership struct {
	Version uint64
	Nodes   []Node
	Folders []Folder
}

// PathClaim asks the server to keep File at Path in the kept folder whose
// root permanode is Permanode. Path is slash-separated and relative to the
// folder. The server gives each file and directory its own permanode (see
// "Kept tree layout" in docs/design.md); callers only ever name the root.
type PathClaim struct {
	Permanode BlobRef
	Path      string
	File      BlobRef
	ModTime   time.Time
}

// ClaimResult reports what the server did with a PathClaim.
type ClaimResult int

const (
	ClaimWritten       ClaimResult = iota // a new signed claim was written
	ClaimUnchanged                        // Path already pointed at File
	ClaimRejectedOlder                    // ModTime not newer than the current entry
)

// PathEntry is the current value of one path in a kept tree.
type PathEntry struct {
	File    BlobRef
	ModTime time.Time
	// Claims are the blobs besides File that must be durable for this path
	// to be recoverable by name: the claims that set the file's content and
	// name it in its directory, and the permanodes of the file and of each
	// directory between it and the root, with the claims naming them. Empty
	// means the server could not say, and the path must not be expired.
	Claims []BlobRef
}

// RegistryClient is what a node uses to talk to the server's registry.
type RegistryClient interface {
	Register(ctx context.Context, n Node) (Membership, error)
	// Watch delivers the current membership and every later change until ctx
	// is done.
	Watch(ctx context.Context) (<-chan Membership, error)
	AddFolder(ctx context.Context, name string) (Folder, error)
	JoinFolder(ctx context.Context, name string) (Folder, error)
	// LeaveFolder removes the calling node from a folder's members (and its
	// keepers, triggering reassignment). Files on disk are untouched.
	LeaveFolder(ctx context.Context, name string) error
	// Keep marks a folder kept, creating its permanode on first use.
	Keep(ctx context.Context, folder string, p KeepPolicy) (BlobRef, error)
	ClaimPath(ctx context.Context, c PathClaim) (ClaimResult, error)
	Paths(ctx context.Context, permanode BlobRef) (map[string]PathEntry, error)
	// PathsAt is Paths as of time at: only claims the server signed at or
	// before at count. at is when the server kept a version, not file mtime.
	PathsAt(ctx context.Context, permanode BlobRef, at time.Time) (map[string]PathEntry, error)
	Durable(ctx context.Context, refs []BlobRef) ([]bool, error)
}

// Signer is the server-side Perkeep identity: the only holder of the key.
type Signer interface {
	NewPermanode(ctx context.Context, title string) (BlobRef, error)
	// ClaimPath writes nothing if the path already points at File, and
	// rejects a claim whose ModTime is not newer than the current entry.
	ClaimPath(ctx context.Context, c PathClaim) (ClaimResult, error)
	Paths(ctx context.Context, permanode BlobRef) (map[string]PathEntry, error)
	// PathsAt is Paths counting only claims signed at or before at.
	PathsAt(ctx context.Context, permanode BlobRef, at time.Time) (map[string]PathEntry, error)
}

// Durability answers whether blobs have reached the bucket.
type Durability interface {
	// Durable reports, for each ref, whether it and every blob it
	// references are in the bucket.
	Durable(ctx context.Context, refs []BlobRef) ([]bool, error)
}

// BlobPutter uploads a local file to perkeepd and returns its file schema
// blobref. Blobs the server already has are not re-sent.
type BlobPutter interface {
	PutFile(ctx context.Context, path string) (BlobRef, error)
}

// ZeroFS supervises the ZeroFS process backing the diskpacked replica.
type ZeroFS interface {
	Start(ctx context.Context) error
	// Dir is the directory the diskpacked replica writes to.
	Dir() string
	// Flush returns nil only once everything written so far is in the bucket.
	Flush(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Peer is a remote Syncthing device reachable over the Transport.
type Peer struct {
	Name   string
	Key    NodeKey
	Addr   Addr
	Device DeviceID
}

// FolderConfig is a Syncthing folder as configured on this node.
type FolderConfig struct {
	ID          string
	Path        string
	Devices     []DeviceID
	ReceiveOnly bool
}

// SyncEventType classifies events from the Syncer.
type SyncEventType int

const (
	SyncItemChanged SyncEventType = iota // a file finished syncing in from a peer
	SyncLocalChange                      // a local change was detected
	SyncConflict                         // a .sync-conflict copy appeared
	SyncPeerConnected
	SyncPeerDisconnected
)

// SyncEvent is one event from the Syncer.
type SyncEvent struct {
	Type   SyncEventType
	Folder string
	Path   string // relative to the folder; for SyncConflict, the conflict copy
	Device DeviceID
	Time   time.Time
}

// Syncer is the embedded Syncthing instance.
type Syncer interface {
	Start(ctx context.Context) error
	DeviceID() DeviceID
	SetPeers(ctx context.Context, peers []Peer) error
	SetFolders(ctx context.Context, folders []FolderConfig) error
	// Scan rescans one path now instead of waiting for the watcher.
	Scan(ctx context.Context, folderID, subpath string) error
	Events() <-chan SyncEvent
	Stop(ctx context.Context) error
}
