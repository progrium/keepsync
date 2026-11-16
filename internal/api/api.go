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
	"os"
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

// Path is how traffic to a peer travels right now.
type Path struct {
	// Direct is true when packets go straight to the peer over UDP.
	Direct bool `json:"direct"`
	// Endpoint is the peer's ip:port when Direct.
	Endpoint string `json:"endpoint,omitempty"`
	// Relay is the DERP region code carrying the traffic when not Direct.
	Relay string `json:"relay,omitempty"`
}

// PathReporter is an optional Transport method. Path reports how traffic to
// the peer with identity key and address addr travels, using whichever
// tunnel currently carries a connection to it. ok is false when no
// connection is open to the peer or the path cannot be told.
type PathReporter interface {
	Path(ctx context.Context, key NodeKey, addr Addr) (p Path, ok bool)
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
	// Archive is the root permanode of the folder's kept tree. It is set
	// when the folder is first kept and stays after Unkeep, so the archive
	// can still be restored and keeping again continues the same tree.
	Archive BlobRef
}

// Membership is the registry's view of the network, versioned so watchers
// can tell updates apart. Version only orders memberships with the same
// Epoch, which the registry picks afresh each time it starts: a registry
// recovered from an older state may publish lower versions than a node has
// seen, and its membership must still win.
type Membership struct {
	Epoch   string
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
	// Version is the file's Syncthing version. The server accepts a
	// version that descends from the kept one and rejects one the kept
	// version descends from (ClaimRejectedOlder). For concurrent versions,
	// or when either side has none, it falls back to the newer-mtime rule.
	Version Version
	// Mode is the file's permission bits, or 0 if unknown. Restore sets
	// them. A change of Mode alone, with the same content, is kept too.
	Mode os.FileMode
}

// ClaimResult reports what the server did with a PathClaim.
type ClaimResult int

const (
	ClaimWritten       ClaimResult = iota // a new signed claim was written
	ClaimUnchanged                        // Path already pointed at File
	ClaimRejectedOlder                    // Version (or ModTime) not newer than the current entry
)

// GoneReason records why a kept file left the sync folder.
type GoneReason string

const (
	GoneNone    GoneReason = ""        // still in sync, or never marked
	GoneExpired GoneReason = "expired" // a keeper expired it
	GoneDeleted GoneReason = "deleted" // deleted (or renamed away) in sync
)

// GoneClaim asks the server to record that the file at Path left sync.
type GoneClaim struct {
	Permanode BlobRef
	Path      string
	Reason    GoneReason
	// File is the kept content the mark applies to. If the path has been
	// kept with other content since, the mark is ignored.
	File BlobRef
}

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
	// Version is the Syncthing version the content was kept with, if any.
	Version Version
	// Mode is the permission bits the file was kept with, or 0 if they
	// were not recorded (files kept before modes were).
	Mode os.FileMode
	// KeptAt is when the server signed the claim that set the content.
	KeptAt time.Time
	// Gone says why the file left sync, if it did, and GoneAt when that
	// was recorded. Keeping the path again clears it.
	Gone   GoneReason
	GoneAt time.Time
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
	// Unkeep stops keeping a folder: its policy and keepers are cleared.
	// The archive stays, and Folder.Archive still names it.
	Unkeep(ctx context.Context, folder string) error
	ClaimPath(ctx context.Context, c PathClaim) (ClaimResult, error)
	Paths(ctx context.Context, permanode BlobRef) (map[string]PathEntry, error)
	// PathsAt is Paths as of time at: only claims the server signed at or
	// before at count. at is when the server kept a version, not file mtime.
	PathsAt(ctx context.Context, permanode BlobRef, at time.Time) (map[string]PathEntry, error)
	Durable(ctx context.Context, refs []BlobRef) ([]bool, error)
	// MarkGone records why a kept file left sync (see GoneClaim).
	MarkGone(ctx context.Context, g GoneClaim) error
	// History returns every content version kept at path, oldest first.
	History(ctx context.Context, permanode BlobRef, path string) ([]PathEntry, error)
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
	// MarkGone records why a kept file left sync. It writes nothing if the
	// path already has that mark, or now holds other content than g.File.
	MarkGone(ctx context.Context, g GoneClaim) error
	// History returns every content version kept at path, oldest first.
	History(ctx context.Context, permanode BlobRef, path string) ([]PathEntry, error)
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

// FileState is one path as Syncthing knows it.
type FileState struct {
	// Known is false if the global index has no entry for the path.
	Known bool
	// Version is the global (current) version.
	Version Version
	Deleted bool
	Dir     bool
	// Current is true when this node holds the global version: its index
	// entry has the global version and it needs nothing for the path.
	Current bool
}

// FolderSync is how far along one folder's sync is, on this node and on the
// peers it shares the folder with.
type FolderSync struct {
	// State is Syncthing's folder state: idle, scanning, syncing,
	// sync-preparing, scan-waiting, sync-waiting, cleaning, error, ...
	State string `json:"state"`
	// StateSince is when State was entered.
	StateSince time.Time `json:"stateSince,omitzero"`
	// Error is the folder error when State is error.
	Error string `json:"error,omitempty"`
	// Local is this node: what the folder holds and what it still needs.
	Local SyncCompletion `json:"local"`
	// Peers are the other members' progress, as far as this node knows.
	Peers []PeerSync `json:"peers,omitempty"`
}

// SyncCompletion is one device's progress on a folder.
type SyncCompletion struct {
	// Pct is 0-100, weighing bytes and items the way Syncthing does.
	Pct         float64 `json:"pct"`
	GlobalBytes int64   `json:"globalBytes"`
	GlobalItems int     `json:"globalItems"`
	// NeedBytes excludes bytes of partly pulled files already fetched.
	NeedBytes   int64 `json:"needBytes"`
	NeedItems   int   `json:"needItems"`
	NeedDeletes int   `json:"needDeletes"`
}

// UpToDate reports whether nothing is needed.
func (c SyncCompletion) UpToDate() bool {
	return c.NeedBytes == 0 && c.NeedItems == 0 && c.NeedDeletes == 0
}

// PeerSync is a peer's progress on a folder.
type PeerSync struct {
	Device DeviceID `json:"device"`
	// Name is the peer's node name, filled in by the node.
	Name string `json:"name,omitempty"`
	// Connected is whether the peer is connected now, filled in by the node.
	Connected bool `json:"connected"`
	// Known is false until the peer has sent its index for the folder, so
	// its progress cannot be told yet.
	Known bool `json:"known"`
	SyncCompletion
}

// NeedFile is a file this node still needs for a folder.
type NeedFile struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Deleted bool   `json:"deleted,omitempty"`
	Dir     bool   `json:"dir,omitempty"`
	// Stage is "pulling" (in progress), "queued" or "waiting".
	Stage string `json:"stage"`
}

// SyncStatusReporter is an optional Syncer method set for sync progress.
type SyncStatusReporter interface {
	FolderSync(folderID string) (FolderSync, error)
	// NeedFiles lists up to limit files this node still needs, in pull
	// order, and the total number needed.
	NeedFiles(folderID string, limit int) ([]NeedFile, int, error)
}

// Traffic is the data moved to and from a peer over the transport.
type Traffic struct {
	// InRate and OutRate are bytes per second over the last few seconds.
	InRate  float64 `json:"inRate"`
	OutRate float64 `json:"outRate"`
	// InTotal and OutTotal count since the transport started.
	InTotal  int64 `json:"inTotal"`
	OutTotal int64 `json:"outTotal"`
}

// TrafficReporter is an optional Transport method. Traffic reports the
// data moved with the peer whose identity is key, ok false if none ever was.
type TrafficReporter interface {
	Traffic(key NodeKey) (t Traffic, ok bool)
}

// Syncer is the embedded Syncthing instance.
type Syncer interface {
	Start(ctx context.Context) error
	DeviceID() DeviceID
	SetPeers(ctx context.Context, peers []Peer) error
	SetFolders(ctx context.Context, folders []FolderConfig) error
	// Scan rescans one path now instead of waiting for the watcher.
	Scan(ctx context.Context, folderID, subpath string) error
	// ModifiedBy reports the device that last changed path in the global
	// index. For a conflict copy that is the device whose losing version
	// it holds (the copy's name carries the winner's ID instead).
	ModifiedBy(folderID, path string) (DeviceID, bool)
	// FileState reports path in Syncthing's global index and whether this
	// node's copy is the current version.
	FileState(folderID, path string) (FileState, error)
	Events() <-chan SyncEvent
	Stop(ctx context.Context) error
}
