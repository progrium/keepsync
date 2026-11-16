package syncer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/syncthing"

	"github.com/progrium/keepsync/internal/api"
)

// Config configures an embedded Syncthing instance.
type Config struct {
	// Dir holds the device certificate (cert.pem, key.pem), the index
	// database and Syncthing's generated config.xml. It is created if
	// missing. The certificate, and so the device ID, survives restarts.
	Dir string
	// Name is this device's name as peers see it.
	Name string
	// Transport carries BEP traffic on api.PortSyncthing.
	Transport api.Transport

	// RescanInterval is the full-rescan period per folder. Zero means
	// Syncthing's default (1h).
	RescanInterval time.Duration
	// DisableWatcher turns off the filesystem watcher, leaving only
	// periodic rescans and Scan. Tests use it for determinism.
	DisableWatcher bool
	// WatcherDelay is how long the watcher aggregates changes before
	// scanning. Zero means Syncthing's default (10s).
	WatcherDelay time.Duration
	// ReconnectInterval is how often Syncthing redials disconnected peers.
	// Zero means 5s, which is cheap here because every dial is local.
	ReconnectInterval time.Duration
	// EventBuffer is the capacity of the Events channel. Zero means 1024.
	EventBuffer int
	// PeerConnections is how many connections to open to each peer.
	// Zero means Syncthing's default (3).
	PeerConnections int
}

// Syncer is an api.Syncer backed by Syncthing running in-process.
type Syncer struct {
	cfg    Config
	cert   tls.Certificate
	id     protocol.DeviceID
	events chan api.SyncEvent

	mu      sync.Mutex
	peers   map[protocol.DeviceID]api.Peer
	folders []api.FolderConfig
	// Whether SetPeers / SetFolders have been called. Until then, Start
	// restores both from the previous run's config.xml.
	peersSet, foldersSet bool
	bridges              map[protocol.DeviceID]*outBridge
	allowed              map[api.NodeKey]bool
	inbound              map[api.NodeKey]*inbound

	// set by Start
	running    bool
	stopped    bool
	listenAddr string // Syncthing's BEP listener on loopback
	ctx        context.Context
	cancel     context.CancelFunc
	evLogger   events.Logger
	wrapper    config.Wrapper
	app        *syncthing.App
	wg         sync.WaitGroup

	// Read by the event pump without s.mu, so a config change waiting on
	// Syncthing never stalls event delivery.
	appRef atomic.Pointer[syncthing.App]
	shorts atomic.Pointer[map[string]api.DeviceID] // short ID -> device ID
}

func (s *Syncer) serve(f func(context.Context) error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		f(s.ctx)
	}()
}

var _ api.Syncer = (*Syncer)(nil)

// New loads or creates the device certificate in cfg.Dir. Nothing runs until
// Start; DeviceID is valid as soon as New returns.
func New(cfg Config) (*Syncer, error) {
	if cfg.Dir == "" {
		return nil, errors.New("syncer: Dir is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("syncer: Transport is required")
	}
	if cfg.ReconnectInterval == 0 {
		cfg.ReconnectInterval = 5 * time.Second
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = 1024
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	cert, err := syncthing.LoadOrGenerateCertificate(
		filepath.Join(cfg.Dir, "cert.pem"),
		filepath.Join(cfg.Dir, "key.pem"),
	)
	if err != nil {
		return nil, fmt.Errorf("syncer: device certificate: %w", err)
	}
	sy := &Syncer{
		cfg:     cfg,
		cert:    cert,
		id:      protocol.NewDeviceID(cert.Certificate[0]),
		events:  make(chan api.SyncEvent, cfg.EventBuffer),
		peers:   map[protocol.DeviceID]api.Peer{},
		bridges: map[protocol.DeviceID]*outBridge{},
		allowed: map[api.NodeKey]bool{},
		inbound: map[api.NodeKey]*inbound{},
	}
	shorts := map[string]api.DeviceID{sy.id.Short().String(): sy.DeviceID()}
	sy.shorts.Store(&shorts)
	return sy, nil
}

// DeviceID returns this node's Syncthing device ID.
func (s *Syncer) DeviceID() api.DeviceID { return api.DeviceID(s.id.String()) }

// Events delivers sync events. The channel is closed by Stop. Events are
// hints: when the reader falls behind, Syncthing drops events for slow
// subscribers, so consumers must not rely on seeing every change.
func (s *Syncer) Events() <-chan api.SyncEvent { return s.events }

// Start runs Syncthing with discovery, relays, NAT, usage reporting,
// crash reporting, upgrades and the GUI off, listening for BEP on a loopback
// port that the Transport bridges to. Peers and folders set before Start are
// applied now.
func (s *Syncer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running || s.stopped {
		return errors.New("syncer: Start called twice")
	}

	port, err := freeLoopbackPort()
	if err != nil {
		return err
	}
	s.listenAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.evLogger = events.NewLogger()
	s.serve(s.evLogger.Serve)

	s.restoreLocked()

	// Bridges first, so peer devices get their loopback addresses.
	if err := s.syncBridgesLocked(); err != nil {
		s.cancel()
		return err
	}
	stcfg := config.New(s.id)
	s.baseOptions(&stcfg)
	s.buildLocked(&stcfg)
	s.wrapper = config.Wrap(filepath.Join(s.cfg.Dir, "config.xml"), stcfg, s.id, s.evLogger)
	// The wrapper saves config.xml when it exits, so Stop waits for it.
	s.serve(s.wrapper.Serve)

	sdb, err := syncthing.OpenDatabase(filepath.Join(s.cfg.Dir, "index-v2"), 0)
	if err != nil {
		s.cancel()
		return fmt.Errorf("syncer: open database: %w", err)
	}

	// Subscribe before the app starts so early events are not missed.
	sub := s.evLogger.Subscribe(events.ItemFinished | events.LocalChangeDetected |
		events.DeviceConnected | events.DeviceDisconnected)

	app, err := syncthing.New(s.wrapper, sdb, s.evLogger, s.cert, syncthing.Options{NoUpgrade: true})
	if err != nil {
		sub.Unsubscribe()
		sdb.Close()
		s.cancel()
		return err
	}
	if err := app.Start(); err != nil {
		sub.Unsubscribe()
		s.cancel()
		return fmt.Errorf("syncer: start syncthing: %w", err)
	}
	s.app = app
	s.appRef.Store(app)
	s.running = true

	s.cfg.Transport.Handle(api.PortSyncthing, s.handleInbound)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.pumpEvents(sub)
	}()
	return nil
}

// restoreLocked loads the folders and peer devices of the previous run from
// config.xml, unless SetFolders / SetPeers already provided them. Without
// this, Syncthing would start with no folders, drop their index ("Cleaning
// metadata for dropped folder") and, once SetFolders adds them back, rescan
// every file as a new version. A delete elsewhere then comes back as a
// conflict copy instead of propagating. Restored peers have no tailcat
// address or key until SetPeers supplies them, so they are not dialed.
func (s *Syncer) restoreLocked() {
	if s.peersSet && s.foldersSet {
		return
	}
	f, err := os.Open(filepath.Join(s.cfg.Dir, "config.xml"))
	if err != nil {
		return
	}
	defer f.Close()
	prev, _, err := config.ReadXML(f, s.id)
	if err != nil {
		slog.Warn("syncer: ignoring unreadable config.xml", "err", err)
		return
	}
	if !s.peersSet {
		shorts := map[string]api.DeviceID{s.id.Short().String(): s.DeviceID()}
		for _, d := range prev.Devices {
			if d.DeviceID == s.id {
				continue
			}
			s.peers[d.DeviceID] = api.Peer{Name: d.Name, Device: api.DeviceID(d.DeviceID.String())}
			shorts[d.DeviceID.Short().String()] = api.DeviceID(d.DeviceID.String())
		}
		s.shorts.Store(&shorts)
	}
	if !s.foldersSet {
		for _, fc := range prev.Folders {
			f := api.FolderConfig{ID: fc.ID, Path: fc.Path, ReceiveOnly: fc.Type == config.FolderTypeReceiveOnly}
			for _, d := range fc.Devices {
				f.Devices = append(f.Devices, api.DeviceID(d.DeviceID.String()))
			}
			s.folders = append(s.folders, f)
		}
	}
}

// baseOptions sets everything that is fixed for keepsync.
func (s *Syncer) baseOptions(c *config.Configuration) {
	o := &c.Options
	o.RawListenAddresses = []string{"tcp://" + s.listenAddr}
	o.GlobalAnnEnabled = false
	o.RawGlobalAnnServers = nil
	o.LocalAnnEnabled = false
	o.RelaysEnabled = false
	o.NATEnabled = false
	o.AnnounceLANAddresses = false
	o.URAccepted = -1
	o.URSeen = 1 << 30
	o.AutoUpgradeIntervalH = 0
	o.CREnabled = false
	o.MinHomeDiskFree = config.Size{Value: 0, Unit: "%"}
	o.ReconnectIntervalS = max(1, int(s.cfg.ReconnectInterval/time.Second))
	o.StartBrowser = false
	c.GUI.Enabled = false
}

// buildLocked writes the device and folder lists from s.peers and
// s.folders into c.
func (s *Syncer) buildLocked(c *config.Configuration) {
	self := c.Defaults.Device.Copy()
	if cur, _, ok := c.Device(s.id); ok {
		self = cur
	}
	self.DeviceID = s.id
	self.Name = s.cfg.Name
	self.Addresses = []string{"dynamic"}
	devices := []config.DeviceConfiguration{self}

	ids := make([]protocol.DeviceID, 0, len(s.peers))
	for id := range s.peers {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b protocol.DeviceID) int { return a.Compare(b) })
	for _, id := range ids {
		p := s.peers[id]
		d := c.Defaults.Device.Copy()
		d.DeviceID = id
		d.Name = p.Name
		d.Introducer = false
		d.AutoAcceptFolders = false
		d.RawNumConnections = s.cfg.PeerConnections
		if b := s.bridges[id]; b != nil {
			d.Addresses = []string{"tcp://" + b.addr()}
		} else {
			// Not dialable until the bridge exists (before Start).
			d.Addresses = nil
		}
		devices = append(devices, d)
	}
	// Assign rather than call c.SetDevices / c.SetFolders: those only add
	// and replace entries, so a removed peer or folder would stay
	// configured, and a folder removed by folder rm would keep syncing.
	c.Devices = devices

	folders := make([]config.FolderConfiguration, 0, len(s.folders))
	for _, f := range s.folders {
		fc := c.Defaults.Folder.Copy()
		fc.ID = f.ID
		fc.Label = f.ID
		fc.Path = f.Path
		fc.FilesystemType = config.FilesystemTypeBasic
		fc.Type = config.FolderTypeSendReceive
		if f.ReceiveOnly {
			fc.Type = config.FolderTypeReceiveOnly
		}
		fc.FSWatcherEnabled = !s.cfg.DisableWatcher
		if s.cfg.WatcherDelay > 0 {
			fc.FSWatcherDelayS = s.cfg.WatcherDelay.Seconds()
		}
		if s.cfg.RescanInterval > 0 {
			fc.RescanIntervalS = max(1, int(s.cfg.RescanInterval/time.Second))
		}
		fc.Devices = []config.FolderDeviceConfiguration{{DeviceID: s.id}}
		for _, d := range f.Devices {
			id, err := protocol.DeviceIDFromString(string(d))
			if err != nil || id == s.id {
				continue
			}
			// Only share with devices we know, or Syncthing would hold a
			// folder entry for a device it cannot reach.
			if _, ok := s.peers[id]; ok {
				fc.Devices = append(fc.Devices, config.FolderDeviceConfiguration{DeviceID: id})
			}
		}
		folders = append(folders, fc)
	}
	c.Folders = folders
}

// applyLocked pushes the current peers and folders into the running
// Syncthing config and waits for it to take effect.
func (s *Syncer) applyLocked(ctx context.Context) error {
	if !s.running {
		return nil
	}
	w, err := s.wrapper.Modify(func(c *config.Configuration) { s.buildLocked(c) })
	if err != nil {
		return fmt.Errorf("syncer: apply config: %w", err)
	}
	done := make(chan struct{})
	go func() { w.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Save now rather than on the wrapper's timer, so a crash right after
	// a change still restarts with this folder list (see restoreLocked).
	return s.wrapper.Save()
}

// SetPeers replaces the peer set. Each peer's key is allowed on the
// Transport, and keys of removed peers are disallowed again (only keys this
// Syncer allowed itself).
func (s *Syncer) SetPeers(ctx context.Context, peers []api.Peer) error {
	next := map[protocol.DeviceID]api.Peer{}
	for _, p := range peers {
		id, err := protocol.DeviceIDFromString(string(p.Device))
		if err != nil {
			return fmt.Errorf("syncer: peer %q: %w", p.Name, err)
		}
		if id == s.id {
			continue
		}
		next[id] = p
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("syncer: stopped")
	}
	s.peers = next
	s.peersSet = true
	shorts := map[string]api.DeviceID{s.id.Short().String(): s.DeviceID()}
	for id := range next {
		shorts[id.Short().String()] = api.DeviceID(id.String())
	}
	s.shorts.Store(&shorts)

	keep := map[api.NodeKey]bool{}
	for _, p := range next {
		keep[p.Key] = true
	}
	var drop, add []api.NodeKey
	for k := range s.allowed {
		if !keep[k] {
			drop = append(drop, k)
			delete(s.allowed, k)
		}
	}
	for k := range keep {
		if !s.allowed[k] {
			add = append(add, k)
			s.allowed[k] = true
		}
	}
	if len(drop) > 0 {
		s.cfg.Transport.Disallow(drop...)
	}
	if len(add) > 0 {
		s.cfg.Transport.Allow(add...)
	}

	if s.running {
		if err := s.syncBridgesLocked(); err != nil {
			return err
		}
	}
	return s.applyLocked(ctx)
}

// SetFolders replaces the folder set. Folders that disappear stop syncing
// here; their files stay on disk. Devices not in the peer set are ignored.
func (s *Syncer) SetFolders(ctx context.Context, folders []api.FolderConfig) error {
	for _, f := range folders {
		if f.ID == "" || f.Path == "" {
			return fmt.Errorf("syncer: folder needs ID and Path: %+v", f)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("syncer: stopped")
	}
	s.folders = slices.Clone(folders)
	s.foldersSet = true
	return s.applyLocked(ctx)
}

// Scan rescans subpath (relative to the folder; "" for the whole folder)
// and returns once the scan is done.
func (s *Syncer) Scan(ctx context.Context, folderID, subpath string) error {
	app := s.appRef.Load()
	if app == nil {
		return errors.New("syncer: not running")
	}
	errc := make(chan error, 1)
	go func() { errc <- app.Internals.ScanFolderSubdirs(folderID, []string{subpath}) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop shuts Syncthing down, closes the bridges and closes Events.
func (s *Syncer) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	app := s.app
	running := s.running
	s.running = false
	for id, b := range s.bridges {
		b.close()
		delete(s.bridges, id)
	}
	s.mu.Unlock()

	if !running {
		close(s.events)
		return nil
	}

	done := make(chan struct{})
	go func() {
		app.Stop(svcutil.ExitSuccess)
		s.cancel()
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		close(s.events)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func freeLoopbackPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
