package pkserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"perkeep.org/pkg/auth"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/client"
	"perkeep.org/pkg/jsonsign"
	"perkeep.org/pkg/schema"
	"perkeep.org/pkg/serverinit"

	"github.com/progrium/keepsync/internal/api"
)

// File names inside Config.Dir.
const (
	secretRingFile = "identity-secring.gpg"
	identityFile   = "identity.json"
	blobsDir       = "blobs"
	indexDir       = "index.leveldb"
)

// replicaName is the name of the diskpacked storage on ZeroFS in the
// perkeepd config. Its handler is served at /sto-<replicaName>/.
const replicaName = "zerofs"

// baseURL is the URL perkeepd believes it is served at. Every connection is
// in-process or over tailcat, so the host is never resolved.
const baseURL = "http://perkeep.keepsync"

// Storage modes (Config.Mode).
const (
	// ModeStore keeps every blob on local disk (packRelated) and replicates
	// it asynchronously to diskpacked on ZeroFS. Reads are local.
	ModeStore = "store"
	// ModeCache has no local blob storage: perkeepd's primary storage is
	// the diskpacked store on ZeroFS, so local disk holds only ZeroFS's
	// cache. The index, the identity and perkeepd's small queues and
	// caches stay in Dir. The bucket holds the same diskpacked layout in
	// both modes.
	ModeCache = "cache"
)

// Config configures a Server.
type Config struct {
	// Mode is ModeStore (the default, also for "") or ModeCache.
	Mode string
	// Dir holds the identity, the local blob storage and the index. It must
	// not be on ZeroFS.
	Dir string
	// ZeroFS backs the diskpacked replica. Its Dir must already exist when
	// Start is called (Start does not start ZeroFS), and Flush is called
	// by Server.Flush to advance the durability watermark.
	ZeroFS api.ZeroFS
	// Transport, if set, serves perkeepd to nodes on api.PortPerkeep, each
	// within its scope (see Scope).
	Transport api.Transport
	// Scope, if set, returns a node's scope, ScopeBlobs or ScopeRead, and
	// anything else for a node that may do nothing (registry.Server.Scope
	// fits). It is asked on every request, so a grant applies at once.
	// With the blobs scope a node may upload, stat and fetch blobs; read
	// adds search, enumeration, the web UI, status and help. No scope may
	// sign, remove blobs or reach the debug handlers. Nil gives every
	// node the blobs scope.
	Scope func(peer api.NodeKey) string
	// Logf, if set, receives log lines. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Identity is the server's Perkeep signing identity.
type Identity struct {
	KeyID string `json:"keyID"`
	// PublicKeyRef is the blobref of the armored public key.
	PublicKeyRef string `json:"publicKeyRef"`
}

// Init creates the server's Perkeep identity in dir if there is none yet,
// and returns it. It is safe to call on an existing dir.
func Init(dir string) (Identity, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return Identity{}, err
	}
	ring := filepath.Join(dir, secretRingFile)
	keyID, err := jsonsign.KeyIdFromRing(ring)
	if errors.Is(err, os.ErrNotExist) {
		keyID, err = jsonsign.GenerateNewSecRing(ring)
	}
	if err != nil {
		return Identity{}, fmt.Errorf("pkserver: identity: %w", err)
	}
	ent, err := jsonsign.EntityFromSecring(keyID, ring)
	if err != nil {
		return Identity{}, fmt.Errorf("pkserver: identity: %w", err)
	}
	armored, err := jsonsign.ArmoredPublicKey(ent)
	if err != nil {
		return Identity{}, fmt.Errorf("pkserver: identity: %w", err)
	}
	id := Identity{KeyID: keyID, PublicKeyRef: blob.RefFromString(armored).String()}
	b, _ := json.MarshalIndent(id, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, identityFile), append(b, '\n'), 0o600); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// SecretRingPath is the path of the identity's secret key ring in dir, for
// the recovery kit.
func SecretRingPath(dir string) string { return filepath.Join(dir, secretRingFile) }

// HighLevelConfig returns the perkeepd high-level config for a server whose
// data is in dir and whose diskpacked replica is in replicaDir. It follows
// the proof of concept: packRelated local storage, replicated asynchronously
// to diskpacked on ZeroFS.
func HighLevelConfig(dir, replicaDir, keyID, authSpec string) map[string]any {
	return map[string]any{
		"listen":             "localhost:3179", // never listened on; see Server
		"baseURL":            baseURL,
		"auth":               authSpec,
		"identity":           keyID,
		"identitySecretRing": filepath.Join(dir, secretRingFile),
		"blobPath":           filepath.Join(dir, blobsDir),
		"packRelated":        true,
		"levelDB":            filepath.Join(dir, indexDir),
		"storage": map[string]any{
			replicaName: map[string]any{"type": "diskpacked", "dir": replicaDir},
		},
		"replicateTo": []string{replicaName},
	}
}

// Server is an embedded perkeepd plus the keepsync signer and durability
// watermark. It implements api.Signer and api.Durability.
type Server struct {
	cfg        Config
	logf       func(string, ...any)
	signer     *schema.Signer
	pubKey     string
	pubRef     blob.Ref
	roots      roots // handler prefixes, for node scopes
	ln         *pipeListener
	hs         *http.Server
	shutdown   interface{ Close() error }
	owner      *http.Client   // owner access to any handler
	bs         *client.Client // the primary blob storage, with owner access
	replica    *client.Client // the blob store on ZeroFS
	layout     string         // LayoutDiskpacked or LayoutFiles
	unregister func()         // drops the registered 9P VFS, if any

	claimMu   sync.Mutex       // serializes signing, so the mtime rule holds
	lastClaim time.Time        // date of the last claim signed; guarded by claimMu
	signed    int              // blobs signed, for tests; guarded by claimMu
	now       func() time.Time // dates claims; nil means time.Now (tests set it)

	treeMu sync.Mutex
	trees  map[api.BlobRef]*treeNode // cached kept trees, by root permanode

	flushMu sync.Mutex // serializes Flush
	wm      watermark
}

var (
	_ api.Signer     = (*Server)(nil)
	_ api.Durability = (*Server)(nil)
)

// Start loads the identity created by Init, starts perkeepd in-process and,
// if cfg.Transport is set, serves it on api.PortPerkeep.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.ZeroFS == nil {
		return nil, errors.New("pkserver: Config.ZeroFS is required")
	}
	s := &Server{cfg: cfg, logf: cfg.Logf, trees: map[api.BlobRef]*treeNode{}}
	if s.logf == nil {
		s.logf = log.Printf
	}
	s.wm.init()
	hermetic(cfg.Dir)
	id, err := readIdentity(cfg.Dir)
	if err != nil {
		return nil, err
	}
	ent, err := jsonsign.EntityFromSecring(id.KeyID, SecretRingPath(cfg.Dir))
	if err != nil {
		return nil, fmt.Errorf("pkserver: %w", err)
	}
	s.pubKey, err = jsonsign.ArmoredPublicKey(ent)
	if err != nil {
		return nil, fmt.Errorf("pkserver: %w", err)
	}
	s.pubRef = blob.RefFromString(s.pubKey)
	s.signer, err = schema.NewSigner(s.pubRef, strings.NewReader(s.pubKey), ent)
	if err != nil {
		return nil, fmt.Errorf("pkserver: %w", err)
	}

	// Nothing listens on a real port, and every connection carries its
	// grant (see grantFor), so the auth modes only matter to requests
	// that somehow arrive without one: give those a password nobody has.
	pw := make([]byte, 16)
	rand.Read(pw)
	access := accessOf(cfg.ZeroFS, cfg.ZeroFS.Dir())
	if s.layout, err = detectLayout(access); err != nil {
		return nil, err
	}
	var vfsKey string
	if access.vfs != nil {
		vfsKey, s.unregister = registerVFS(access.vfs)
	}
	pkconf, err := loadConfig(cfg.Mode, cfg.Dir, access, s.layout, vfsKey, id.KeyID, "userpass:keepsync:"+hex.EncodeToString(pw))
	if err != nil {
		s.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	s.shutdown, err = pkconf.InstallHandlers(mux, baseURL)
	if err != nil {
		return nil, fmt.Errorf("pkserver: starting perkeepd: %w", err)
	}

	s.ln = newPipeListener()
	s.hs = &http.Server{
		Handler: s.filter(mux),
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if gc, ok := c.(*grantConn); ok && gc.peer != "" {
				ctx = context.WithValue(ctx, peerKeyCtx{}, gc.peer)
			}
			return auth.GrantCtxKey.WithValue(ctx, grantFor(c))
		},
		ErrorLog: log.New(logWriter{s.logf}, "pkserver: http: ", 0),
	}
	go s.hs.Serve(s.ln)

	s.owner = &http.Client{Transport: s.Transport(auth.OpAll)}
	s.bs, err = s.newClient("")
	if err != nil {
		s.Close()
		return nil, err
	}
	if s.cacheMode() {
		// The primary storage is the blob store on ZeroFS.
		s.replica = s.bs
	} else {
		s.replica, err = s.newClient("/sto-" + replicaName + "/")
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	if err := pkconf.UploadPublicKey(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("pkserver: uploading public key: %w", err)
	}
	if err := s.loadRoots(ctx); err != nil {
		s.Close()
		return nil, err
	}
	if cfg.Transport != nil {
		cfg.Transport.Handle(api.PortPerkeep, func(conn net.Conn, peer api.NodeKey) {
			s.ln.push(&grantConn{Conn: conn, grant: nodeGrant, peer: peer})
		})
	}
	return s, nil
}

func (s *Server) cacheMode() bool { return s.cfg.Mode == ModeCache }

// CheckMode reports an unknown mode.
func CheckMode(mode string) error {
	switch mode {
	case "", ModeStore, ModeCache:
		return nil
	}
	return fmt.Errorf("pkserver: unknown mode %q (want %s or %s)", mode, ModeStore, ModeCache)
}

// loadConfig builds the perkeepd config for mode. Store mode is
// HighLevelConfig as is. Cache mode starts from the same config's low-level
// form and makes the blob store on ZeroFS the primary blob storage (/bs/),
// dropping the local blob storage and the replication to ZeroFS.
// Everything else that genconfig puts in the local blob dir (the index
// sync queue, the thumbnail cache and its metadata, the importer cache)
// stays local, so ZeroFS sees only blob writes and the blob store's own
// files. In LayoutFiles the store on ZeroFS is swapped for a files store,
// through the mount or over 9P (vfsKey).
func loadConfig(mode, dir string, a zeroFSAccess, layout, vfsKey, keyID, authSpec string) (*serverinit.Config, error) {
	if err := CheckMode(mode); err != nil {
		return nil, err
	}
	replicaDir := a.dir
	if replicaDir == "" {
		// Over 9P there is no directory; the handler is replaced below
		// before anything opens it.
		replicaDir = filepath.Join(dir, "zerofs-9p")
	}
	raw, err := json.Marshal(HighLevelConfig(dir, replicaDir, keyID, authSpec))
	if err != nil {
		return nil, err
	}
	conf, err := serverinit.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("pkserver: perkeepd config: %w", err)
	}
	if mode != ModeCache && layout == LayoutDiskpacked {
		return conf, nil
	}
	low := conf.LowLevelJSONConfig()
	prefixes, ok := low["prefixes"].(map[string]any)
	if !ok {
		return nil, errors.New("pkserver: perkeepd config has no prefixes")
	}
	sto := "/sto-" + replicaName + "/"
	replica, ok := prefixes[sto].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("pkserver: perkeepd config has no %s", sto)
	}
	if layout != LayoutDiskpacked {
		replica["handler"], replica["handlerArgs"] = replicaHandler(a, layout, vfsKey)
	}
	if mode == ModeCache {
		prefixes["/bs/"] = replica
		for _, p := range []string{sto, "/sync-to-" + replicaName + "/", "/bs-loose/", "/bs-packed/"} {
			delete(prefixes, p)
		}
	}
	delete(low, "_knownkeys")
	raw, err = json.Marshal(low)
	if err != nil {
		return nil, err
	}
	conf, err = serverinit.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("pkserver: perkeepd %s-mode config: %w", mode, err)
	}
	return conf, nil
}

func readIdentity(dir string) (Identity, error) {
	var id Identity
	b, err := os.ReadFile(filepath.Join(dir, identityFile))
	if err != nil {
		return id, fmt.Errorf("pkserver: no identity (run Init first): %w", err)
	}
	if err := json.Unmarshal(b, &id); err != nil {
		return id, fmt.Errorf("pkserver: %s: %w", identityFile, err)
	}
	return id, nil
}

// newClient returns an owner client of the in-process server. path is a
// storage handler prefix, or "" for the server's advertised blob root.
func (s *Server) newClient(path string) (*client.Client, error) {
	var c *client.Client
	var err error
	if path == "" {
		c, err = client.New(client.OptionServer(baseURL), client.OptionNoExternalConfig())
	} else {
		c, err = client.New(client.OptionServer(baseURL + path))
		if err == nil {
			c, err = c.NewPathClient(path)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("pkserver: client: %w", err)
	}
	c.SetHTTPClient(&http.Client{Transport: s.Transport(auth.OpAll)})
	c.Logger = log.New(logWriter{s.logf}, "pkserver: client: ", 0)
	return c, nil
}

// Transport returns an http.RoundTripper to the in-process perkeepd whose
// requests are granted ops (see perkeep.org/pkg/auth). The request URL's
// host is ignored.
func (s *Server) Transport(ops auth.Operation) http.RoundTripper {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return s.ln.dial(ctx, ops)
		},
		ForceAttemptHTTP2: false,
	}
}

// URL is the base URL that requests through Transport should use.
func (s *Server) URL() string { return baseURL }

// PublicKeyRef is the blobref of the server's armored public key.
func (s *Server) PublicKeyRef() api.BlobRef { return api.BlobRef(s.pubRef.String()) }

// Close stops serving and shuts perkeepd down. It does not flush; call Flush
// first for everything to be in the bucket.
func (s *Server) Close() error {
	if s.cfg.Transport != nil {
		s.cfg.Transport.Handle(api.PortPerkeep, func(conn net.Conn, _ api.NodeKey) { conn.Close() })
	}
	var errs []error
	if s.hs != nil {
		errs = append(errs, s.hs.Close())
	}
	if s.shutdown != nil {
		errs = append(errs, s.shutdown.Close())
	}
	if s.unregister != nil {
		s.unregister()
	}
	return errors.Join(errs...)
}

type logWriter struct{ logf func(string, ...any) }

func (w logWriter) Write(p []byte) (int, error) {
	w.logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// hermetic points perkeep's host directories into dir, so the embedded
// perkeepd never reads or writes a user's own Perkeep config, cache or
// state. They are process-wide, so a variable already set is left alone.
func hermetic(dir string) {
	for k, sub := range map[string]string{
		"CAMLI_CONFIG_DIR": "perkeep-config",
		"CAMLI_VAR_DIR":    "perkeep-var",
		"CAMLI_CACHE_DIR":  "perkeep-cache",
	} {
		if os.Getenv(k) == "" {
			os.Setenv(k, filepath.Join(dir, sub))
		}
	}
}
