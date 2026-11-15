package pkserver

// The perkeepd handlers and storage types the high-level config uses,
// registered as cmd perkeepd registers them.
import (
	_ "perkeep.org/pkg/blobserver/blobpacked"
	_ "perkeep.org/pkg/blobserver/cond"
	_ "perkeep.org/pkg/blobserver/diskpacked"
	_ "perkeep.org/pkg/blobserver/localdisk"
	_ "perkeep.org/pkg/blobserver/replica"
	_ "perkeep.org/pkg/importer" // the high-level config always has /importer/
	_ "perkeep.org/pkg/search"
	_ "perkeep.org/pkg/server" // sync, status, ui and the other handlers
	_ "perkeep.org/pkg/sorted/kvfile"
	_ "perkeep.org/pkg/sorted/leveldb"
)
