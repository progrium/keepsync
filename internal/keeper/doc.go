// Package keeper runs the keep loop and expiry for kept folders.
// See docs/design.md, "Keeping and expiry".
//
// A Keeper owns one kept folder on one node. Each Pass walks the folder
// (skipping Syncthing's files, .keepsync, the ignore files and anything
// matched by .stignore or .keepignore), puts every file that has been
// unmodified for the grace period through api.BlobPutter, and asks the
// registry to set camliPath:<path> to the resulting fileref. A file that
// never settles is kept anyway once its oldest unkept version reaches
// max-defer, and again each time it reaches the cap. A missing file is never
// turned into a delete.
//
// When the policy has an Expire window, a Pass then removes files that are
// unmodified for that long, but only if the file's current fileref is in the
// kept tree and the server reports it durable. If the server cannot answer,
// expiry pauses and nothing is removed.
//
// Keep implements `keepsync keep` (permanode plus .keepsync), Status
// implements `keepsync keep status`, and KeepNow keeps paths immediately for
// the conflict hook.
//
// Limits: max-defer is tracked in memory, so after a restart a never-settling
// file's cap is measured from the first version seen after the restart. The
// built-in ignore matcher covers common .stignore syntax; wiring can pass
// Syncthing's own matcher as Config.Exclude.
package keeper
