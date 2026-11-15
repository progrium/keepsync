// Copyright (C) 2021 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// keepsync patch: upstream generates this file from gui/ with go generate,
// and the generated file is not part of the published module, so importing
// lib/syncthing fails to build without -tags noassets. keepsync runs
// Syncthing with the GUI disabled, so this stub serves the same minimal
// assets as noassets.go, without needing the build tag.

//go:build !noassets

package auto

import (
	"bytes"
	"compress/gzip"

	"github.com/syncthing/syncthing/lib/assets"
)

func Assets() map[string]assets.Asset {
	buf := new(bytes.Buffer)
	gw := gzip.NewWriter(buf)
	_, _ = gw.Write([]byte("<html></html>"))
	_ = gw.Flush()
	return map[string]assets.Asset{
		"default/index.html": {
			Gzipped: true,
			Content: buf.String(),
		},
	}
}
