//go:build db2

// This file only compiles under `-tags db2`. It registers the IBM Db2 connector
// via the build-tag-safe `extra` hook so the default CGo-free engine build has
// zero Db2/CGo footprint. See engine/Dockerfile.db2 for the enabling image.
package connectors

import (
	"github.com/lakesense/lakesense/engine/internal/connectors/db2"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func init() {
	extra = append(extra, func(r *sdk.Registry) {
		r.Register(db2.Type, db2.New)
	})
}
