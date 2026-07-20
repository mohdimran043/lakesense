// Package connectors assembles the default connector registry. Each connector
// registers here exactly once; the CLI and tests consume this registry.
package connectors

import (
	"github.com/lakesense/lakesense/engine/internal/connectors/postgres"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Default returns a registry with every built-in connector.
func Default() *sdk.Registry {
	r := sdk.NewRegistry()
	r.Register(postgres.Type, postgres.New)
	return r
}
