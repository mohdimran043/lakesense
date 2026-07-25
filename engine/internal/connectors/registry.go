// Package connectors assembles the default connector registry. Each connector
// registers here exactly once; the CLI and tests consume this registry.
package connectors

import (
	"github.com/lakesense/lakesense/engine/internal/connectors/cassandra"
	"github.com/lakesense/lakesense/engine/internal/connectors/clickhouse"
	"github.com/lakesense/lakesense/engine/internal/connectors/dynamodb"
	"github.com/lakesense/lakesense/engine/internal/connectors/elasticsearch"
	"github.com/lakesense/lakesense/engine/internal/connectors/kafka"
	"github.com/lakesense/lakesense/engine/internal/connectors/mongodb"
	"github.com/lakesense/lakesense/engine/internal/connectors/mssql"
	"github.com/lakesense/lakesense/engine/internal/connectors/mysql"
	"github.com/lakesense/lakesense/engine/internal/connectors/objectstore"
	"github.com/lakesense/lakesense/engine/internal/connectors/oracle"
	"github.com/lakesense/lakesense/engine/internal/connectors/postgres"
	"github.com/lakesense/lakesense/engine/internal/connectors/redis"
	"github.com/lakesense/lakesense/engine/internal/connectors/sqlite"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// extra holds registration hooks contributed by build-tagged connectors (e.g.
// db2, which needs CGo + IBM's clidriver and so cannot be a plain import here —
// that would force CGo into the default static build). Each such connector adds
// a hook from an init() in its own //go:build-tagged registry file; the default
// build sees an empty slice and has zero footprint from those connectors.
var extra []func(*sdk.Registry)

// Default returns a registry with every built-in connector.
func Default() *sdk.Registry {
	r := sdk.NewRegistry()
	r.Register(postgres.Type, postgres.New)
	r.Register(mysql.Type, mysql.New)
	r.Register(mongodb.Type, mongodb.New)
	r.Register(mssql.Type, mssql.New)
	r.Register(oracle.Type, oracle.New)
	r.Register(clickhouse.Type, clickhouse.New)
	r.Register(cassandra.Type, cassandra.New)
	r.Register(dynamodb.Type, dynamodb.New)
	r.Register(elasticsearch.Type, elasticsearch.New)
	r.Register(kafka.Type, kafka.New)
	r.Register(objectstore.Type, objectstore.New)
	r.Register(redis.Type, redis.New)
	r.Register(sqlite.Type, sqlite.New)
	for _, reg := range extra {
		reg(r)
	}
	return r
}
