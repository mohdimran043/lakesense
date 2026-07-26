package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/runner"
)

// specsFixture mirrors the JSON `lsengine catalog` prints: an array of connector
// Specs whose config_schema is a small JSON-Schema subset. It exercises every
// normalization rule (order, password, enum, array, integer, boolean, required,
// title/description/default) plus presets.
const specsFixture = `[
  {
    "type": "postgres",
    "display_name": "PostgreSQL",
    "capabilities": ["full_load", "incremental", "cdc"],
    "maturity": "certified",
    "config_schema": {
      "title": "PostgreSQL source",
      "required": ["host", "password"],
      "properties": {
        "host": {"type": "string", "title": "Host"},
        "port": {"type": "integer", "title": "Port", "default": 5432},
        "password": {"type": "string", "title": "Password", "format": "password"},
        "sslmode": {"type": "string", "title": "SSL mode", "default": "prefer",
          "enum": ["disable", "require"]},
        "cdc_auto_create": {"type": "boolean", "title": "Auto-create slot",
          "default": true, "description": "Create the replication slot"}
      }
    },
    "presets": [
      {"name": "cockroachdb", "display_name": "CockroachDB", "maturity": "stable",
       "notes": "no logical replication"}
    ]
  },
  {
    "type": "kafka",
    "display_name": "Kafka",
    "capabilities": ["full_load", "incremental"],
    "maturity": "beta",
    "config_schema": {
      "required": ["brokers", "topic"],
      "properties": {
        "brokers": {"type": "array", "items": {"type": "string"},
          "title": "Bootstrap brokers", "description": "host:port"},
        "topic": {"type": "string", "title": "Topic"}
      }
    }
  }
]`

func TestNormalizeCatalog(t *testing.T) {
	cat, err := normalizeCatalog([]byte(specsFixture))
	require.NoError(t, err)
	require.Len(t, cat, 2)

	pg := cat[0]
	assert.Equal(t, "postgres", pg.Type)
	assert.Equal(t, "PostgreSQL", pg.Label)
	assert.Equal(t, "certified", pg.Maturity)
	assert.Equal(t, []string{"full_load", "incremental", "cdc"}, pg.Capabilities)
	require.Len(t, pg.Presets, 1)
	assert.Equal(t, "cockroachdb", pg.Presets[0].Name)
	assert.Equal(t, "CockroachDB", pg.Presets[0].Label)

	// Field order follows the schema's properties declaration order.
	var keys []string
	for _, f := range pg.Fields {
		keys = append(keys, f.Key)
	}
	assert.Equal(t, []string{"host", "port", "password", "sslmode", "cdc_auto_create"}, keys)

	byKey := map[string]Field{}
	for _, f := range pg.Fields {
		byKey[f.Key] = f
	}
	assert.Equal(t, "string", byKey["host"].Type)
	assert.Equal(t, "Host", byKey["host"].Label)
	assert.True(t, byKey["host"].Required)

	assert.Equal(t, "integer", byKey["port"].Type)
	assert.Equal(t, "5432", byKey["port"].Default)
	assert.False(t, byKey["port"].Required)

	assert.Equal(t, "string", byKey["password"].Type)
	assert.True(t, byKey["password"].Secret)
	assert.True(t, byKey["password"].Required)

	assert.Equal(t, "enum", byKey["sslmode"].Type)
	assert.Equal(t, []string{"disable", "require"}, byKey["sslmode"].Enum)
	assert.Equal(t, "prefer", byKey["sslmode"].Default)

	assert.Equal(t, "boolean", byKey["cdc_auto_create"].Type)
	assert.Equal(t, "true", byKey["cdc_auto_create"].Default)
	assert.Equal(t, "Create the replication slot", byKey["cdc_auto_create"].Hint)

	kafka := cat[1]
	assert.Equal(t, "beta", kafka.Maturity)
	kf := map[string]Field{}
	for _, f := range kafka.Fields {
		kf[f.Key] = f
	}
	assert.Equal(t, "string-list", kf["brokers"].Type)
	assert.Equal(t, "host:port", kf["brokers"].Hint)
	assert.True(t, kf["brokers"].Required)
}

// TestNormalizeCatalogBadSchemaSkipsFields proves one connector's malformed
// schema does not blank the whole catalog: the connector still lists, with no
// fields, and the others are unaffected.
func TestNormalizeCatalogBadSchemaSkipsFields(t *testing.T) {
	const bad = `[
	  {"type": "broken", "display_name": "Broken", "maturity": "beta",
	   "config_schema": {"properties": "not-an-object"}},
	  {"type": "sqlite", "display_name": "SQLite", "maturity": "beta",
	   "config_schema": {"required": ["path"],
	     "properties": {"path": {"type": "string", "title": "Path"}}}}
	]`
	cat, err := normalizeCatalog([]byte(bad))
	require.NoError(t, err)
	require.Len(t, cat, 2)
	assert.Equal(t, "broken", cat[0].Type)
	assert.Empty(t, cat[0].Fields)
	assert.Equal(t, "sqlite", cat[1].Type)
	require.Len(t, cat[1].Fields, 1)
	assert.Equal(t, "path", cat[1].Fields[0].Key)
}

// catalogRunner is a pipelineRunner whose Catalog returns canned Spec JSON and
// counts calls, so the handler test can assert the result shape and that the
// catalog is memoized (fetched from the engine at most once).
type catalogRunner struct{ calls int }

func (c *catalogRunner) Run(context.Context, int64) (runner.RunResult, error) {
	return runner.RunResult{}, nil
}
func (c *catalogRunner) Backfill(context.Context, int64, runner.BackfillOpts) (runner.RunResult, error) {
	return runner.RunResult{}, nil
}
func (c *catalogRunner) Catalog(context.Context) ([]byte, error) {
	c.calls++
	return []byte(specsFixture), nil
}

func TestListConnectorsHandler(t *testing.T) {
	cr := &catalogRunner{}
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), runner: cr}
	h := chiRouter(s)

	do := func() []ConnectorCatalog {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/connectors", nil))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var got []ConnectorCatalog
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		return got
	}

	first := do()
	require.Len(t, first, 2)
	assert.Equal(t, "postgres", first[0].Type)
	assert.Equal(t, "PostgreSQL", first[0].Label)
	assert.Equal(t, "string-list", fieldByKey(t, first[1].Fields, "brokers").Type)

	second := do()
	assert.Equal(t, first, second)
	assert.Equal(t, 1, cr.calls, "catalog must be memoized: engine invoked once")
}

// TestNormalizeCatalogShowIf proves the x-show-if schema extension normalizes
// into Field.ShowIf so the wizard can render provider-conditional fields (e.g.
// Object Storage's s3/azure split).
func TestNormalizeCatalogShowIf(t *testing.T) {
	const spec = `[{"type":"objectstore","display_name":"Object Storage","maturity":"beta",
	  "config_schema":{"required":["bucket","endpoint"],"properties":{
	    "provider":{"type":"string","title":"Provider","default":"s3","enum":["s3","azure"]},
	    "bucket":{"type":"string","title":"Bucket"},
	    "endpoint":{"type":"string","title":"Endpoint (s3)","x-show-if":{"field":"provider","eq":"s3"}},
	    "account":{"type":"string","title":"Account (azure)","x-show-if":{"field":"provider","eq":"azure"}}
	  }}}]`
	cat, err := normalizeCatalog([]byte(spec))
	require.NoError(t, err)
	require.Len(t, cat, 1)

	bucket := fieldByKey(t, cat[0].Fields, "bucket")
	assert.Nil(t, bucket.ShowIf, "unconditional field has no show_if")

	endpoint := fieldByKey(t, cat[0].Fields, "endpoint")
	require.NotNil(t, endpoint.ShowIf)
	assert.Equal(t, "provider", endpoint.ShowIf.Field)
	assert.Equal(t, "s3", endpoint.ShowIf.Eq)
	assert.True(t, endpoint.Required, "endpoint is required (enforced only when its show_if matches)")

	account := fieldByKey(t, cat[0].Fields, "account")
	require.NotNil(t, account.ShowIf)
	assert.Equal(t, "azure", account.ShowIf.Eq)
}

func fieldByKey(t *testing.T, fields []Field, key string) Field {
	t.Helper()
	for _, f := range fields {
		if f.Key == key {
			return f
		}
	}
	t.Fatalf("field %q not found", key)
	return Field{}
}
