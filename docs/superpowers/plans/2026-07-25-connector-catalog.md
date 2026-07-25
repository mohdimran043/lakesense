# Registry-driven Connector Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the create-pipeline wizard's connector list and per-connector config form derive from the engine registry, so all 16 shipping sources appear automatically and the list can never drift out of date again.

**Architecture:** A new engine `catalog` verb prints every registered connector's Spec as a JSON array. The backend shells that verb behind the existing Engine seam, normalizes each Spec's JSON-Schema config into flat typed fields, and serves them at `GET /api/v1/connectors` (memoized once per process). The frontend wizard fetches that endpoint and renders the source picker + settings form entirely from the data. To let typed config values (integers, booleans, and array fields like Kafka brokers) survive the round-trip to the engine, the control-plane's endpoint settings are widened from `map[string]string` to `map[string]any`.

**Tech Stack:** Go (engine `lsengine` CLI, backend chi/pgx), React + TypeScript + TanStack Query + Tailwind (frontend), `gopkg.in/yaml.v3` (configver).

## Global Constraints

- Engine, backend, frontend are three separate Go/Node modules. The **backend must NOT import engine Go packages** — it decodes engine JSON with local structs (as `runner/catalog.go` already does). Copied verbatim from the codebase reality.
- `make check` must stay green: Go lint 0 issues, `go vet` clean, all tests pass under `-race`; frontend strict `tsc` + `oxlint` + `vite build`.
- No new engine third-party dependencies (catalog uses only stdlib + existing registry).
- **Connector-honesty principle:** the wizard shows every connector's real maturity badge (certified/stable/beta) from its Spec; there is no "Coming soon" — everything the registry lists actually ships. Never hardcode or fake a badge.
- Commit after each task with a `feat:`/`refactor:` message ending in the project's `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer. Commits go straight to `main` (local; origin is behind), matching the connector-shipping workflow.

---

### Task 1: Engine `catalog` verb

**Files:**
- Modify: `engine/internal/cli/cli.go` (add `sort` import, `catalog` dispatch case, `runCatalog`, usage line)
- Test: `engine/internal/cli/cli_test.go` (add a dispatch row + a new `TestCatalogEmitsAllConnectors`)

**Interfaces:**
- Consumes: `connectors.Default() *sdk.Registry`, `(*sdk.Registry).Names() []string`, `(*sdk.Registry).New(name string) (sdk.Connector, error)`, `sdk.Connector.Spec() sdk.Spec`, existing `writeJSON(w io.Writer, v any) error` helper in `cli.go`.
- Produces: `lsengine catalog` → stdout is a JSON array `[]sdk.Spec` sorted by `Type`. Consumed by Task 4's backend seam.

- [ ] **Step 1: Write the failing test**

Add to `engine/internal/cli/cli_test.go`:

```go
// TestCatalogEmitsAllConnectors proves `catalog` prints every registered
// connector's Spec as a JSON array (not events), sorted by type, each with a
// non-empty display name — the surface a UI builds its whole source picker from.
func TestCatalogEmitsAllConnectors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"catalog"}, &stdout, &stderr)
	require.Equal(t, 0, code, "stderr: %s", stderr.String())

	var specs []sdk.Spec
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &specs))

	byType := map[string]sdk.Spec{}
	var types []string
	for _, sp := range specs {
		byType[sp.Type] = sp
		types = append(types, sp.Type)
		assert.NotEmpty(t, sp.DisplayName, "connector %q missing display name", sp.Type)
	}
	for _, want := range []string{
		"postgres", "mysql", "mongodb", "mssql", "clickhouse", "cassandra",
		"dynamodb", "elasticsearch", "kafka", "objectstore", "redis", "sqlite",
	} {
		_, ok := byType[want]
		assert.True(t, ok, "catalog missing connector %q", want)
	}
	assert.True(t, sort.StringsAreSorted(types), "catalog must be sorted by type: %v", types)
}
```

Add a row to the `TestRunDispatch` table (inside the `tests` slice) so the verb is wired:

```go
		{name: "catalog lists connectors", args: []string{"catalog"}, wantCode: 0, wantOut: `"type"`},
```

Add `"sort"` to the test file's imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/cli/ -run TestCatalogEmitsAllConnectors -v`
Expected: FAIL — `Run` returns code 2 ("unknown command \"catalog\""), so `json.Unmarshal` gets usage text and errors.

- [ ] **Step 3: Add the dispatch case, verb, and usage line**

In `engine/internal/cli/cli.go`, add `"sort"` to the import block (keep imports grouped/sorted).

In the `switch cmd` inside `Run`, add after the `case "spec":` block:

```go
	case "catalog":
		err = runCatalog(ctx, rest, stdout)
```

In `usage`, add a line under `spec` in the Commands list:

```go
  catalog   print every connector's config schema as a JSON array
```

Add the verb implementation next to `runSpec`:

```go
// runCatalog emits every registered connector's Spec as a JSON array — the
// surface a UI builds its whole source picker from. Like spec, it needs no
// connection. Order is deterministic (sorted by type) so consumers and tests
// see a stable list.
func runCatalog(_ context.Context, _ []string, stdout io.Writer) error {
	reg := connectors.Default()
	names := reg.Names()
	sort.Strings(names)
	specs := make([]sdk.Spec, 0, len(names))
	for _, name := range names {
		c, err := reg.New(name)
		if err != nil {
			return err
		}
		specs = append(specs, c.Spec())
	}
	return writeJSON(stdout, specs)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && go test ./internal/cli/ -run 'TestCatalog|TestRunDispatch' -v`
Expected: PASS.

- [ ] **Step 5: Full engine check**

Run: `cd engine && go build ./... && go vet ./... && go test ./internal/cli/`
Expected: builds, vet clean, tests pass.

- [ ] **Step 6: Commit**

```bash
git add engine/internal/cli/cli.go engine/internal/cli/cli_test.go
git commit -m "feat(engine): catalog verb — every connector's Spec as a JSON array

$(printf 'Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>')"
```

---

### Task 2: Widen control-plane endpoint settings to `map[string]any`

**Why:** The engine's connector configs have integer fields (`port`, `max_connections`), boolean fields (`cdc_auto_create`), and array fields (Kafka `brokers`, Cassandra `hosts`, Elasticsearch `addresses` — all `[]string`). A `map[string]string` settings map can only carry strings, so those fields either fail the engine's JSON unmarshal at run time or can't be sent at all. Widening to `map[string]any` lets the wizard submit properly typed JSON that flows unchanged through storage and into the engine config.

**Files:**
- Modify: `backend/internal/configver/configver.go:31` (`Endpoint.Settings` type)
- Modify: `backend/internal/pipelines/pipelines.go:19` (`Endpoint.Settings` type)
- Modify: `backend/internal/envs/promote.go` (`applyOverrides`, `missingIn` signatures — merge into a `map[string]any`)
- Modify: `backend/internal/runner/catalog.go:23` (`flattenEndpoint`'s local `Settings` type)
- Test: `backend/internal/configver/configver_test.go` (update literals + add mixed-type idempotency test), `backend/internal/pipelines/service_test.go` (update literals), `backend/internal/envs/promote_test.go` (update `Endpoint.Settings` literals only)

**Interfaces:**
- Consumes: nothing new.
- Produces: `configver.Endpoint.Settings map[string]any`, `pipelines.Endpoint.Settings map[string]any` (the JSON-decoded shape of a create-pipeline request's `source`/`destination` `settings`). `envs.Overrides` stays `map[string]string` (credential overrides are always scalar and assign cleanly into `any`). Task 5's frontend relies on the write API accepting `settings` values that are strings, numbers, booleans, or string arrays.

- [ ] **Step 1: Write the failing idempotency test**

Add to `backend/internal/configver/configver_test.go`:

```go
// TestCanonicalYAMLMixedTypesIdempotent guards the map[string]any widening:
// settings holding an integer, a boolean, and a string list must serialize
// deterministically and survive a Parse→CanonicalYAML round-trip byte-for-byte,
// which is what version dedup and rollback depend on.
func TestCanonicalYAMLMixedTypesIdempotent(t *testing.T) {
	c := Config{
		Name: "kafka-pipe",
		Source: Endpoint{Type: "kafka", Settings: map[string]any{
			"brokers":     []any{"b1:9092", "b2:9092"},
			"topic":       "orders",
			"max_bytes":   1048576,
			"tls":         true,
			"description": "true", // a genuine string that looks like a bool
		}},
		Destination: Endpoint{Type: "parquet", Settings: map[string]any{"path": "./out"}},
		Schedule:    "@daily",
		Streams:     []Stream{{Name: "orders", Mode: "full_load"}},
	}
	y1, err := CanonicalYAML(c)
	require.NoError(t, err)

	parsed, err := Parse(y1)
	require.NoError(t, err)
	y2, err := CanonicalYAML(parsed)
	require.NoError(t, err)

	assert.Equal(t, y1, y2, "canonical YAML must be idempotent across Parse")
	assert.Contains(t, y1, "brokers:")
	assert.Contains(t, y1, "b1:9092")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/configver/ -run TestCanonicalYAMLMixedTypesIdempotent`
Expected: FAIL to **compile** — `Endpoint.Settings` is `map[string]string`, so the `map[string]any` literal is a type error.

- [ ] **Step 3: Widen the production types**

`backend/internal/configver/configver.go` — change the `Endpoint` struct field:

```go
// Endpoint is a source or destination, with connector-specific settings kept in
// a sorted map so serialization stays deterministic. Values are any because
// connector configs include integers, booleans, and string arrays, not only
// strings (e.g. Kafka brokers). yaml.v3 sorts keys and renders scalars and
// sequences deterministically, so equal configs still yield identical YAML.
type Endpoint struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings,omitempty"`
}
```

`backend/internal/pipelines/pipelines.go` — change the `Endpoint` struct field (line ~19):

```go
	Settings map[string]any `json:"settings,omitempty"`
```

`backend/internal/runner/catalog.go` — in `flattenEndpoint`, change the local anonymous struct's field so array/number/bool values pass straight through into the engine config document:

```go
	var ep struct {
		Type     string         `json:"type"`
		Settings map[string]any `json:"settings"`
	}
```

`backend/internal/envs/promote.go` — `applyOverrides` merges into a `map[string]any`; `missingIn` reads a `map[string]any`. `Overrides` itself stays `map[string]string` (scalar credential overrides assign cleanly into `any`):

```go
// applyOverrides merges overrides onto an endpoint's settings (override wins),
// leaving unspecified settings intact.
func applyOverrides(e configver.Endpoint, over map[string]string) configver.Endpoint {
	merged := map[string]any{}
	for k, v := range e.Settings {
		merged[k] = v
	}
	for k, v := range over {
		merged[k] = v
	}
	if len(merged) == 0 {
		merged = nil
	}
	return configver.Endpoint{Type: e.Type, Settings: merged}
}
```

```go
func missingIn(settings map[string]any, over map[string]string, side string) []string {
	var out []string
	for k := range settings {
		if sensitiveKeys[k] {
			if _, ok := over[k]; !ok {
				out = append(out, fmt.Sprintf("%s.%s", side, k))
			}
		}
	}
	return out
}
```

- [ ] **Step 4: Update the existing test literals to compile**

Change every `Endpoint{... Settings: map[string]string{...}}` literal to `map[string]any{...}` (the values are unchanged). `envs.Overrides{Source: map[string]string{...}}` literals stay as-is. Exact locations:

- `backend/internal/configver/configver_test.go:16` — `Settings: map[string]any{"host": "db", "database": "shop"}`
- `backend/internal/configver/configver_test.go:28` — `b.Source.Settings = map[string]any{"database": "shop", "host": "db"}`
- `backend/internal/pipelines/service_test.go:88` — `Settings: map[string]any{"host": "db"}`
- `backend/internal/pipelines/service_test.go:89` — `Settings: map[string]any{"path": "./out"}`
- `backend/internal/envs/promote_test.go:14` — `Settings: map[string]any{"host": "dev-db", "database": "shop", "password": "devpw", "sslmode": "disable"}`
- `backend/internal/envs/promote_test.go:15` — `Settings: map[string]any{"path": "s3://dev-lake/orders"}`

Leave the `Overrides{Source: map[string]string{...}, Destination: map[string]string{...}}` literals in `promote_test.go` (lines 23–24, 42, 49, 57–58) and `service_test.go` (lines 203–204) unchanged.

- [ ] **Step 5: Run the affected suites**

Run: `cd backend && go build ./... && go test ./internal/configver/ ./internal/pipelines/ ./internal/envs/ ./internal/runner/`
Expected: builds; `TestCanonicalYAMLMixedTypesIdempotent` PASSES; no regressions. (Postgres-gated integration tests in `pipelines` skip without `LAKESENSE_TEST_DB` — that is expected.)

- [ ] **Step 6: Commit**

```bash
git add backend/internal/configver backend/internal/pipelines/pipelines.go backend/internal/envs backend/internal/runner/catalog.go
git commit -m "refactor(backend): endpoint settings map[string]any for typed connector config

Integer/boolean/array config fields (Kafka brokers, Postgres port, cdc flags)
need non-string JSON to survive the round-trip to the engine. Settings widen to
map[string]any end-to-end; promotion overrides stay scalar. Canonical YAML stays
deterministic (idempotency test added).

$(printf 'Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>')"
```

---

### Task 3: Backend connector-catalog normalizer

**Files:**
- Create: `backend/internal/api/connectorcatalog.go`
- Test: `backend/internal/api/connectorcatalog_test.go`

**Interfaces:**
- Consumes: raw catalog JSON bytes (the `[]sdk.Spec` array printed by `lsengine catalog`), decoded with **local** structs (no engine import).
- Produces (used by Task 4's handler and Task 5's frontend types):
  - `type Field struct { Key, Label, Type string; Secret, Required bool; Default string; Enum []string; Hint string }` with JSON tags `key,label,type,secret,required,default,enum,hint` (the bools/strings/slice use `omitempty` except `key`/`label`/`type`).
  - `type Preset struct { Name, Label, Maturity, Notes string }`.
  - `type ConnectorCatalog struct { Type, Label, Maturity string; Capabilities []string; Presets []Preset; Fields []Field }`.
  - `func normalizeCatalog(specsJSON []byte) ([]ConnectorCatalog, error)`.

- [ ] **Step 1: Write the failing normalizer tests**

Create `backend/internal/api/connectorcatalog_test.go`:

```go
package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd backend && go test ./internal/api/ -run TestNormalizeCatalog`
Expected: FAIL to compile — `normalizeCatalog`, `Field`, `ConnectorCatalog` are undefined.

- [ ] **Step 3: Implement the normalizer**

Create `backend/internal/api/connectorcatalog.go`:

```go
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// Field is one normalized config-form input the wizard renders. It is a flat
// projection of a JSON-Schema property, typed for the UI.
type Field struct {
	Key      string   `json:"key"`
	Label    string   `json:"label"`
	Type     string   `json:"type"` // string | integer | boolean | enum | string-list
	Secret   bool     `json:"secret,omitempty"`
	Required bool     `json:"required,omitempty"`
	Default  string   `json:"default,omitempty"`
	Enum     []string `json:"enum,omitempty"`
	Hint     string   `json:"hint,omitempty"`
}

// Preset is a named wire-compatible variant of a connector (e.g. CockroachDB).
type Preset struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Maturity string `json:"maturity,omitempty"`
	Notes    string `json:"notes,omitempty"`
}

// ConnectorCatalog is one connector as the wizard consumes it: identity,
// maturity badge, capabilities, presets, and normalized config fields.
type ConnectorCatalog struct {
	Type         string   `json:"type"`
	Label        string   `json:"label"`
	Maturity     string   `json:"maturity"`
	Capabilities []string `json:"capabilities"`
	Presets      []Preset `json:"presets,omitempty"`
	Fields       []Field  `json:"fields"`
}

// specDoc is the subset of the engine's sdk.Spec JSON the catalog needs. The
// backend is a separate module, so we decode with local types rather than
// importing the engine.
type specDoc struct {
	Type         string          `json:"type"`
	DisplayName  string          `json:"display_name"`
	Capabilities []string        `json:"capabilities"`
	Maturity     string          `json:"maturity"`
	ConfigSchema json.RawMessage `json:"config_schema"`
	Presets      []struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		Maturity    string `json:"maturity"`
		Notes       string `json:"notes"`
	} `json:"presets"`
}

// schemaDoc is the JSON-Schema subset a config_schema uses.
type schemaDoc struct {
	Required   []string        `json:"required"`
	Properties json.RawMessage `json:"properties"`
}

// propSchema is one property in a config_schema.
type propSchema struct {
	Type        string          `json:"type"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	Format      string          `json:"format"`
	Enum        []string        `json:"enum"`
	Default     json.RawMessage `json:"default"`
	Items       struct {
		Type string `json:"type"`
	} `json:"items"`
}

// normalizeCatalog turns the engine's []Spec JSON into wizard-ready connectors.
// A connector whose schema fails to parse is still listed (with no fields), so
// one bad schema can never blank the whole wizard.
func normalizeCatalog(specsJSON []byte) ([]ConnectorCatalog, error) {
	var specs []specDoc
	if err := json.Unmarshal(specsJSON, &specs); err != nil {
		return nil, fmt.Errorf("parse connector catalog: %w", err)
	}
	out := make([]ConnectorCatalog, 0, len(specs))
	for _, sp := range specs {
		fields, err := normalizeFields(sp.ConfigSchema)
		if err != nil {
			slog.Default().Warn("connector schema parse failed; listing with no fields",
				"connector", sp.Type, "err", err)
			fields = nil
		}
		cc := ConnectorCatalog{
			Type:         sp.Type,
			Label:        sp.DisplayName,
			Maturity:     sp.Maturity,
			Capabilities: sp.Capabilities,
			Fields:       fields,
		}
		for _, pr := range sp.Presets {
			cc.Presets = append(cc.Presets, Preset{
				Name: pr.Name, Label: pr.DisplayName, Maturity: pr.Maturity, Notes: pr.Notes,
			})
		}
		out = append(out, cc)
	}
	return out, nil
}

// normalizeFields flattens a config_schema's properties into typed Fields, in
// declaration order (Go maps don't preserve order, so keys are read off the raw
// token stream).
func normalizeFields(schema json.RawMessage) ([]Field, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	var doc schemaDoc
	if err := json.Unmarshal(schema, &doc); err != nil {
		return nil, err
	}
	if len(doc.Properties) == 0 {
		return nil, nil
	}
	required := map[string]bool{}
	for _, r := range doc.Required {
		required[r] = true
	}
	keys, err := orderedKeys(doc.Properties)
	if err != nil {
		return nil, err
	}
	var props map[string]propSchema
	if err := json.Unmarshal(doc.Properties, &props); err != nil {
		return nil, err
	}
	fields := make([]Field, 0, len(keys))
	for _, k := range keys {
		p := props[k]
		f := Field{
			Key:      k,
			Label:    orDefault(p.Title, k),
			Type:     fieldType(p),
			Secret:   p.Format == "password",
			Required: required[k],
			Default:  stringifyDefault(p.Default),
			Hint:     p.Description,
		}
		if f.Type == "enum" {
			f.Enum = p.Enum
		}
		fields = append(fields, f)
	}
	return fields, nil
}

// fieldType maps a JSON-Schema property to the wizard's input type.
func fieldType(p propSchema) string {
	switch {
	case len(p.Enum) > 0:
		return "enum"
	case p.Type == "array" && p.Items.Type == "string":
		return "string-list"
	case p.Type == "integer":
		return "integer"
	case p.Type == "boolean":
		return "boolean"
	default:
		return "string"
	}
}

// orderedKeys returns a JSON object's keys in declaration order. It reads the
// key tokens off a decoder and consumes each value with Decode so nested
// objects/arrays are skipped correctly.
func orderedKeys(obj json.RawMessage) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("properties is not an object")
	}
	var keys []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected string property key, got %T", keyTok)
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// stringifyDefault renders a JSON default value as a string for the form: a JSON
// string is unquoted; anything else (number, bool) uses its raw text.
func stringifyDefault(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test ./internal/api/ -run TestNormalizeCatalog -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add backend/internal/api/connectorcatalog.go backend/internal/api/connectorcatalog_test.go
git commit -m "feat(backend): connector-catalog normalizer (JSON-Schema -> typed fields)

$(printf 'Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>')"
```

---

### Task 4: Backend `Catalog` engine seam + `GET /connectors` handler

**Files:**
- Modify: `backend/internal/runner/runner.go` (add `Catalog` to the `Engine` interface + a `(*Runner).Catalog` delegate)
- Modify: `backend/internal/runner/engine.go` (add `execEngine.Catalog` shelling `lsengine catalog`)
- Modify: `backend/internal/api/writes.go` (add `Catalog` to the `pipelineRunner` interface)
- Modify: `backend/internal/api/api.go` (add memo fields + `sync` import to `Server`)
- Modify: `backend/internal/api/handlers.go` (register route + `listConnectors` handler)
- Modify: `backend/internal/api/writes_test.go` (add `Catalog` to `fakeRunner`)
- Modify: any fake `Engine` in `backend/internal/runner/*_test.go` (add a `Catalog` method so the interface still satisfies)
- Test: `backend/internal/api/connectorcatalog_test.go` (add a handler + memoization test)

**Interfaces:**
- Consumes: `normalizeCatalog([]byte) ([]ConnectorCatalog, error)` from Task 3; `lsengine catalog` from Task 1.
- Produces: `runner.Engine.Catalog(ctx context.Context) ([]byte, error)`, `(*runner.Runner).Catalog(ctx) ([]byte, error)`, `pipelineRunner.Catalog(ctx) ([]byte, error)`, and `GET /api/v1/connectors` → `[]ConnectorCatalog` JSON. Task 5's frontend calls this endpoint.

- [ ] **Step 1: Add `Catalog` to the runner Engine seam**

In `backend/internal/runner/runner.go`, add to the `Engine` interface (after `Backfill`):

```go
	// Catalog returns `lsengine catalog` stdout: every registered connector's
	// Spec as a JSON array. Static per engine build.
	Catalog(ctx context.Context) ([]byte, error)
```

Add a delegate method on `*Runner` (near `Backfill`):

```go
// Catalog returns the engine's connector catalog JSON. It is static per engine
// build; callers (the API) memoize it.
func (r *Runner) Catalog(ctx context.Context) ([]byte, error) {
	return r.engine.Catalog(ctx)
}
```

In `backend/internal/runner/engine.go`, add:

```go
// Catalog runs `lsengine catalog` and returns its stdout (the []Spec JSON).
func (e execEngine) Catalog(ctx context.Context) ([]byte, error) {
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, e.Path, "catalog")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("lsengine catalog: %w: %s", err, errb.String())
	}
	return out.Bytes(), nil
}
```

- [ ] **Step 2: Make existing fakes satisfy the widened interface**

In `backend/internal/api/writes_test.go`, add a `Catalog` method to `fakeRunner` (returns an empty array; the run/backfill tests don't exercise it):

```go
func (f *fakeRunner) Catalog(_ context.Context) ([]byte, error) { return []byte("[]"), nil }
```

Find every other type that implements `runner.Engine` in tests and add a `Catalog` method:

Run: `cd backend && grep -rln "func.*Discover(ctx context.Context, .*string) (\[\]byte" internal/runner`
For each fake engine found (e.g. in `runner_test.go`), add:

```go
func (f *fakeEngine) Catalog(_ context.Context) ([]byte, error) { return []byte("[]"), nil }
```

(Use the actual receiver name/type the file uses.)

- [ ] **Step 3: Add the `Catalog` method to the `pipelineRunner` interface**

In `backend/internal/api/writes.go`, extend the `pipelineRunner` interface:

```go
type pipelineRunner interface {
	Run(ctx context.Context, id int64) (runner.RunResult, error)
	Backfill(ctx context.Context, id int64, o runner.BackfillOpts) (runner.RunResult, error)
	// Catalog returns the engine's connector catalog JSON (static per build).
	Catalog(ctx context.Context) ([]byte, error)
}
```

- [ ] **Step 4: Write the failing handler test**

Add to `backend/internal/api/connectorcatalog_test.go` (imports: add `context`, `encoding/json`, `log/slog`, `net/http`, `net/http/httptest`, `github.com/lakesense/lakesense/backend/internal/runner`):

```go
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
```

Note: `io` must be imported in this test file too.

- [ ] **Step 5: Run test to verify it fails**

Run: `cd backend && go test ./internal/api/ -run TestListConnectorsHandler`
Expected: FAIL to compile — `Server` has no memo fields and no `listConnectors`; route unregistered.

- [ ] **Step 6: Add memo state, the route, and the handler**

In `backend/internal/api/api.go`, add `"sync"` to imports and two fields to `Server`:

```go
type Server struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	pipelines *pipelines.Service
	runner    pipelineRunner
	audit     audit.Recorder
	now       func() time.Time

	// connMu guards the memoized connector catalog. The catalog is static per
	// engine build, so it is fetched from the engine at most once per process.
	connMu    sync.Mutex
	connCache []ConnectorCatalog
}
```

In `backend/internal/api/handlers.go`, register the route inside `registerData` (next to the other `r.Get`s):

```go
	r.Get("/connectors", s.listConnectors)
```

Add the handler (in `handlers.go`):

```go
// listConnectors serves the engine's connector registry as wizard-ready
// catalog entries. The catalog cannot change without a new engine binary, so
// the first successful result is memoized; failures are not cached.
func (s *Server) listConnectors(w http.ResponseWriter, r *http.Request) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.connCache != nil {
		writeJSON(w, http.StatusOK, s.connCache)
		return
	}
	if s.runner == nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "connector catalog unavailable"})
		return
	}
	raw, err := s.runner.Catalog(r.Context())
	if err != nil {
		s.logger.Error("connector catalog unavailable", "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "connector catalog unavailable"})
		return
	}
	cat, err := normalizeCatalog(raw)
	if err != nil {
		s.logger.Error("connector catalog malformed", "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "connector catalog malformed"})
		return
	}
	s.connCache = cat
	writeJSON(w, http.StatusOK, cat)
}
```

- [ ] **Step 7: Run the backend suites**

Run: `cd backend && go build ./... && go vet ./... && go test ./internal/api/ ./internal/runner/`
Expected: builds, vet clean, all tests pass (including `TestListConnectorsHandler` and the memoization assertion).

- [ ] **Step 8: Commit**

```bash
git add backend/internal/runner backend/internal/api
git commit -m "feat(backend): GET /connectors — memoized registry-driven catalog

Engine seam gains Catalog() shelling lsengine catalog; the handler normalizes it
once per process and serves []ConnectorCatalog.

$(printf 'Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>')"
```

---

### Task 5: Frontend wizard reads the catalog

**Files:**
- Modify: `frontend/src/lib/api.ts` (add `ConnectorField`/`ConnectorCatalog` types, widen `EndpointInput.settings`, add `api.connectors`)
- Modify: `frontend/src/lib/hooks.ts` (add `useConnectors`)
- Modify: `frontend/src/pages/CreatePipeline.tsx` (full rewrite of the source list + settings rendering)

**Interfaces:**
- Consumes: `GET /connectors` → `ConnectorCatalog[]` from Task 4; the widened write API (`settings` values may be string/number/boolean/string[]) from Task 2.
- Produces: a wizard whose source picker and per-connector form are 100% data-driven.

- [ ] **Step 1: Add the API types, widen settings, add the client method**

In `frontend/src/lib/api.ts`, change `EndpointInput`:

```ts
export interface EndpointInput {
  type: string;
  // Values are typed to match the connector's config schema: strings, numbers,
  // booleans, or string arrays (e.g. Kafka brokers). The backend stores them as
  // map[string]any and passes them straight to the engine config.
  settings: Record<string, unknown>;
}
```

Add near the other interfaces (e.g. after `EndpointInput`):

```ts
export interface ConnectorField {
  key: string;
  label: string;
  type: "string" | "integer" | "boolean" | "enum" | "string-list";
  secret?: boolean;
  required?: boolean;
  default?: string;
  enum?: string[];
  hint?: string;
}

export interface ConnectorCatalog {
  type: string;
  label: string;
  maturity: string;
  capabilities: string[];
  presets?: { name: string; label: string; maturity?: string; notes?: string }[];
  fields: ConnectorField[];
}
```

Add to the `api` object:

```ts
  connectors: () => get<ConnectorCatalog[]>("/connectors"),
```

- [ ] **Step 2: Add the query hook**

In `frontend/src/lib/hooks.ts`, add (next to the other read hooks):

```ts
export const useConnectors = () =>
  useQuery({ queryKey: ["connectors"], queryFn: api.connectors, ...opts });
```

- [ ] **Step 3: Rewrite `CreatePipeline.tsx`**

Replace the entire contents of `frontend/src/pages/CreatePipeline.tsx` with:

```tsx
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge, Button, Card, Field, Input, Select, cn } from "../components/ui";
import { useConnectors } from "../lib/hooks";
import { useCreatePipeline } from "../lib/mutations";
import type { ConnectorCatalog, ConnectorField, CreatePipelineRequest } from "../lib/api";

// Destinations are engine writers, not registry connectors (no Spec), so they
// stay a small hardcoded list — shaped like a catalog entry for uniform rendering.
const DESTINATIONS: ConnectorCatalog[] = [
  {
    type: "parquet",
    label: "Parquet",
    maturity: "lakehouse",
    capabilities: [],
    fields: [{ key: "path", label: "Output directory", type: "string", required: true, hint: "./out" }],
  },
  {
    type: "ndjson",
    label: "NDJSON",
    maturity: "",
    capabilities: [],
    fields: [{ key: "path", label: "Output directory", type: "string", required: true, hint: "./out" }],
  },
];

const MODES = ["full_load", "incremental", "cdc"];
const STEPS = ["Source", "Destination", "Streams", "Review"];

// A settings value mirrors a ConnectorField's type while editing: text/enum/
// integer inputs hold strings, checkboxes hold booleans, string-lists hold arrays.
type SettingVal = string | boolean | string[];

interface Endpoint {
  type: string;
  settings: Record<string, SettingVal>;
}
interface Stream {
  name: string;
  mode: string;
  cursor_field: string;
}
interface State {
  name: string;
  environment: string;
  schedule: string;
  source: Endpoint;
  destination: Endpoint;
  streams: Stream[];
}

const initialState: State = {
  name: "",
  environment: "dev",
  schedule: "@daily",
  source: { type: "", settings: {} },
  destination: { type: "parquet", settings: {} },
  streams: [{ name: "", mode: "full_load", cursor_field: "" }],
};

// seedDefaults pre-fills a connector's settings with its schema defaults so the
// form shows the same values the engine would use (esp. booleans/enums).
function seedDefaults(fields: ConnectorField[]): Record<string, SettingVal> {
  const out: Record<string, SettingVal> = {};
  for (const f of fields) {
    if (f.default === undefined || f.default === "") continue;
    if (f.type === "boolean") out[f.key] = f.default === "true";
    else if (f.type === "string-list") out[f.key] = [f.default];
    else out[f.key] = f.default;
  }
  return out;
}

function isEmptyVal(v: SettingVal | undefined): boolean {
  if (v === undefined) return true;
  if (Array.isArray(v)) return v.filter((x) => x.trim()).length === 0;
  if (typeof v === "boolean") return false;
  return !v.trim();
}

function validateStep(step: number, s: State, source: ConnectorCatalog | undefined): Record<string, string> {
  const e: Record<string, string> = {};
  if (step === 0) {
    if (!s.name.trim()) e.name = "A name is required.";
    if (!source) e.source = "Choose a source.";
    source?.fields.forEach((f) => {
      if (f.required && isEmptyVal(s.source.settings[f.key])) e["src_" + f.key] = `${f.label} is required.`;
    });
  }
  if (step === 1) {
    const dst = DESTINATIONS.find((d) => d.type === s.destination.type);
    dst?.fields.forEach((f) => {
      if (f.required && isEmptyVal(s.destination.settings[f.key])) e["dst_" + f.key] = `${f.label} is required.`;
    });
  }
  if (step === 2) {
    if (s.streams.length === 0) e.streams = "Add at least one stream.";
    s.streams.forEach((st, i) => {
      if (!st.name.trim()) e["stream_" + i] = "Stream name is required.";
      if (st.mode === "incremental" && !st.cursor_field.trim()) e["cursor_" + i] = "Incremental needs a cursor field.";
    });
  }
  return e;
}

// buildSettings converts the editing state into the typed JSON the write API
// stores: integers as numbers, booleans as booleans, string-lists as arrays,
// everything else as trimmed strings. Empty scalars are dropped.
function buildSettings(fields: ConnectorField[], raw: Record<string, SettingVal>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const f of fields) {
    const v = raw[f.key];
    if (v === undefined) continue;
    if (f.type === "string-list") {
      const arr = (Array.isArray(v) ? v : []).map((x) => x.trim()).filter(Boolean);
      if (arr.length) out[f.key] = arr;
    } else if (f.type === "boolean") {
      if (typeof v === "boolean") out[f.key] = v;
    } else if (f.type === "integer") {
      if (typeof v === "string" && v.trim()) out[f.key] = Number(v);
    } else if (typeof v === "string" && v.trim()) {
      out[f.key] = v.trim();
    }
  }
  return out;
}

function toRequest(s: State, source: ConnectorCatalog | undefined): CreatePipelineRequest {
  const dst = DESTINATIONS.find((d) => d.type === s.destination.type);
  return {
    name: s.name.trim(),
    environment: s.environment,
    source: { type: s.source.type, settings: buildSettings(source?.fields ?? [], s.source.settings) },
    destination: { type: s.destination.type, settings: buildSettings(dst?.fields ?? [], s.destination.settings) },
    schedule: s.schedule,
    streams: s.streams.map((st) => ({
      name: st.name.trim(),
      mode: st.mode,
      ...(st.mode === "incremental" && st.cursor_field.trim() ? { cursor_field: st.cursor_field.trim() } : {}),
    })),
  };
}

const MATURITY_LABEL: Record<string, string> = {
  certified: "Certified",
  stable: "Stable",
  beta: "Beta",
  lakehouse: "Lakehouse",
};

export function CreatePipeline() {
  const nav = useNavigate();
  const create = useCreatePipeline();
  const connectors = useConnectors();
  const [step, setStep] = useState(0);
  const [s, setS] = useState<State>(initialState);

  const sources = connectors.data ?? [];
  const source = sources.find((c) => c.type === s.source.type);

  // Once the catalog loads, default the source to postgres (or the first entry)
  // if the current selection isn't in it.
  useEffect(() => {
    if (sources.length === 0) return;
    setS((prev) => {
      if (sources.some((c) => c.type === prev.source.type)) return prev;
      const preferred = sources.find((c) => c.type === "postgres") ?? sources[0];
      return { ...prev, source: { type: preferred.type, settings: seedDefaults(preferred.fields) } };
    });
  }, [sources]);

  const errors = validateStep(step, s, source);
  const stepValid = Object.keys(errors).length === 0;

  const selectSource = (type: string) => {
    const c = sources.find((x) => x.type === type);
    setS({ ...s, source: { type, settings: c ? seedDefaults(c.fields) : {} } });
  };
  const selectDest = (type: string) => {
    const d = DESTINATIONS.find((x) => x.type === type);
    setS({ ...s, destination: { type, settings: d ? seedDefaults(d.fields) : {} } });
  };
  const setSetting = (which: "source" | "destination", key: string, val: SettingVal) =>
    setS({ ...s, [which]: { ...s[which], settings: { ...s[which].settings, [key]: val } } });

  const setStream = (i: number, patch: Partial<Stream>) =>
    setS({ ...s, streams: s.streams.map((st, j) => (j === i ? { ...st, ...patch } : st)) });
  const addStream = () => setS({ ...s, streams: [...s.streams, { name: "", mode: "full_load", cursor_field: "" }] });
  const removeStream = (i: number) => setS({ ...s, streams: s.streams.filter((_, j) => j !== i) });

  const submit = () => {
    create.mutate(toRequest(s, source), {
      onSuccess: (p) => nav(`/pipelines/${p.id}`),
    });
  };

  const dst = DESTINATIONS.find((d) => d.type === s.destination.type);

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">New pipeline</h1>
        <p className="mt-1 text-sm text-muted">Connect a source, choose a lakehouse destination, pick streams.</p>
      </div>

      <ProgressRail step={step} />

      <Card className="p-6">
        {step === 0 && (
          <div className="space-y-4">
            <Field label="Pipeline name" error={errors.name}>
              <Input value={s.name} invalid={!!errors.name} placeholder="Orders to Lake" onChange={(e) => setS({ ...s, name: e.target.value })} />
            </Field>
            <div className="grid grid-cols-2 gap-4">
              <Field label="Environment">
                <Select value={s.environment} onChange={(e) => setS({ ...s, environment: e.target.value })}>
                  <option value="dev">dev</option>
                  <option value="staging">staging</option>
                  <option value="prod">prod</option>
                </Select>
              </Field>
              <Field label="Schedule" hint="How often it runs">
                <Select value={s.schedule} onChange={(e) => setS({ ...s, schedule: e.target.value })}>
                  <option value="">Manual</option>
                  <option value="@hourly">Hourly</option>
                  <option value="@daily">Daily</option>
                  <option value="@weekly">Weekly</option>
                </Select>
              </Field>
            </div>

            {connectors.isLoading && <p className="text-sm text-muted">Loading connectors…</p>}
            {connectors.isError && (
              <div className="rounded-control border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
                Couldn't load the connector catalog.{" "}
                <button className="underline" onClick={() => connectors.refetch()}>
                  Retry
                </button>
              </div>
            )}
            {!connectors.isLoading && !connectors.isError && (
              <>
                <ConnectorPicker
                  items={sources.map((c) => ({ type: c.type, label: c.label, badge: MATURITY_LABEL[c.maturity] ?? c.maturity }))}
                  selected={s.source.type}
                  onSelect={selectSource}
                  error={errors.source}
                />
                <SettingsFields fields={source?.fields ?? []} settings={s.source.settings} errPrefix="src_" errors={errors} onChange={(k, v) => setSetting("source", k, v)} />
              </>
            )}
          </div>
        )}

        {step === 1 && (
          <div className="space-y-4">
            <ConnectorPicker
              items={DESTINATIONS.map((d) => ({ type: d.type, label: d.label, badge: MATURITY_LABEL[d.maturity] ?? d.maturity }))}
              selected={s.destination.type}
              onSelect={selectDest}
              error={errors.destination}
            />
            <SettingsFields fields={dst?.fields ?? []} settings={s.destination.settings} errPrefix="dst_" errors={errors} onChange={(k, v) => setSetting("destination", k, v)} />
          </div>
        )}

        {step === 2 && (
          <div className="space-y-3">
            {errors.streams && <p className="text-xs text-danger">{errors.streams}</p>}
            {s.streams.map((st, i) => (
              <div key={i} className="rounded-control border border-line p-3">
                <div className="grid grid-cols-[1fr,auto] gap-3">
                  <Field label="Stream" error={errors["stream_" + i]}>
                    <Input value={st.name} invalid={!!errors["stream_" + i]} placeholder="public.orders" onChange={(e) => setStream(i, { name: e.target.value })} />
                  </Field>
                  <Field label="Mode">
                    <Select value={st.mode} onChange={(e) => setStream(i, { mode: e.target.value })}>
                      {MODES.map((m) => (
                        <option key={m} value={m}>
                          {m}
                        </option>
                      ))}
                    </Select>
                  </Field>
                </div>
                {st.mode === "incremental" && (
                  <Field label="Cursor field" error={errors["cursor_" + i]}>
                    <Input value={st.cursor_field} invalid={!!errors["cursor_" + i]} placeholder="updated_at" onChange={(e) => setStream(i, { cursor_field: e.target.value })} />
                  </Field>
                )}
                {s.streams.length > 1 && (
                  <button className="mt-2 text-xs text-faint hover:text-danger" onClick={() => removeStream(i)}>
                    Remove
                  </button>
                )}
              </div>
            ))}
            <Button variant="ghost" size="sm" onClick={addStream}>
              + Add stream
            </Button>
          </div>
        )}

        {step === 3 && <Review s={s} />}

        {create.isError && (
          <div className="mt-4 rounded-control border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
            {(create.error as Error).message}
          </div>
        )}

        <div className="mt-6 flex items-center justify-between">
          <Button variant="ghost" onClick={() => (step === 0 ? nav("/pipelines") : setStep(step - 1))}>
            {step === 0 ? "Cancel" : "Back"}
          </Button>
          {step < STEPS.length - 1 ? (
            <Button variant="primary" disabled={!stepValid} onClick={() => setStep(step + 1)}>
              Next
            </Button>
          ) : (
            <Button variant="primary" disabled={create.isPending} onClick={submit}>
              {create.isPending ? "Creating…" : "Create pipeline"}
            </Button>
          )}
        </div>
      </Card>
    </div>
  );
}

function ProgressRail({ step }: { step: number }) {
  return (
    <div className="flex items-center gap-2">
      {STEPS.map((label, i) => (
        <div key={label} className="flex flex-1 items-center gap-2">
          <div
            className={cn(
              "flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-[11px] tnum",
              i < step && "border-signal bg-signal/15 text-signal",
              i === step && "border-signal text-signal",
              i > step && "border-line text-faint",
            )}
          >
            {i + 1}
          </div>
          <span className={cn("text-xs", i === step ? "text-text" : "text-faint")}>{label}</span>
          {i < STEPS.length - 1 && <div className={cn("h-px flex-1", i < step ? "bg-signal/40" : "bg-line")} />}
        </div>
      ))}
    </div>
  );
}

interface PickerItem {
  type: string;
  label: string;
  badge?: string;
}

function ConnectorPicker({
  items,
  selected,
  onSelect,
  error,
}: {
  items: PickerItem[];
  selected: string;
  onSelect: (type: string) => void;
  error?: string;
}) {
  return (
    <div>
      <span className="mb-1 block text-xs font-medium text-muted">Connector</span>
      <div className="grid grid-cols-3 gap-2">
        {items.map((c) => (
          <button
            key={c.type}
            onClick={() => onSelect(c.type)}
            className={cn(
              "flex flex-col items-start gap-1 rounded-control border px-3 py-2 text-left transition-colors",
              c.type === selected ? "border-signal bg-signal/10" : "border-line hover:border-signal/50",
            )}
          >
            <span className="text-sm text-text">{c.label}</span>
            {c.badge && (
              <Badge tone="signal" className="!text-[10px]">
                {c.badge}
              </Badge>
            )}
          </button>
        ))}
      </div>
      {error && <p className="mt-1 text-xs text-danger">{error}</p>}
    </div>
  );
}

function SettingsFields({
  fields,
  settings,
  errPrefix,
  errors,
  onChange,
}: {
  fields: ConnectorField[];
  settings: Record<string, SettingVal>;
  errPrefix: string;
  errors: Record<string, string>;
  onChange: (key: string, val: SettingVal) => void;
}) {
  if (fields.length === 0) return null;
  return (
    <div className="grid grid-cols-2 gap-3">
      {fields.map((f) => (
        <div key={f.key} className={f.type === "string-list" ? "col-span-2" : ""}>
          <FieldControl f={f} value={settings[f.key]} err={errors[errPrefix + f.key]} onChange={(v) => onChange(f.key, v)} />
        </div>
      ))}
    </div>
  );
}

function FieldControl({
  f,
  value,
  err,
  onChange,
}: {
  f: ConnectorField;
  value: SettingVal | undefined;
  err?: string;
  onChange: (val: SettingVal) => void;
}) {
  const label = f.required ? f.label : `${f.label} (optional)`;

  if (f.type === "boolean") {
    const checked = value === true;
    return (
      <Field label={label} hint={f.hint}>
        <label className="flex items-center gap-2 text-sm text-text">
          <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
          <span className="text-muted">{f.hint || "Enabled"}</span>
        </label>
      </Field>
    );
  }

  if (f.type === "enum") {
    return (
      <Field label={label} error={err} hint={f.hint}>
        <Select value={typeof value === "string" ? value : f.default ?? ""} onChange={(e) => onChange(e.target.value)}>
          {(f.enum ?? []).map((opt) => (
            <option key={opt} value={opt}>
              {opt === "" ? "—" : opt}
            </option>
          ))}
        </Select>
      </Field>
    );
  }

  if (f.type === "string-list") {
    const items = Array.isArray(value) && value.length > 0 ? value : [""];
    const update = (i: number, v: string) => onChange(items.map((x, j) => (j === i ? v : x)));
    const add = () => onChange([...items, ""]);
    const remove = (i: number) => onChange(items.filter((_, j) => j !== i));
    return (
      <Field label={label} error={err} hint={f.hint}>
        <div className="space-y-2">
          {items.map((item, i) => (
            <div key={i} className="flex gap-2">
              <Input value={item} placeholder={f.hint} onChange={(e) => update(i, e.target.value)} />
              {items.length > 1 && (
                <button className="px-2 text-faint hover:text-danger" onClick={() => remove(i)} type="button" aria-label="Remove">
                  −
                </button>
              )}
            </div>
          ))}
          <button className="text-xs text-signal hover:underline" onClick={add} type="button">
            + Add
          </button>
        </div>
      </Field>
    );
  }

  // string / integer
  return (
    <Field label={label} error={err} hint={f.hint}>
      <Input
        type={f.secret ? "password" : f.type === "integer" ? "number" : "text"}
        value={typeof value === "string" ? value : ""}
        invalid={!!err}
        placeholder={f.default || f.hint}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

function Review({ s }: { s: State }) {
  const row = (label: string, value: string) => (
    <div className="flex justify-between border-b border-line py-2 last:border-0">
      <span className="text-xs text-faint">{label}</span>
      <span className="tnum text-xs text-text">{value}</span>
    </div>
  );
  return (
    <div className="space-y-1">
      {row("Name", s.name || "—")}
      {row("Environment", s.environment)}
      {row("Schedule", s.schedule || "Manual")}
      {row("Source", s.source.type)}
      {row("Destination", s.destination.type)}
      {row("Streams", s.streams.map((st) => `${st.name} (${st.mode})`).join(", ") || "—")}
    </div>
  );
}
```

> Note: the original `Review` renders a summary table. If the existing file's `Review` differs (e.g. shows more rows), preserve its exact JSX below the `function Review` line rather than the version above — the fields it reads (`s.name`, `s.environment`, `s.schedule`, `s.source.type`, `s.destination.type`, `s.streams`) are unchanged, so no other edit is needed. Read the current `Review` (CreatePipeline.tsx lines ~344-360) before overwriting and keep its body.

- [ ] **Step 4: Typecheck, lint, and build the frontend**

Run: `cd frontend && npx tsc --noEmit && npm run lint && npm run build`
Expected: no type errors, no lint errors, Vite build succeeds. (Use the exact scripts the repo defines; if `npm run lint` differs, match `frontend/package.json`.)

- [ ] **Step 5: Manual verification against a live backend**

Run the stack (`cd deploy && docker compose up -d --build`), open the wizard, and confirm:
- All 16 sources (12 connector types) appear with correct maturity badges and **no "Coming soon"**.
- Selecting Kafka shows a repeatable `brokers` editor; Postgres shows a numeric `port` and an `sslmode` dropdown and a `cdc_auto_create` checkbox.
- Creating a SQLite→Parquet pipeline returns 201 and navigates to its detail page.

Record the result. If the backend isn't reachable, note that this step is deferred (build gate still enforces correctness) — do not claim it passed without running it.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/api.ts frontend/src/lib/hooks.ts frontend/src/pages/CreatePipeline.tsx
git commit -m "feat(frontend): registry-driven connector wizard (all 16 sources, typed forms)

$(printf 'Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>')"
```

---

## Self-Review Notes

- **Spec coverage:** Component 1 (engine `catalog` verb) → Task 1. Component 2 (backend `/connectors` + normalization + memoization) → Tasks 3–4. Component 3 (frontend catalog-driven wizard) → Task 5. Error handling (502 on engine failure, skip bad schema) → Tasks 3–4. The spec's implicit requirement that string-list fields "submit as arrays" surfaced a `map[string]string` limitation the spec missed → Task 2 (approved scope extension: widen to `map[string]any`, which also fixes typed integer/boolean fields).
- **Known limitation carried forward:** Object Storage still exposes S3 + Azure fields flat (labeled per provider in its schema); provider-conditional hiding remains a v1 non-goal.
- **Type consistency:** `normalizeCatalog` (Task 3) is the exact function Task 4's handler calls; `ConnectorCatalog`/`Field` JSON shapes match the frontend `ConnectorCatalog`/`ConnectorField` TS types (Task 5). `runner.Engine.Catalog`, `(*Runner).Catalog`, and `pipelineRunner.Catalog` share one signature `(ctx) ([]byte, error)`.
- **Follow-on (separate specs, per the design doc):** B — roadmap/family-preset connectors (they appear automatically once registered); C — website improvements.
```