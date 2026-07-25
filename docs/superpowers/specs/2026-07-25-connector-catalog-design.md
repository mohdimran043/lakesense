# Registry-driven Connector Catalog — Design

**Date:** 2026-07-25
**Status:** Approved (brainstorm)

## Problem

The create-pipeline wizard's source list is hardcoded in the frontend
(`frontend/src/pages/CreatePipeline.tsx`, `SOURCES` array) and has drifted badly
out of date. It lists only 5 sources — postgres + sqlite as ready, and
mysql/mongodb/kafka as "Coming soon" — while the engine registry now ships **16
verified sources across 12 registered connector types** (objectstore serves
S3/GCS/MinIO/Azure, elasticsearch serves ES/OpenSearch, cassandra serves
Cassandra/ScyllaDB, each via presets). Most are invisible in the UI, and the
three "Coming soon" entries actually ship.

Root cause: nothing downstream of the engine is driven by the engine registry.

- Engine (`engine/internal/cli/cli.go`): `spec --connector <type>` returns **one**
  connector's Spec; there is no "list all" verb, though `Registry.Names()`
  (`engine/internal/sdk/sdk.go`) enumerates all of them.
- Backend (`backend/internal/api/handlers.go`): no `/connectors` endpoint — the
  API never surfaces the registry.
- Frontend: hardcoded `SOURCES`, so it drifts every time a connector ships.

## Goal

Make the wizard's connector list and per-connector config form **derive from the
engine registry**, so it can never drift again. Every connector that ships in the
engine appears automatically; roadmap-only connectors (not registered) stay
honestly absent.

## Non-goals

- Destinations stay hardcoded (parquet/ndjson are engine *writers*, not registry
  connectors — a separate concept with no Spec).
- No provider-conditional field hiding in v1 (see Known Limitations).
- No new connectors in this work (that is follow-on sub-project B).

## Architecture & data flow

```
engine registry (16 specs)
      │  lsengine catalog  → JSON []Spec
      ▼
backend  Engine.Catalog() seam (shells the binary, like Discover/Sync/Backfill)
      │  normalize each Spec.ConfigSchema → flat Fields[]   (memoized once/process)
      ▼
GET /api/v1/connectors  → []ConnectorCatalog
      ▼
frontend  useConnectors() → ConnectorPicker + SettingsFields (fully data-driven)
```

The engine remains the single source of truth. The catalog is static per engine
build, so the backend fetches once and memoizes.

## Component 1 — Engine `catalog` verb

New CLI verb in `engine/internal/cli/cli.go` (a new verb, **not** an overload of
`spec`, whose "`--connector` is required" contract is asserted by an existing
test):

- Iterate `registry.Default()` via `Registry.Names()`, `New(name).Spec()` for
  each, collect into `[]sdk.Spec`, write as a JSON array to stdout.
- Deterministic order (sort by Type).

**Test:** `catalog` returns a JSON array containing every registered connector
type (postgres, mysql, mongodb, mssql, clickhouse, cassandra, elasticsearch,
kafka, objectstore, redis, dynamodb, sqlite), each with a non-empty DisplayName.

## Component 2 — Backend `GET /connectors` + normalization

**Engine seam** (`backend/internal/runner/engine.go` + interface in `runner.go`):
add `Catalog(ctx) ([]byte, error)`; `execEngine` shells `lsengine catalog`. The
fake engine used in tests returns canned Spec JSON.

**Normalizer** (`backend/internal/api/connectorcatalog.go`, new): parse each
`Spec.ConfigSchema` (a small JSON-Schema subset) into normalized fields.

```go
type Field struct {
    Key      string   `json:"key"`
    Label    string   `json:"label"`
    Type     string   `json:"type"`     // string | integer | boolean | string-list | enum
    Secret   bool     `json:"secret,omitempty"`
    Required bool     `json:"required,omitempty"`
    Default  string   `json:"default,omitempty"`
    Enum     []string `json:"enum,omitempty"`
    Hint     string   `json:"hint,omitempty"`
}

type ConnectorCatalog struct {
    Type         string   `json:"type"`
    Label        string   `json:"label"`         // Spec.DisplayName
    Maturity     string   `json:"maturity"`      // certified | stable | beta
    Capabilities []string `json:"capabilities"`
    Presets      []Preset `json:"presets,omitempty"`
    Fields       []Field  `json:"fields"`
}
```

Normalization rules (JSON-Schema property → Field):
- `format:"password"` → `Secret: true`.
- `enum` present → `Type:"enum"`, `Enum:[...]`.
- `type:"array"` with string items → `Type:"string-list"`.
- `type:"integer"` → `"integer"`; `type:"boolean"` → `"boolean"`; else `"string"`.
- membership in schema `required[]` → `Required: true`.
- `title` → `Label` (fallback: key); `description` → `Hint`; `default` →
  `Default` (stringified).
- Field order follows the schema's `properties` declaration order (preserved by
  decoding into an ordered structure, since Go maps don't preserve order — use a
  `json.RawMessage` + manual key-order pass, or the ordered-properties helper).

**Handler** (`backend/internal/api/handlers.go`): `r.Get("/connectors",
s.listConnectors)`. Calls `Engine.Catalog()`, normalizes, returns
`[]ConnectorCatalog`. **Memoized** on first success (sync.Once-style) — the
catalog can't change without a new engine binary. On engine failure (binary
missing/errors), return 502 with a clear message; do not cache the failure.

**Tests:**
- Normalizer unit tests over representative schemas: password→secret,
  enum→enum+options, array→string-list, integer→integer, required propagation,
  title/description/default mapping, property order preserved.
- Handler test with a fake Engine returning canned specs → asserts the shaped
  JSON and that a second call does not re-invoke the engine (memoized).

## Component 3 — Frontend wizard reads the catalog

`frontend/src/lib/api.ts`: add the `ConnectorCatalog`/`Field` types and a
`useConnectors()` query hitting `/connectors`.

`frontend/src/pages/CreatePipeline.tsx`:
- Delete the hardcoded `SOURCES` array; source the list from `useConnectors()`.
- Loading state while fetching; error state on failure (retry). **No hardcoded
  fallback** — a silent fallback is exactly what caused the drift.
- Default the selected source to the first catalog entry (or postgres if present).
- `SettingsFields` renders by normalized `Field.type`:
  - `string` → text `Input` (password when `secret`),
  - `integer` → numeric `Input`,
  - `boolean` → checkbox,
  - `enum` → `Select` over `Field.enum`,
  - `string-list` → **repeatable +/− row editor**; joined into an array on
    submit.
- `Maturity` drives the badge; there is no "Coming soon" — everything listed
  ships. Required-field validation reads `Field.required`.
- Destinations remain the existing hardcoded `DESTINATIONS`.

The submitted `settings` object keeps the same shape the write API already
accepts (`string-list` fields submit as arrays; scalar fields as strings).

## Error handling

- Engine binary missing/failing → backend 502, wizard shows an error with retry.
- A connector whose ConfigSchema fails to parse → normalizer logs and skips that
  connector's fields (renders the connector with an empty form rather than
  failing the whole catalog), so one bad schema can't blank the wizard.

## Known limitations (v1)

- **Object Storage** exposes S3 and Azure fields flat; its schema labels them
  "(s3)"/"(azure)". Provider-conditional field hiding (driven by the `provider`
  enum) is a clean follow-up, not v1.

## Testing summary

- Engine: catalog-verb test (all types present).
- Backend: normalizer unit tests + memoized-handler test with a fake engine.
- Frontend: builds under existing `make check` (tsc strict + oxlint + vite).
- Manual: run the wizard against a live backend, confirm all 16 sources appear
  with correct badges and forms.

## Follow-on work (separate specs, after this ships)

- **B. Missing/roadmap connectors** — MySQL/PG family presets (MariaDB, Aurora,
  CockroachDB, TimescaleDB, YugabyteDB, AlloyDB, Percona, TiDB), surfacing
  ScyllaDB (already a cassandra preset). **Honesty constraint:** Oracle and DB2
  need heavy/licensed images that can't be verified locally, so they cannot be
  badged shipped/verified the way the others were — they stay roadmap. Each of
  these appears in the wizard automatically once registered, thanks to this work.
- **C. Website improvements** — scope TBD in its own brainstorm.
