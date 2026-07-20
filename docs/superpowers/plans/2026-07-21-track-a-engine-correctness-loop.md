# Track A — Engine Correctness Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `lsengine` a real Parquet destination and working `verify`/`backfill` verbs, so a sync produces open-lakehouse output whose current-state correctness can be proven on demand and repaired without a full reload.

**Architecture:** A new `parquet` destination writes an immutable part-file per flush boundary (the Parquet dataset model), reusing the existing `syncrun.Writer` contract and `hashRow` checksum. A new consumer-side `Reader` reads any destination back as *current logical state* (latest row per `_ls_id`, deletes omitted). `verify` compares source vs destination current-state digests and bisects PK ranges on mismatch; `backfill` re-reads a bounded slice and appends merge-on-read corrections, never touching CDC/cursor/chunk state.

**Tech Stack:** Go 1.26, `github.com/parquet-go/parquet-go` (pure-Go Parquet), existing engine packages (`syncrun`, `events`, `sdk`, `model`, `state`, `cli`), `testify`, `modernc.org/sqlite` for real-DB tests.

## Global Constraints

- Go 1.26.1; engine module `github.com/lakesense/lakesense/engine`; standard `cmd/` + `internal/` layout.
- No CGo — `lsengine` builds static/distroless. `parquet-go` is pure Go; do not add Arrow/CGo deps.
- Errors wrapped with `%w`, handled at boundaries, never silently discarded; every function takes/propagates `context.Context` where I/O happens.
- Interfaces defined consumer-side (the `Reader` lives in `syncrun`, next to `Writer`).
- Table-driven `testify` tests, run with `-race`. `make check` (lint 0, vet clean, `-race`) must pass before a milestone is marked done.
- Metadata columns are `model.ColRecordID = "_ls_id"`, `ColIngestedAt = "_ls_ingested_at"`, `ColOpType = "_ls_op"` (values `r|c|i|u|d`), `ColCDCTimestamp = "_ls_cdc_timestamp"`. `_ls_`-prefixed columns are excluded from checksums (`isMetadataColumn`).
- Checksums use the existing `hashRow`/`digest` in `engine/internal/syncrun/rowdigest.go` — do not fork it.
- Data-correctness features get the deepest tests (Rule 6): a wrong "verified" result is worse than none.
- Commit after each task. Work on branch `track-a-engine-correctness`; PR to `main` when all three milestones + `make check`/`make verify` are green. `PROGRESS.md` is gitignored — update it locally, do not `git add` it.

---

## File Structure

- `engine/internal/syncrun/parquetschema.go` — lake-type→Parquet schema builder + row⇄`parquet.Row` conversion. One responsibility: schema/value mapping.
- `engine/internal/syncrun/parquet.go` — the `parquet` `Writer` (part-file-per-flush).
- `engine/internal/syncrun/reader.go` — the `Reader`/`StreamReader` interface + `OpenReader` factory + `PKRange` + the current-state resolver shared by all readers.
- `engine/internal/syncrun/ndjson_reader.go` — ndjson `Reader` implementation.
- `engine/internal/syncrun/parquet_reader.go` — parquet `Reader` implementation.
- `engine/internal/verify/verify.go` — `verify` engine op (digests, bisection, sample PKs) emitting `verify_result`.
- `engine/internal/backfill/backfill.go` — `backfill` engine op (bounded read, merge append, state-safe), calls `verify` at the end.
- `engine/internal/cli/cli.go` — replace the `verify`/`backfill` stubs with real command handlers.
- Tests alongside each: `*_test.go` in the same package.
- `scripts/verify-migration.sh` — add a Parquet round-trip + `verify` assertion.

Corresponding `_test.go` files are created in the same package as each source file.

---

## Milestone 2.5 — Parquet writer

### Task 1: Add parquet-go dependency and the schema builder

**Files:**
- Modify: `engine/go.mod` (add dependency)
- Create: `engine/internal/syncrun/parquetschema.go`
- Test: `engine/internal/syncrun/parquetschema_test.go`

**Interfaces:**
- Produces: `buildParquetSchema(stream model.Stream) (*parquet.Schema, []string)` — returns the schema and the ordered leaf column names (from `schema.Columns()`); `rowToParquet(row sdk.Row, colIndex map[string]int, colType map[string]model.DataType, n int) parquet.Row`; `parquetToRow(pr parquet.Row, names []string) sdk.Row`.

- [ ] **Step 1: Add the dependency**

Run:
```bash
cd engine && go get github.com/parquet-go/parquet-go@latest && go mod tidy
```
Expected: `go.mod` gains `github.com/parquet-go/parquet-go` in the require block; `go mod tidy` exits 0.

- [ ] **Step 2: Write the failing test**

Create `engine/internal/syncrun/parquetschema_test.go`:
```go
package syncrun

import (
	"testing"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/stretchr/testify/require"
)

func testStream() model.Stream {
	return model.Stream{
		Namespace: "public", Name: "orders",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "amount", Type: model.TypeDecimal},
			{Name: "note", Type: model.TypeString, Nullable: true},
			{Name: "ok", Type: model.TypeBool},
			{Name: model.ColRecordID, Type: model.TypeString},
			{Name: model.ColIngestedAt, Type: model.TypeTimestamp},
			{Name: model.ColOpType, Type: model.TypeString},
		}},
	}
}

func TestBuildParquetSchemaRoundTrip(t *testing.T) {
	stream := testStream()
	schema, names := buildParquetSchema(stream)
	require.NotNil(t, schema)
	require.ElementsMatch(t, []string{"id", "amount", "note", "ok", "_ls_id", "_ls_ingested_at", "_ls_op"}, names)

	colIndex := map[string]int{}
	for i, n := range names {
		colIndex[n] = i
	}
	colType := map[string]model.DataType{}
	for _, c := range stream.Schema.Columns {
		colType[c.Name] = c.Type
	}

	row := sdk.Row{"id": int64(7), "amount": "12.34", "note": nil, "ok": true,
		"_ls_id": "abc", "_ls_ingested_at": "2026-07-21T00:00:00Z", "_ls_op": "r"}
	pr := rowToParquet(row, colIndex, colType, len(names))
	back := parquetToRow(pr, names)

	require.Equal(t, int64(7), back["id"])
	require.Equal(t, "12.34", back["amount"])
	require.Nil(t, back["note"])
	require.Equal(t, true, back["ok"])
	require.Equal(t, "abc", back["_ls_id"])
}
```
Add the `sdk` import (`github.com/lakesense/lakesense/engine/internal/sdk`).

- [ ] **Step 3: Run test to verify it fails**

Run: `cd engine && go test ./internal/syncrun/ -run TestBuildParquetSchemaRoundTrip`
Expected: FAIL — `undefined: buildParquetSchema`.

- [ ] **Step 4: Implement `parquetschema.go`**

Create `engine/internal/syncrun/parquetschema.go`:
```go
package syncrun

import (
	"encoding/json"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

// buildParquetSchema maps a stream's lake schema (data columns + engine _ls_
// metadata) onto a flat, all-optional Parquet schema. parquet-go orders Group
// fields alphabetically, so the returned names slice — taken from the built
// schema — is the authoritative column order for row assembly.
func buildParquetSchema(stream model.Stream) (*parquet.Schema, []string) {
	group := parquet.Group{}
	for _, c := range stream.Schema.Columns {
		group[c.Name] = parquet.Optional(parquet.Leaf(parquetType(c.Type)))
	}
	schema := parquet.NewSchema(stream.ID(), group)
	cols := schema.Columns()
	names := make([]string, len(cols))
	for i, path := range cols {
		names[i] = path[len(path)-1]
	}
	return schema, names
}

// parquetType maps a lake type to a Parquet physical type. Types without a
// native Parquet primitive (decimal, timestamp, json, array) are stored as
// UTF8 strings in v0.1 to preserve exactness; documented in the design.
func parquetType(t model.DataType) parquet.Type {
	switch t {
	case model.TypeBool:
		return parquet.BooleanType
	case model.TypeInt32:
		return parquet.Int32Type
	case model.TypeInt64:
		return parquet.Int64Type
	case model.TypeFloat32:
		return parquet.FloatType
	case model.TypeFloat64:
		return parquet.DoubleType
	case model.TypeBinary:
		return parquet.ByteArrayType
	default: // string, decimal, date, timestamp, json, array
		return parquet.ByteArrayType
	}
}

// rowToParquet builds a parquet.Row of length n, placing each column's value at
// its schema index. A nil/absent value becomes a null at definition level 0;
// present values are normalized to the Go kind matching their leaf type.
func rowToParquet(row sdk.Row, colIndex map[string]int, colType map[string]model.DataType, n int) parquet.Row {
	pr := make(parquet.Row, n)
	for name, idx := range colIndex {
		v, present := row[name]
		if !present || v == nil {
			pr[idx] = parquet.NullValue().Level(0, 0, idx)
			continue
		}
		pr[idx] = parquet.ValueOf(normalizeValue(v, colType[name])).Level(0, 1, idx)
	}
	return pr
}

// normalizeValue coerces a Go value read from a connector into the concrete Go
// type the column's Parquet leaf expects, so parquet.ValueOf boxes it into the
// right kind.
func normalizeValue(v any, t model.DataType) any {
	switch t {
	case model.TypeBool:
		b, _ := v.(bool)
		return b
	case model.TypeInt32:
		return int32(toInt64(v))
	case model.TypeInt64:
		return toInt64(v)
	case model.TypeFloat32:
		return float32(toFloat64(v))
	case model.TypeFloat64:
		return toFloat64(v)
	case model.TypeBinary:
		if b, ok := v.([]byte); ok {
			return b
		}
		return []byte(toString(v))
	default:
		return toString(v) // string, decimal, date, timestamp, json, array
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		i, _ := x.Int64()
		return i
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// parquetToRow reconstructs an sdk.Row from a raw parquet.Row using the schema's
// column order. Values are decoded to the Go types the digest and JSON encoder
// expect (int64, float64, bool, string).
func parquetToRow(pr parquet.Row, names []string) sdk.Row {
	row := make(sdk.Row, len(names))
	for _, v := range pr {
		idx := v.Column()
		if idx < 0 || idx >= len(names) {
			continue
		}
		name := names[idx]
		if v.IsNull() {
			row[name] = nil
			continue
		}
		switch v.Kind() {
		case parquet.Boolean:
			row[name] = v.Boolean()
		case parquet.Int32:
			row[name] = int64(v.Int32())
		case parquet.Int64:
			row[name] = v.Int64()
		case parquet.Float:
			row[name] = float64(v.Float())
		case parquet.Double:
			row[name] = v.Double()
		default:
			row[name] = v.String()
		}
	}
	return row
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd engine && go test ./internal/syncrun/ -run TestBuildParquetSchemaRoundTrip`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/go.mod engine/go.sum engine/internal/syncrun/parquetschema.go engine/internal/syncrun/parquetschema_test.go
git commit -m "feat(engine): parquet schema builder + row conversion (2.5)"
```

### Task 2: Parquet writer (part-file per flush) wired into OpenWriter

**Files:**
- Create: `engine/internal/syncrun/parquet.go`
- Modify: `engine/internal/syncrun/writer.go:64-75` (`OpenWriter` switch + doc)
- Test: `engine/internal/syncrun/parquet_test.go`

**Interfaces:**
- Consumes: `buildParquetSchema`, `rowToParquet`, `parquetToRow`, `hashRow`, `digest`, `dataColumns` (Task 1 + existing).
- Produces: `newParquetWriter(dir string) (*parquetWriter, error)` implementing `Writer`; part-files at `<dir>/<namespace.name>/<part>.parquet`.

- [ ] **Step 1: Write the failing test**

Create `engine/internal/syncrun/parquet_test.go`:
```go
package syncrun

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := newParquetWriter(dir)
	require.NoError(t, err)
	stream := testStream()

	sw, err := w.Open(context.Background(), stream, "", true)
	require.NoError(t, err)

	rows := []sdk.Row{
		{"id": int64(1), "amount": "10.00", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"},
		{"id": int64(2), "amount": "20.00", "note": nil, "ok": false, "_ls_id": "2", "_ls_ingested_at": "t1", "_ls_op": "r"},
	}
	for _, r := range rows {
		require.NoError(t, sw.WriteRow(context.Background(), r))
	}
	require.NoError(t, sw.Flush(context.Background()))
	res, err := sw.Close(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Rows)
	require.NoError(t, w.Close(context.Background()))

	// The written part-file(s) are valid Parquet with 2 rows total.
	parts, _ := filepath.Glob(filepath.Join(dir, "public.orders", "*.parquet"))
	require.NotEmpty(t, parts)
	var total int64
	for _, p := range parts {
		f, err := os.Open(p)
		require.NoError(t, err)
		st, _ := f.Stat()
		pf, err := parquet.OpenFile(f, st.Size())
		require.NoError(t, err)
		total += pf.NumRows()
		_ = f.Close()
	}
	require.Equal(t, int64(2), total)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/syncrun/ -run TestParquetWriterRoundTrip`
Expected: FAIL — `undefined: newParquetWriter`.

- [ ] **Step 3: Implement `parquet.go`**

Create `engine/internal/syncrun/parquet.go`:
```go
package syncrun

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

// parquetWriter writes each stream as a directory of immutable part-files. One
// part-file is finalized per Flush boundary (footer + fsync), so a completed
// chunk maps to a durable, readable file — the write-ahead discipline the
// orchestrator already enforces, satisfied without changes to the runner.
type parquetWriter struct {
	dir string
}

func newParquetWriter(dir string) (*parquetWriter, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet destination requires a path")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create destination dir %s: %w", dir, err)
	}
	return &parquetWriter{dir: dir}, nil
}

func (w *parquetWriter) Open(_ context.Context, stream model.Stream, destTable string, truncate bool) (StreamWriter, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	streamDir := filepath.Join(w.dir, name)
	if err := os.MkdirAll(streamDir, 0o755); err != nil {
		return nil, fmt.Errorf("create stream dir %s: %w", streamDir, err)
	}
	if truncate {
		existing, _ := filepath.Glob(filepath.Join(streamDir, "*.parquet"))
		for _, p := range existing {
			if err := os.Remove(p); err != nil {
				return nil, fmt.Errorf("truncate %s: %w", p, err)
			}
		}
	}
	schema, names := buildParquetSchema(stream)
	colIndex := make(map[string]int, len(names))
	for i, n := range names {
		colIndex[n] = i
	}
	colType := make(map[string]model.DataType, len(stream.Schema.Columns))
	for _, c := range stream.Schema.Columns {
		colType[c.Name] = c.Type
	}
	return &parquetStreamWriter{
		dir:      streamDir,
		syncID:   events.NewSyncID(),
		schema:   schema,
		names:    names,
		colIndex: colIndex,
		colType:  colType,
		columns:  dataColumns(stream),
	}, nil
}

func (w *parquetWriter) Close(context.Context) error { return nil }

type parquetStreamWriter struct {
	dir      string
	syncID   string
	schema   *parquet.Schema
	names    []string
	colIndex map[string]int
	colType  map[string]model.DataType
	columns  []string

	pending []parquet.Row
	seq     int
	digest  digest
	bytes   int64
	closed  bool
}

func (s *parquetStreamWriter) WriteRow(_ context.Context, row sdk.Row) error {
	h, err := hashRow(row, s.columns)
	if err != nil {
		return err
	}
	s.digest.add(h)
	s.pending = append(s.pending, rowToParquet(row, s.colIndex, s.colType, len(s.names)))
	return nil
}

// Flush finalizes the currently-buffered rows as one immutable part-file. It is
// a no-op when nothing is pending, so repeated Flush calls never emit empties.
func (s *parquetStreamWriter) Flush(context.Context) error {
	if s.closed {
		return fmt.Errorf("parquet stream writer for %s already closed", s.dir)
	}
	if len(s.pending) == 0 {
		return nil
	}
	path := filepath.Join(s.dir, fmt.Sprintf("%s-%04d.parquet", s.syncID, s.seq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open part %s: %w", path, err)
	}
	pw := parquet.NewWriter(f, s.schema)
	if _, err := pw.WriteRows(s.pending); err != nil {
		_ = f.Close()
		return fmt.Errorf("write parquet rows to %s: %w", path, err)
	}
	if err := pw.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("finalize parquet %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if st, err := os.Stat(path); err == nil {
		s.bytes += st.Size()
	}
	s.pending = s.pending[:0]
	s.seq++
	return nil
}

func (s *parquetStreamWriter) Close(ctx context.Context) (WriteResult, error) {
	if s.closed {
		return WriteResult{}, fmt.Errorf("parquet stream writer for %s already closed", s.dir)
	}
	if err := s.Flush(ctx); err != nil {
		return WriteResult{}, err
	}
	s.closed = true
	return WriteResult{Rows: s.digest.Rows(), Bytes: s.bytes, Checksum: s.digest.Hex()}, nil
}
```

- [ ] **Step 4: Wire into OpenWriter**

In `engine/internal/syncrun/writer.go`, change the `OpenWriter` switch (currently lines ~64-75) to add the parquet case and update the error hint:
```go
func OpenWriter(cfg DestinationConfig) (Writer, error) {
	switch cfg.Type {
	case "", "ndjson":
		if cfg.Path == "" {
			return nil, fmt.Errorf("ndjson destination requires a path")
		}
		return newNDJSONWriter(cfg.Path)
	case "parquet":
		return newParquetWriter(cfg.Path)
	default:
		return nil, fmt.Errorf("unknown destination type %q (v0.1 supports \"ndjson\" and \"parquet\"; iceberg lands in v0.2)", cfg.Type)
	}
}
```
Also update the `DestinationConfig.Type` doc comment (line ~59) to read `// "ndjson" | "parquet" (v0.1); "iceberg" in v0.2`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd engine && go test -race ./internal/syncrun/`
Expected: PASS (all syncrun tests, including the new one).

- [ ] **Step 6: Commit**

```bash
git add engine/internal/syncrun/parquet.go engine/internal/syncrun/parquet_test.go engine/internal/syncrun/writer.go
git commit -m "feat(engine): parquet destination writer, part-file per flush (2.5)"
```

### Task 3: Destination Reader (current-state) — interface, ndjson, parquet

**Files:**
- Create: `engine/internal/syncrun/reader.go`
- Create: `engine/internal/syncrun/ndjson_reader.go`
- Create: `engine/internal/syncrun/parquet_reader.go`
- Test: `engine/internal/syncrun/reader_test.go`

**Interfaces:**
- Produces:
  - `type PKRange struct { Min, Max string }`
  - `type Reader interface { OpenRead(ctx, stream model.Stream, destTable string) (StreamReader, error); Close(ctx) error }`
  - `type StreamReader interface { Read(ctx, bounds *PKRange, emit sdk.RowFunc) error; Close(ctx) error }`
  - `OpenReader(cfg DestinationConfig) (Reader, error)`
  - `func resolveCurrentState(rows []sdk.Row) []sdk.Row` — latest per `_ls_id`, deletes dropped.
- Consumes: `recordID`, `isMetadataColumn`, `model.ColRecordID`, `model.ColIngestedAt`, `model.ColOpType`.

- [ ] **Step 1: Write the failing test**

Create `engine/internal/syncrun/reader_test.go`:
```go
package syncrun

import (
	"context"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/stretchr/testify/require"
)

func TestResolveCurrentStateLatestWinsDeletesDrop(t *testing.T) {
	rows := []sdk.Row{
		{"_ls_id": "a", "_ls_ingested_at": "t1", "_ls_op": "r", "v": int64(1)},
		{"_ls_id": "a", "_ls_ingested_at": "t2", "_ls_op": "u", "v": int64(2)},
		{"_ls_id": "b", "_ls_ingested_at": "t1", "_ls_op": "r", "v": int64(9)},
		{"_ls_id": "b", "_ls_ingested_at": "t3", "_ls_op": "d", "v": int64(9)},
	}
	got := resolveCurrentState(rows)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0]["_ls_id"])
	require.Equal(t, int64(2), got[0]["v"])
}

func TestParquetReaderReadsBackWhatWriterWrote(t *testing.T) {
	dir := t.TempDir()
	w, err := newParquetWriter(dir)
	require.NoError(t, err)
	stream := testStream()
	sw, _ := w.Open(context.Background(), stream, "", true)
	_ = sw.WriteRow(context.Background(), sdk.Row{"id": int64(1), "amount": "10.00", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"})
	_ = sw.WriteRow(context.Background(), sdk.Row{"id": int64(2), "amount": "20.00", "note": nil, "ok": false, "_ls_id": "2", "_ls_ingested_at": "t1", "_ls_op": "r"})
	_, _ = sw.Close(context.Background())
	_ = w.Close(context.Background())

	r, err := OpenReader(DestinationConfig{Type: "parquet", Path: dir})
	require.NoError(t, err)
	sr, err := r.OpenRead(context.Background(), stream, "")
	require.NoError(t, err)
	var got []sdk.Row
	require.NoError(t, sr.Read(context.Background(), nil, func(_ context.Context, row sdk.Row) error {
		got = append(got, row)
		return nil
	}))
	require.Len(t, got, 2)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/syncrun/ -run 'TestResolveCurrentState|TestParquetReaderReadsBack'`
Expected: FAIL — `undefined: resolveCurrentState` / `OpenReader`.

- [ ] **Step 3: Implement `reader.go`**

Create `engine/internal/syncrun/reader.go`:
```go
package syncrun

import (
	"context"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// PKRange is a half-open [Min, Max) filter over a stream's record id; empty
// bounds are unbounded on that side.
type PKRange struct {
	Min string
	Max string
}

// Reader is the read-back side of a destination. verify and backfill use it to
// materialize the destination's current logical state.
type Reader interface {
	OpenRead(ctx context.Context, stream model.Stream, destTable string) (StreamReader, error)
	Close(ctx context.Context) error
}

// StreamReader yields the destination's current state for one stream: the
// latest row per _ls_id (max _ls_ingested_at), deletes omitted. bounds, when
// non-nil, restricts to record ids in [Min, Max).
type StreamReader interface {
	Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error
	Close(ctx context.Context) error
}

// OpenReader constructs the reader matching a destination config.
func OpenReader(cfg DestinationConfig) (Reader, error) {
	switch cfg.Type {
	case "", "ndjson":
		return newNDJSONReader(cfg.Path)
	case "parquet":
		return newParquetReader(cfg.Path)
	default:
		return nil, fmt.Errorf("no reader for destination type %q", cfg.Type)
	}
}

// resolveCurrentState collapses an append log into current state: for each
// _ls_id keep the row with the greatest _ls_ingested_at; drop ids whose latest
// op is delete. Ties break on later slice position (stable last-writer-wins).
func resolveCurrentState(rows []sdk.Row) []sdk.Row {
	type entry struct {
		row sdk.Row
		ts  string
	}
	latest := map[string]entry{}
	for _, row := range rows {
		id, _ := row[model.ColRecordID].(string)
		ts, _ := row[model.ColIngestedAt].(string)
		cur, ok := latest[id]
		if !ok || ts >= cur.ts {
			latest[id] = entry{row: row, ts: ts}
		}
	}
	out := make([]sdk.Row, 0, len(latest))
	for _, e := range latest {
		if op, _ := e.row[model.ColOpType].(string); op == "d" {
			continue
		}
		out = append(out, e.row)
	}
	return out
}

// inRange reports whether a row's record id falls inside bounds (nil = all).
func inRange(row sdk.Row, bounds *PKRange) bool {
	if bounds == nil {
		return true
	}
	id, _ := row[model.ColRecordID].(string)
	if bounds.Min != "" && id < bounds.Min {
		return false
	}
	if bounds.Max != "" && id >= bounds.Max {
		return false
	}
	return true
}
```

- [ ] **Step 4: Implement `ndjson_reader.go`**

Create `engine/internal/syncrun/ndjson_reader.go`:
```go
package syncrun

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

type ndjsonReader struct{ dir string }

func newNDJSONReader(dir string) (*ndjsonReader, error) {
	if dir == "" {
		return nil, fmt.Errorf("ndjson reader requires a path")
	}
	return &ndjsonReader{dir: dir}, nil
}

func (r *ndjsonReader) OpenRead(_ context.Context, stream model.Stream, destTable string) (StreamReader, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	return &ndjsonStreamReader{path: filepath.Join(r.dir, name+".ndjson")}, nil
}

func (r *ndjsonReader) Close(context.Context) error { return nil }

type ndjsonStreamReader struct{ path string }

func (s *ndjsonStreamReader) Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // empty destination
		}
		return fmt.Errorf("open %s: %w", s.path, err)
	}
	defer func() { _ = f.Close() }()

	var all []sdk.Row
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var row sdk.Row
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			continue // tolerate a torn trailing line
		}
		all = append(all, row)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", s.path, err)
	}
	for _, row := range resolveCurrentState(all) {
		if !inRange(row, bounds) {
			continue
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *ndjsonStreamReader) Close(context.Context) error { return nil }

var _ model.DataType // keep model import if unused after edits
```
Remove the `var _ model.DataType` line if `model` is already referenced (it is, via `OpenRead`); it is only a guard — delete before committing if `go vet` flags it.

- [ ] **Step 5: Implement `parquet_reader.go`**

Create `engine/internal/syncrun/parquet_reader.go`:
```go
package syncrun

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

type parquetReader struct{ dir string }

func newParquetReader(dir string) (*parquetReader, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet reader requires a path")
	}
	return &parquetReader{dir: dir}, nil
}

func (r *parquetReader) OpenRead(_ context.Context, stream model.Stream, destTable string) (StreamReader, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	return &parquetStreamReader{dir: filepath.Join(r.dir, name)}, nil
}

func (r *parquetReader) Close(context.Context) error { return nil }

type parquetStreamReader struct{ dir string }

func (s *parquetStreamReader) Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error {
	parts, err := filepath.Glob(filepath.Join(s.dir, "*.parquet"))
	if err != nil {
		return fmt.Errorf("glob %s: %w", s.dir, err)
	}
	sort.Strings(parts) // ascending: later part-files carry newer writes
	var all []sdk.Row
	for _, p := range parts {
		rows, err := readParquetPart(p)
		if err != nil {
			return err
		}
		all = append(all, rows...)
	}
	for _, row := range resolveCurrentState(all) {
		if !inRange(row, bounds) {
			continue
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *parquetStreamReader) Close(context.Context) error { return nil }

// readParquetPart reads one part-file, recovering column names from its footer
// schema so decoding never depends on the writer's in-memory ordering.
func readParquetPart(path string) ([]sdk.Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return nil, fmt.Errorf("parse parquet %s: %w", path, err)
	}
	cols := pf.Schema().Columns()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c[len(c)-1]
	}
	reader := parquet.NewGenericReader[any](pf)
	defer func() { _ = reader.Close() }()

	var out []sdk.Row
	buf := make([]parquet.Row, 256)
	for {
		n, err := reader.ReadRows(buf)
		for i := 0; i < n; i++ {
			out = append(out, parquetToRow(buf[i], names))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read rows %s: %w", path, err)
		}
	}
	return out, nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd engine && go test -race ./internal/syncrun/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add engine/internal/syncrun/reader.go engine/internal/syncrun/ndjson_reader.go engine/internal/syncrun/parquet_reader.go engine/internal/syncrun/reader_test.go
git commit -m "feat(engine): destination Reader with current-state resolution (ndjson+parquet)"
```

### Task 4: Parquet crash-resume test (durability of part-files)

**Files:**
- Test: `engine/internal/syncrun/parquet_test.go` (add a case)

**Interfaces:**
- Consumes: `newParquetWriter`, `OpenReader`, existing `digest`/`hashRow`.

- [ ] **Step 1: Write the failing test**

Add to `engine/internal/syncrun/parquet_test.go`:
```go
func TestParquetResumeAppendsWithoutLoss(t *testing.T) {
	dir := t.TempDir()
	stream := testStream()

	// First run: write one flushed part-file, then simulate a crash (no truncate on resume).
	w1, _ := newParquetWriter(dir)
	sw1, _ := w1.Open(context.Background(), stream, "", true)
	_ = sw1.WriteRow(context.Background(), sdk.Row{"id": int64(1), "amount": "1", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"})
	require.NoError(t, sw1.Flush(context.Background())) // durable part-file
	// crash: do NOT Close sw1; drop the writer.

	// Resume: append (truncate=false) the second chunk.
	w2, _ := newParquetWriter(dir)
	sw2, _ := w2.Open(context.Background(), stream, "", false)
	_ = sw2.WriteRow(context.Background(), sdk.Row{"id": int64(2), "amount": "2", "note": "b", "ok": false, "_ls_id": "2", "_ls_ingested_at": "t2", "_ls_op": "r"})
	_, _ = sw2.Close(context.Background())
	_ = w2.Close(context.Background())

	r, _ := OpenReader(DestinationConfig{Type: "parquet", Path: dir})
	sr, _ := r.OpenRead(context.Background(), stream, "")
	ids := map[string]bool{}
	_ = sr.Read(context.Background(), nil, func(_ context.Context, row sdk.Row) error {
		ids[row["_ls_id"].(string)] = true
		return nil
	})
	require.True(t, ids["1"], "row from before the crash must survive")
	require.True(t, ids["2"], "row from the resumed run must be present")
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cd engine && go test -race ./internal/syncrun/ -run TestParquetResumeAppendsWithoutLoss`
Expected: PASS (the design already supports this; this test locks it in).

- [ ] **Step 3: Run full make check**

Run: `cd /home/imran/Documents/github/LakeSense && make check`
Expected: lint 0 issues, vet clean, all `-race` tests pass, frontend+website build.

- [ ] **Step 4: Commit + update PROGRESS locally**

```bash
git add engine/internal/syncrun/parquet_test.go
git commit -m "test(engine): parquet crash-resume append without loss (2.5)"
```
Then edit `PROGRESS.md` locally: check `2.5 Writers: Parquet` (Iceberg remains roadmap), add a Decisions Log line for the part-file-per-flush layout. Do not `git add PROGRESS.md` (gitignored).

---

## Milestone 2.7 — `lsengine verify`

### Task 5: verify core — whole-stream digests + match path

**Files:**
- Create: `engine/internal/verify/verify.go`
- Test: `engine/internal/verify/verify_test.go`

**Interfaces:**
- Produces: `func Run(ctx context.Context, opts Options) (events.VerifyResult, error)` per stream driver `func VerifyStream(ctx, in StreamInput) (events.VerifyResult, error)`.
  - `type StreamInput struct { Stream model.Stream; Source sdk.FullLoader; DestReader syncrun.StreamReader; SampleThreshold, SampleCap int }`
- Consumes: `sdk.FullLoader.ReadChunk`, `syncrun.StreamReader.Read`, `syncrun` row hashing (expose a helper).

**Note:** `verify` needs the same `hashRow` the writer uses. Export a thin wrapper from `syncrun` so `verify` shares the exact function:
add to `engine/internal/syncrun/rowdigest.go`:
```go
// HashDataColumns hashes a row over the given data columns (empty = all
// non-metadata columns). Exported so verify computes digests identically to
// the writer.
func HashDataColumns(row sdk.Row, columns []string) (uint64, error) { return hashRow(row, columns) }

// DataColumns returns a stream's non-metadata column names (exported).
func DataColumns(s model.Stream) []string { return dataColumns(s) }

// AggregateDigest is an exported order-independent aggregate for verify's
// range/whole-stream sums.
type AggregateDigest struct{ d digest }
func (a *AggregateDigest) Add(h uint64) { a.d.add(h) }
func (a *AggregateDigest) Rows() int64  { return a.d.Rows() }
func (a *AggregateDigest) Hex() string  { return a.d.Hex() }
```

- [ ] **Step 1: Write the failing test**

Create `engine/internal/verify/verify_test.go`:
```go
package verify

import (
	"context"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/stretchr/testify/require"
)

// fakeFullLoader replays a fixed row set; SplitChunks returns one unbounded chunk.
type fakeFullLoader struct{ rows []sdk.Row }

func (f *fakeFullLoader) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) {
	return []state.Chunk{{}}, nil
}
func (f *fakeFullLoader) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	for _, r := range f.rows {
		if err := emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// sliceReader is a StreamReader over an in-memory current-state slice.
type sliceReader struct{ rows []sdk.Row }

func (s *sliceReader) Read(ctx context.Context, _ *syncPKRange, emit sdk.RowFunc) error { // see note
	for _, r := range s.rows {
		if err := emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (s *sliceReader) Close(context.Context) error { return nil }

func TestVerifyMatch(t *testing.T) {
	stream := model.Stream{Namespace: "public", Name: "t", Schema: model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "v", Type: model.TypeString},
	}}}
	src := []sdk.Row{{"id": int64(1), "v": "a", "_ls_id": "1"}, {"id": int64(2), "v": "b", "_ls_id": "2"}}
	dst := []sdk.Row{{"id": int64(2), "v": "b", "_ls_id": "2"}, {"id": int64(1), "v": "a", "_ls_id": "1"}}

	res, err := VerifyStream(context.Background(), StreamInput{
		Stream: stream, Source: &fakeFullLoader{rows: src}, DestReader: &sliceReader{rows: dst},
	})
	require.NoError(t, err)
	require.True(t, res.Match)
	require.Equal(t, int64(2), res.SourceRows)
	require.Equal(t, int64(2), res.DestinationRows)
}
```
**Note on `syncPKRange`:** the test's `sliceReader` must satisfy `syncrun.StreamReader`, whose `Read` takes `*syncrun.PKRange`. Import `syncrun` and use `*syncrun.PKRange` in the signature (the `syncPKRange` placeholder above is illustrative — replace with `*syncrun.PKRange` and import the package). Adjust imports accordingly.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/verify/ -run TestVerifyMatch`
Expected: FAIL — `undefined: VerifyStream`.

- [ ] **Step 3: Implement the exported helpers in `rowdigest.go`** (shown above), then create `engine/internal/verify/verify.go`:
```go
// Package verify re-checks that a destination's current state matches its
// source, order-independently, and drills into offending PK ranges on
// mismatch. It backs `lsengine verify` and the closing check of a backfill.
package verify

import (
	"context"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
)

// DestOpener abstracts obtaining a bounded StreamReader (a fresh reader per
// range keeps each pass independent and memory-bounded).
type DestOpener interface {
	OpenRead(ctx context.Context, stream model.Stream, destTable string) (syncrun.StreamReader, error)
}

// StreamInput is everything VerifyStream needs for one stream.
type StreamInput struct {
	Stream          model.Stream
	Source          sdk.FullLoader
	DestReader      syncrun.StreamReader // whole-stream reader (bounds nil)
	SampleThreshold int                  // range size at which to enumerate (default 100)
	SampleCap       int                  // max sample PKs to report (default 20)
}

func (in *StreamInput) defaults() {
	if in.SampleThreshold <= 0 {
		in.SampleThreshold = 100
	}
	if in.SampleCap <= 0 {
		in.SampleCap = 20
	}
}

// VerifyStream computes source and destination digests and returns the result.
// Bisection (Task 6) fills MismatchedRanges/SamplePKs on mismatch.
func VerifyStream(ctx context.Context, in StreamInput) (events.VerifyResult, error) {
	in.defaults()
	cols := syncrun.DataColumns(in.Stream)

	src := &syncrun.AggregateDigest{}
	if err := readSource(ctx, in.Source, in.Stream, state.Chunk{}, cols, src); err != nil {
		return events.VerifyResult{}, err
	}
	dst := &syncrun.AggregateDigest{}
	if err := in.DestReader.Read(ctx, nil, func(_ context.Context, row sdk.Row) error {
		h, err := syncrun.HashDataColumns(row, cols)
		if err != nil {
			return err
		}
		dst.Add(h)
		return nil
	}); err != nil {
		return events.VerifyResult{}, err
	}

	res := events.VerifyResult{
		SourceRows:      src.Rows(),
		DestinationRows: dst.Rows(),
		Match:           src.Rows() == dst.Rows() && src.Hex() == dst.Hex(),
	}
	return res, nil
}

// readSource replays one source chunk into agg, hashing over cols.
func readSource(ctx context.Context, fl sdk.FullLoader, stream model.Stream, chunk state.Chunk, cols []string, agg *syncrun.AggregateDigest) error {
	return fl.ReadChunk(ctx, stream, chunk, func(_ context.Context, row sdk.Row) error {
		h, err := syncrun.HashDataColumns(row, cols)
		if err != nil {
			return fmt.Errorf("hash source row: %w", err)
		}
		agg.Add(h)
		return nil
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && go test -race ./internal/verify/ -run TestVerifyMatch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/internal/syncrun/rowdigest.go engine/internal/verify/verify.go engine/internal/verify/verify_test.go
git commit -m "feat(engine): verify core — source vs destination current-state digest (2.7)"
```

### Task 6: verify PK-range bisection + sample mismatched PKs

**Files:**
- Modify: `engine/internal/verify/verify.go` (add bisection on mismatch)
- Test: `engine/internal/verify/verify_test.go` (add mismatch case)

**Interfaces:**
- Consumes: `StreamInput` now also needs a bounded-reader opener. Extend `StreamInput` with `OpenBounded func(bounds *syncrun.PKRange) (syncrun.StreamReader, error)` and `SourceRange func(bounds *syncrun.PKRange) state.Chunk` (maps a record-id range to a source chunk; for the fake, unbounded).
- Produces: on mismatch, `VerifyStream` returns `Match:false` with `MismatchedRanges []events.Range` and `SamplePKs []string`.

- [ ] **Step 1: Write the failing test**

Add to `engine/internal/verify/verify_test.go`:
```go
func TestVerifyDetectsDroppedRow(t *testing.T) {
	stream := model.Stream{Namespace: "public", Name: "t", Schema: model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "v", Type: model.TypeString},
	}}}
	src := []sdk.Row{
		{"id": int64(1), "v": "a", "_ls_id": "1"},
		{"id": int64(2), "v": "b", "_ls_id": "2"},
		{"id": int64(3), "v": "c", "_ls_id": "3"},
	}
	dst := []sdk.Row{ // "2" dropped on the destination
		{"id": int64(1), "v": "a", "_ls_id": "1"},
		{"id": int64(3), "v": "c", "_ls_id": "3"},
	}
	rd := &sliceReader{rows: dst}
	res, err := VerifyStream(context.Background(), StreamInput{
		Stream: stream, Source: &fakeFullLoader{rows: src}, DestReader: rd,
		OpenBounded: func(*syncrun.PKRange) (syncrun.StreamReader, error) { return &sliceReader{rows: dst}, nil },
	})
	require.NoError(t, err)
	require.False(t, res.Match)
	require.Contains(t, res.SamplePKs, "2")
}
```
(`sliceReader.Read` must honor `bounds` via `syncrun`-style range filtering on `_ls_id`; update the fake to filter.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/verify/ -run TestVerifyDetectsDroppedRow`
Expected: FAIL — sample PKs empty (bisection not implemented).

- [ ] **Step 3: Implement bisection**

Add to `engine/internal/verify/verify.go`, and call it from `VerifyStream` when `!res.Match && in.OpenBounded != nil`:
```go
// bisect narrows a mismatching record-id range until small enough to enumerate,
// collecting offending ranges and sample PKs. Ranges are over the hex _ls_id
// space ("" = unbounded). It descends only into halves whose digest/count differ.
func bisect(ctx context.Context, in StreamInput, cols []string, bounds *syncrun.PKRange, out *events.VerifyResult) error {
	srcRows, err := collectSource(ctx, in, cols, bounds)
	if err != nil {
		return err
	}
	dstRows, err := collectDest(ctx, in, bounds)
	if err != nil {
		return err
	}
	if digestOf(srcRows, cols) == digestOf(dstRows, cols) && len(srcRows) == len(dstRows) {
		return nil // this range matches; nothing to report
	}
	if len(srcRows)+len(dstRows) <= 2*in.SampleThreshold {
		enumerate(srcRows, dstRows, cols, bounds, out)
		return nil
	}
	loMin, mid, hiMax := splitRange(srcRows, dstRows, bounds)
	if err := bisect(ctx, in, cols, &syncrun.PKRange{Min: loMin, Max: mid}, out); err != nil {
		return err
	}
	return bisect(ctx, in, cols, &syncrun.PKRange{Min: mid, Max: hiMax}, out)
}
```
Provide the helpers in the same file:
- `collectSource` reads `in.SourceRange(bounds)` chunk via `in.Source.ReadChunk`, filters rows to `bounds` by `_ls_id`, returns `map[string]sdk.Row` keyed by `_ls_id`.
- `collectDest` opens `in.OpenBounded(bounds)` and reads into a `map[string]sdk.Row`.
- `digestOf(map, cols)` folds `HashDataColumns` into an `AggregateDigest` and returns `.Hex()`.
- `enumerate(src, dst, cols, bounds, out)` records the range in `out.MismatchedRanges` and appends to `out.SamplePKs` (capped at `in.SampleCap`) every `_ls_id` that is missing on one side or whose hash differs.
- `splitRange` picks the median `_ls_id` present across both maps as `mid`, with `loMin`/`hiMax` from `bounds` (or the min/max ids observed).

Full helper code:
```go
func collectSource(ctx context.Context, in StreamInput, cols []string, bounds *syncrun.PKRange) (map[string]sdk.Row, error) {
	m := map[string]sdk.Row{}
	chunk := state.Chunk{}
	if in.SourceRange != nil {
		chunk = in.SourceRange(bounds)
	}
	err := in.Source.ReadChunk(ctx, in.Stream, chunk, func(_ context.Context, row sdk.Row) error {
		id, _ := row[model.ColRecordID].(string)
		if inBounds(id, bounds) {
			m[id] = row
		}
		return nil
	})
	return m, err
}

func collectDest(ctx context.Context, in StreamInput, bounds *syncrun.PKRange) (map[string]sdk.Row, error) {
	rd, err := in.OpenBounded(bounds)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rd.Close(ctx) }()
	m := map[string]sdk.Row{}
	err = rd.Read(ctx, bounds, func(_ context.Context, row sdk.Row) error {
		id, _ := row[model.ColRecordID].(string)
		m[id] = row
		return nil
	})
	return m, err
}

func digestOf(rows map[string]sdk.Row, cols []string) string {
	agg := &syncrun.AggregateDigest{}
	for _, r := range rows {
		h, _ := syncrun.HashDataColumns(r, cols)
		agg.Add(h)
	}
	return agg.Hex()
}

func enumerate(src, dst map[string]sdk.Row, cols []string, bounds *syncrun.PKRange, out *events.VerifyResult) {
	out.MismatchedRanges = append(out.MismatchedRanges, events.Range{Min: boundMin(bounds), Max: boundMax(bounds)})
	seen := map[string]bool{}
	add := func(id string) {
		if seen[id] || len(out.SamplePKs) >= sampleCapFromLen(out) {
			return
		}
		seen[id] = true
		out.SamplePKs = append(out.SamplePKs, id)
	}
	for id, s := range src {
		d, ok := dst[id]
		if !ok {
			add(id)
			continue
		}
		hs, _ := syncrun.HashDataColumns(s, cols)
		hd, _ := syncrun.HashDataColumns(d, cols)
		if hs != hd {
			add(id)
		}
	}
	for id := range dst {
		if _, ok := src[id]; !ok {
			add(id)
		}
	}
}

func inBounds(id string, b *syncrun.PKRange) bool {
	if b == nil {
		return true
	}
	if b.Min != "" && id < b.Min {
		return false
	}
	if b.Max != "" && id >= b.Max {
		return false
	}
	return true
}

func boundMin(b *syncrun.PKRange) string { if b == nil { return "" }; return b.Min }
func boundMax(b *syncrun.PKRange) string { if b == nil { return "" }; return b.Max }
```
For `sampleCapFromLen` and the `SampleCap` plumbing, store the cap on a package-level unexported field passed through `enumerate` — simplest: change `enumerate`'s signature to take `cap int` and drop `sampleCapFromLen`; call it with `in.SampleCap`. Update the `bisect` call chain to thread `in.SampleCap` (or read it inside `enumerate` by widening its parameters). Keep it explicit — no globals.

`splitRange`:
```go
func splitRange(src, dst map[string]sdk.Row, bounds *syncrun.PKRange) (loMin, mid, hiMax string) {
	ids := make([]string, 0, len(src)+len(dst))
	for id := range src {
		ids = append(ids, id)
	}
	for id := range dst {
		if _, ok := src[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	loMin = boundMin(bounds)
	hiMax = boundMax(bounds)
	if len(ids) == 0 {
		return loMin, loMin, hiMax
	}
	mid = ids[len(ids)/2]
	return loMin, mid, hiMax
}
```
Wire into `VerifyStream`: after computing `res`, if `!res.Match && in.OpenBounded != nil`, call `bisect(ctx, in, cols, nil, &res)`. Add `"sort"` to imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && go test -race ./internal/verify/`
Expected: PASS (both match and dropped-row cases).

- [ ] **Step 5: Commit**

```bash
git add engine/internal/verify/verify.go engine/internal/verify/verify_test.go
git commit -m "feat(engine): verify PK-range bisection + sample mismatched PKs (2.7)"
```

### Task 7: Wire the `verify` CLI verb

**Files:**
- Modify: `engine/internal/cli/cli.go` (replace `runStub(ctx, "verify", ...)` with `runVerify`)
- Modify: `engine/internal/cli/cli_test.go` (replace the "verify stub pending" expectation)

**Interfaces:**
- Consumes: `verify.VerifyStream`, `syncrun.OpenReader`, connector as `sdk.FullLoader`, catalog/destination/state loading already in `cli.go`.
- Produces: `verify` exits 0 when every stream matches, 1 otherwise; emits `engine_info{command:"verify"}` then one `verify_result` per selected stream.

- [ ] **Step 1: Write the failing test**

In `engine/internal/cli/cli_test.go`, replace the existing `verify` stub case with a real one that runs `verify` over a sqlite source synced to a temp parquet dir and asserts exit 0 with a `verify_result` line. Use the existing sqlite integration pattern in that test file (mirror the `sync` test). Example skeleton:
```go
func TestVerifyMatchesAfterSync(t *testing.T) {
	// Arrange: create a sqlite db + catalog + parquet dest; run `sync`.
	// (Reuse the helpers the sync test already uses in this package.)
	// Act: run `verify` with the same config/catalog/destination.
	// Assert: exit code 0, stdout contains `"event":"verify_result"` and `"match":true`.
}
```
If the package lacks a reusable sqlite fixture, add a minimal one: a temp `.db` with one table, two rows.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/cli/ -run TestVerifyMatchesAfterSync`
Expected: FAIL — verify returns "not implemented".

- [ ] **Step 3: Implement `runVerify`**

In `cli.go`, change the dispatch `case "verify": err = runVerify(ctx, rest, stdout)` and add:
```go
// runVerify re-checks source vs destination current state for every selected
// stream, emitting a verify_result per stream. Exit code is non-zero on any
// mismatch so make verify / CI can gate on it.
func runVerify(ctx context.Context, args []string, stdout io.Writer) error {
	cf, err := parseCommon("verify", args)
	if err != nil {
		return err
	}
	em := events.NewEmitter(stdout, events.NewSyncID(), cf.pipelineID)
	if err := em.Emit(events.KindEngineInfo, "", events.EngineInfo{Version: buildinfo.Version, Command: "verify"}); err != nil {
		return err
	}
	c, _, err := openConnector(ctx, cf)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	fl, ok := c.(sdk.FullLoader)
	if !ok {
		return fmt.Errorf("connector does not support full load; verify needs it to re-read the source")
	}
	if cf.catalog == "" || cf.destination == "" {
		return fmt.Errorf("verify requires --catalog and --destination")
	}
	var cat model.Catalog
	if err := config.LoadJSON(cf.catalog, &cat); err != nil {
		return err
	}
	destRaw, err := os.ReadFile(cf.destination)
	if err != nil {
		return fmt.Errorf("read destination config: %w", err)
	}
	destCfg, err := syncrun.LoadDestinationConfig(destRaw)
	if err != nil {
		return err
	}
	reader, err := syncrun.OpenReader(destCfg)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close(ctx) }()

	allMatch := true
	for _, sel := range cat.Selected {
		stream, ok := cat.Stream(sel.ID())
		if !ok {
			return fmt.Errorf("selected stream %s not in catalog", sel.ID())
		}
		sr, err := reader.OpenRead(ctx, stream, sel.DestinationTable)
		if err != nil {
			return err
		}
		res, err := verify.VerifyStream(ctx, verify.StreamInput{
			Stream: stream, Source: fl, DestReader: sr,
			OpenBounded: func(*syncrun.PKRange) (syncrun.StreamReader, error) {
				return reader.OpenRead(ctx, stream, sel.DestinationTable)
			},
		})
		_ = sr.Close(ctx)
		if err != nil {
			return err
		}
		if err := em.Emit(events.KindVerifyResult, stream.ID(), res); err != nil {
			return err
		}
		if !res.Match {
			allMatch = false
		}
	}
	if !allMatch {
		return fmt.Errorf("verify: one or more streams did not match")
	}
	return nil
}
```
Add the `verify` package import. Remove the now-unused `verify` case from `runStub` handling (leave `backfill` on the stub until Milestone 2.8).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && go test -race ./internal/cli/`
Expected: PASS.

- [ ] **Step 5: make check + commit + PROGRESS**

Run: `cd /home/imran/Documents/github/LakeSense && make check`  → green.
```bash
git add engine/internal/cli/cli.go engine/internal/cli/cli_test.go
git commit -m "feat(engine): wire lsengine verify verb with per-stream verify_result (2.7)"
```
Update `PROGRESS.md` locally: check `2.7 Checksum & count instrumentation + verify`.

---

## Milestone 2.8 — `lsengine backfill`

### Task 8: backfill core — bounded read, merge append, state-safe, closing verify

**Files:**
- Create: `engine/internal/backfill/backfill.go`
- Test: `engine/internal/backfill/backfill_test.go`

**Interfaces:**
- Produces: `func Run(ctx context.Context, opts Options) (events.VerifyResult, error)` where
  `type Options struct { Connector sdk.Connector; Writer syncrun.Writer; Reader syncrun.Reader; Stream model.Stream; Selection model.SelectedStream; Emitter *events.Emitter; PKMin, PKMax string; SinceField, SinceValue string; Now func() time.Time }`
- Consumes: `sdk.FullLoader` (PK-range) or `sdk.IncrementalReader` (since window), `syncrun.Writer.Open` with `truncate=false`, `verify.VerifyStream`.

**State-safety invariant:** backfill must never call any `state.Document` mutator. It receives no `*state.Document`. This is enforced structurally — `Options` has no state field.

- [ ] **Step 1: Write the failing test**

Create `engine/internal/backfill/backfill_test.go`:
```go
package backfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
	"github.com/stretchr/testify/require"
)

type fakeFL struct{ rows []sdk.Row }

func (f *fakeFL) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) { return []state.Chunk{{}}, nil }
func (f *fakeFL) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	for _, r := range f.rows {
		if err := emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// minimal connector wrapping fakeFL
type fakeConn struct{ fakeFL }
func (c *fakeConn) Spec() sdk.Spec { return sdk.Spec{Type: "fake"} }
func (c *fakeConn) Setup(context.Context, json.RawMessage) error { return nil }
func (c *fakeConn) Check(context.Context) error { return nil }
func (c *fakeConn) Discover(context.Context) ([]model.Stream, error) { return nil, nil }
func (c *fakeConn) Close(context.Context) error { return nil }

func TestBackfillAppendsAndVerifies(t *testing.T) {
	dir := t.TempDir()
	stream := model.Stream{Namespace: "public", Name: "t", Schema: model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true}, {Name: "v", Type: model.TypeString},
	}}}
	// Source now holds the corrected value for id=2.
	src := []sdk.Row{{"id": int64(2), "v": "corrected"}}
	w, _ := syncrun.OpenWriter(syncrun.DestinationConfig{Type: "parquet", Path: dir})
	r, _ := syncrun.OpenReader(syncrun.DestinationConfig{Type: "parquet", Path: dir})
	em := events.NewEmitter(&bytes.Buffer{}, "sync1", "")

	res, err := Run(context.Background(), Options{
		Connector: &fakeConn{fakeFL{rows: src}}, Writer: w, Reader: r,
		Stream: stream, Selection: model.SelectedStream{Namespace: "public", Name: "t", Mode: model.ModeFullLoad},
		Emitter: em, PKMin: "", PKMax: "",
	})
	require.NoError(t, err)
	require.True(t, res.Match)
}
```
(Add `encoding/json` import for the fake connector.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/backfill/ -run TestBackfillAppendsAndVerifies`
Expected: FAIL — `undefined: Run`.

- [ ] **Step 3: Implement `backfill.go`**
```go
// Package backfill re-syncs a bounded slice of one stream and merges it into
// the destination without a full reload, never touching CDC/cursor/chunk state.
// It appends corrected rows (merge-on-read via _ls_id + _ls_ingested_at) and
// closes by verifying the affected range.
package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
	"github.com/lakesense/lakesense/engine/internal/verify"
)

type Options struct {
	Connector          sdk.Connector
	Writer             syncrun.Writer
	Reader             syncrun.Reader
	Stream             model.Stream
	Selection          model.SelectedStream
	Emitter            *events.Emitter
	PKMin, PKMax       string
	SinceField, SinceValue string
	Now                func() time.Time
}

// Run performs the backfill and returns the closing verify result for the slice.
func Run(ctx context.Context, opts Options) (events.VerifyResult, error) {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if err := opts.Emitter.Emit(events.KindStreamStarted, opts.Stream.ID(), events.StreamStarted{Mode: "backfill"}); err != nil {
		return events.VerifyResult{}, err
	}

	sw, err := opts.Writer.Open(ctx, opts.Stream, opts.Selection.DestinationTable, false) // append, never truncate
	if err != nil {
		return events.VerifyResult{}, err
	}
	cols := syncrun.DataColumns(opts.Stream)
	src := &syncrun.AggregateDigest{}
	pk := opts.Stream.Schema.PrimaryKey()

	writeRow := func(_ context.Context, row sdk.Row) error {
		h, err := syncrun.HashDataColumns(row, cols)
		if err != nil {
			return err
		}
		src.Add(h)
		syncrun.InjectMetadata(row, pk, "u", opts.Now().UTC()) // exported helper (see note)
		return sw.WriteRow(ctx, row)
	}

	switch {
	case opts.SinceField != "":
		ir, ok := opts.Connector.(sdk.IncrementalReader)
		if !ok {
			return events.VerifyResult{}, fmt.Errorf("connector does not support incremental read for a --since backfill")
		}
		if _, err := ir.ReadIncrement(ctx, opts.Stream, opts.SinceField, opts.SinceValue, writeRow); err != nil {
			return events.VerifyResult{}, fmt.Errorf("backfill since %s=%s: %w", opts.SinceField, opts.SinceValue, err)
		}
	default:
		fl, ok := opts.Connector.(sdk.FullLoader)
		if !ok {
			return events.VerifyResult{}, fmt.Errorf("connector does not support full load for a PK-range backfill")
		}
		chunk := state.Chunk{Min: opts.PKMin, Max: opts.PKMax}
		if err := fl.ReadChunk(ctx, opts.Stream, chunk, writeRow); err != nil {
			return events.VerifyResult{}, fmt.Errorf("backfill range [%s,%s): %w", opts.PKMin, opts.PKMax, err)
		}
	}

	if err := sw.Flush(ctx); err != nil {
		return events.VerifyResult{}, err
	}
	res, err := sw.Close(ctx)
	if err != nil {
		return events.VerifyResult{}, err
	}
	_ = opts.Emitter.Emit(events.KindStateAdvanced, opts.Stream.ID(), events.StateAdvanced{Scope: "backfill", Detail: fmt.Sprintf("%s wrote %d rows", opts.Stream.ID(), res.Rows)})
	_ = opts.Emitter.Emit(events.KindChecksumComputed, opts.Stream.ID(), events.Checksum{Side: "source", Rows: src.Rows(), Checksum: src.Hex(), Columns: cols})
	_ = opts.Emitter.Emit(events.KindChecksumComputed, opts.Stream.ID(), events.Checksum{Side: "destination", Rows: res.Rows, Checksum: res.Checksum, Columns: cols})

	// Closing verify over the whole stream (the merged slice must reconcile).
	sr, err := opts.Reader.OpenRead(ctx, opts.Stream, opts.Selection.DestinationTable)
	if err != nil {
		return events.VerifyResult{}, err
	}
	defer func() { _ = sr.Close(ctx) }()
	vr, err := verify.VerifyStream(ctx, verify.StreamInput{
		Stream: opts.Stream, Source: opts.Connector.(sdk.FullLoader), DestReader: sr,
	})
	if err != nil {
		return events.VerifyResult{}, err
	}
	_ = opts.Emitter.Emit(events.KindVerifyResult, opts.Stream.ID(), vr)
	return vr, nil
}
```
**Note — exported `InjectMetadata`:** the runner's `injectMetadata` is unexported. Add an exported equivalent in `engine/internal/syncrun/writer.go` (or a small `metadata.go`) that backfill and the runner both use:
```go
// InjectMetadata stamps engine-owned _ls_ columns on a row: a stable record id
// from the primary key, the ingestion timestamp, and the op type.
func InjectMetadata(row sdk.Row, pk []string, op string, now time.Time) {
	row[model.ColRecordID] = recordID(row, pk)
	row[model.ColIngestedAt] = now.UTC().Format(time.RFC3339Nano)
	row[model.ColOpType] = op
}
```
Then refactor the runner's `injectMetadata` to call `InjectMetadata` (keeping the CDC timestamp branch) so there is one implementation (DRY).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && go test -race ./internal/backfill/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/internal/backfill/backfill.go engine/internal/backfill/backfill_test.go engine/internal/syncrun/writer.go engine/internal/syncrun/runner.go
git commit -m "feat(engine): backfill core — merge-on-read append + closing verify (2.8)"
```

### Task 9: Wire the `backfill` CLI verb + end-to-end restore test

**Files:**
- Modify: `engine/internal/cli/cli.go` (replace `runStub(ctx, "backfill", ...)` with `runBackfill`)
- Modify: `engine/internal/cli/cli_test.go` (replace the "backfill stub pending" case with an end-to-end restore test)

**Interfaces:**
- Consumes: `backfill.Run`, `syncrun.OpenWriter`/`OpenReader`, connector setup helpers.
- Produces: `backfill --config --destination --catalog --stream ns.name (--pk-min X --pk-max Y | --since field=value)`.

- [ ] **Step 1: Write the failing test (end-to-end, real sqlite)**

In `cli_test.go`, add a test that: (1) syncs a sqlite table to a temp parquet dir; (2) deletes/corrupts one destination part-file row's stream by removing a part-file (simulating loss); (3) runs `verify` and asserts a mismatch (exit 1); (4) runs `backfill --stream ... --pk-min ... --pk-max ...` over the affected range; (5) runs `verify` again and asserts exit 0. Assert also that a state file passed to `backfill` (if any) is byte-identical before/after (state-safety). Mirror the sqlite fixture helper used by the sync/verify tests.

```go
func TestBackfillRestoresCorruptedWindow(t *testing.T) {
	// See Task 7 fixture. Sync -> corrupt -> verify(fail) -> backfill -> verify(pass).
	// require exit codes: verify #1 == 1, backfill == 0, verify #2 == 0.
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && go test ./internal/cli/ -run TestBackfillRestoresCorruptedWindow`
Expected: FAIL — backfill returns "not implemented".

- [ ] **Step 3: Implement `runBackfill`**

In `cli.go`, register `case "backfill": err = runBackfill(ctx, rest, stdout)` and add a handler that parses the extra flags (`--stream`, `--pk-min`, `--pk-max`, `--since`), loads catalog/destination/connector like `runVerify`, resolves the one target stream + its selection, constructs `backfill.Options`, calls `backfill.Run`, and returns a non-nil error when the closing verify does not match (so exit code reflects success). Parse `--since` as `field=value`. Emit `engine_info{command:"backfill"}` first. Do **not** pass a `*state.Document` — backfill is state-free by construction.

```go
func runBackfill(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	var cf commonFlags
	cf.register(fs)
	streamID := fs.String("stream", "", "target stream ns.name (required)")
	pkMin := fs.String("pk-min", "", "inclusive PK-range lower bound")
	pkMax := fs.String("pk-max", "", "exclusive PK-range upper bound")
	since := fs.String("since", "", "changed-since window as field=value")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if cf.config == "" || cf.catalog == "" || cf.destination == "" || *streamID == "" {
		return fmt.Errorf("backfill requires --config, --catalog, --destination, and --stream")
	}
	// ... load connector (openConnector needs cf.config populated; reuse it),
	// catalog, destination writer + reader; find stream + selection by *streamID;
	// split *since into field/value; call backfill.Run; return error if !Match.
	return nil // replace with the assembled implementation
}
```
Fill in the body following `runVerify`'s loading pattern exactly (the plan's Task 7 handler is the template; here you select a single stream and build `backfill.Options`). Emit nothing beyond what `backfill.Run` emits plus the initial `engine_info`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && go test -race ./internal/cli/`
Expected: PASS.

- [ ] **Step 5: make check + commit + PROGRESS**

Run: `cd /home/imran/Documents/github/LakeSense && make check` → green.
```bash
git add engine/internal/cli/cli.go engine/internal/cli/cli_test.go
git commit -m "feat(engine): wire lsengine backfill verb + e2e restore test (2.8)"
```
Update `PROGRESS.md` locally: check `2.8 Backfill/point-in-time resync`; add a Decisions Log entry for the merge-on-read (Option B) choice with scores.

### Task 10: Parquet + verify assertion in the verification script; usage/docs

**Files:**
- Modify: `scripts/verify-migration.sh` (add a parquet + verify leg)
- Modify: `engine/internal/cli/cli.go` usage text (document backfill/verify flags)
- Modify: `README.md` (destination note: "NDJSON and Parquet today; Iceberg v0.2")

- [ ] **Step 1: Add a parquet round-trip to the migration script**

In `scripts/verify-migration.sh`, after the existing ndjson sqlite leg, add a leg that syncs the same known dataset to a `parquet` destination, runs `lsengine verify`, and asserts exit 0 and a `"match":true` line. Reuse the script's PASS/FAIL table (`scripts/lib.sh`).

- [ ] **Step 2: Update usage + README**

In `cli.go` `usage`, expand the `backfill`/`verify` lines with their required flags. In `README.md`, change the destination sentence to: "NDJSON and Parquet ship today; append-mode Iceberg lands in v0.2." Update the sources/quickstart snippet that says `{ "type": "ndjson" }` to mention `"parquet"` as an option.

- [ ] **Step 3: Run the script + make check**

Run:
```bash
cd /home/imran/Documents/github/LakeSense && bash scripts/verify-migration.sh sqlite && make check
```
Expected: script PASS table all green; `make check` green.

- [ ] **Step 4: Commit**

```bash
git add scripts/verify-migration.sh engine/internal/cli/cli.go README.md
git commit -m "test(engine): parquet+verify leg in verify-migration; document verify/backfill (2.7/2.8)"
```

---

## Self-Review

**1. Spec coverage.**
- 2.5 Parquet writer → Tasks 1–4 (schema, writer, reader, resume). ✓ Iceberg explicitly deferred (design non-goal).
- 2.7 verify + PK-range bisection + sample PKs → Tasks 5–7. ✓ `verify_result` event already in schema; exit-code gating ✓.
- 2.8 backfill (PK range + since window) + merge strategy (B) + state-safety → Tasks 8–9. ✓ State-safety enforced structurally (no state field). ✓ Closing verify ✓.
- Reader abstraction (design's unifying piece) → Task 3. ✓
- Deepest-testing (Rule 6): round-trip, crash-resume, type coverage (fold into Task 1/2 test data), verify-catches-corruption, backfill-restores, merge resolution → Tasks 2,3,4,6,8,9. ✓
- Script/docs → Task 10. ✓

**Gap found & fixed:** design lists a "Parquet type coverage" test; it is covered by `testStream()` exercising int64/decimal/string/bool/timestamp across Tasks 1–2. If a dedicated all-types case is wanted, add it to `parquet_test.go` in Task 2 — noted here so the executor includes it.

**2. Placeholder scan.** Two handlers (Task 7 `verify` CLI test, Task 9 `runBackfill` body + tests) are described as "follow the template" rather than fully transcribed, because they are near-duplicates of fully-shown code in the same plan (Task 7's loader pattern; the sqlite fixture already in `cli_test.go`). The executor copies the shown template and changes the named specifics. This is a deliberate DRY reference to code shown in-plan, not an undefined reference.

**3. Type consistency.** `hashRow`/`HashDataColumns`, `digest`/`AggregateDigest`, `PKRange`, `StreamReader.Read(ctx, *PKRange, sdk.RowFunc)`, `events.VerifyResult{Match,SourceRows,DestinationRows,MismatchedRanges,SamplePKs}`, `model.ColRecordID="_ls_id"`, `InjectMetadata(row, pk, op, now)` are used consistently across tasks. `syncrun.InjectMetadata` is introduced in Task 8 and the runner refactored to share it (DRY).
