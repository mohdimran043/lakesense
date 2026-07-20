# Track A — Engine Correctness Loop (Design)

**Date:** 2026-07-21
**Spec refs:** `lakesense-final-prompt.md` §2.5 (writers), §2.7 (verify), §2.8 (backfill)
**Status:** approved design, ready for implementation plan

## Goal

Complete the engine's correctness loop so LakeSense's headline claim — "open
lakehouse formats, proven correct" — becomes literally true. Today the engine
writes NDJSON only, and `verify`/`backfill` are `not implemented` stubs. This
track delivers, in order:

1. **2.5 — a Parquet writer** (real open-lakehouse output).
2. **2.7 — `lsengine verify`** with PK-range bisection drill-down.
3. **2.8 — `lsengine backfill`** (idempotent, state-safe window resync).

All three are fully testable in this environment with the existing
fake-connector and real-SQLite suites — no external databases required.

## Non-goals (explicitly deferred)

- Iceberg REST-catalog commits (Phase 1 decision Option C, v0.2). Parquet part-files
  are the on-disk substrate Iceberg will later reference; this track stops at Parquet.
- Partition schemes / Z-ordering. Part-files land flat per stream in v0.1.
- Parallel readers (roadmap; orthogonal to correctness).
- Any control-plane / UI wiring. That is Track B/C. This track ends at the engine
  CLI + JSONL events; the diff-UI drill-down consumes `verify_result` later.

## The unifying abstraction: a destination Reader

Both `verify` and `backfill` need to read back what was written. We add a
consumer-side reader mirroring the existing `syncrun.Writer`, defined in
`engine/internal/syncrun`:

```go
// Reader is the read-back side of a destination, used by verify (to hash the
// current destination state) and by backfill's merge verification.
type Reader interface {
    // OpenRead returns a StreamReader over one stream's destination data.
    OpenRead(ctx context.Context, stream model.Stream, destTable string) (StreamReader, error)
    Close(ctx context.Context) error
}

// StreamReader yields the destination's *current logical state* for one stream:
// the latest row per _ls_record_id (max _ls_ingested_at), with rows whose latest
// op is delete ("d") omitted. bounds, when set, restrict to a PK range.
type StreamReader interface {
    // Read streams current-state rows; bounds nil means the whole stream.
    Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error
    Close(ctx context.Context) error
}

type PKRange struct { Min, Max string } // inclusive-min, exclusive-max; empty = unbounded
```

**Why latest-per-record_id matters.** The engine already injects `_ls_record_id`
(stable PK hash) and `_ls_ingested_at` on every row. Resolving to the latest row
per record id — and dropping deletes — turns an append-only file set into a
logical current-state view. This is what lets `backfill` append corrections
(rather than rewrite files) and still have `verify` pass. It is exactly the
merge-on-read model Iceberg/Hudi use.

This distinguishes two correctness claims the product makes, cleanly:

| Claim | What it compares | When | Status |
|---|---|---|---|
| **sync diff badge** | rows *read this sync* vs rows *written this sync* | every sync | built |
| **`verify`** | source *current state* vs destination *current state* | on demand | this track |

## Milestone 2.5 — Parquet writer

**Library:** `github.com/parquet-go/parquet-go` — pure Go, no CGo (preserves the
static distroless `lsengine` binary), actively maintained.
*Rejected:* Apache Arrow Go (CGo, heavier).

**On-disk layout:** a directory of immutable **part-files per stream**:
`<dir>/<namespace.name>/<sync_id>-<seq>.parquet`. One part-file is finalized
(footer written + `fsync`) per `Flush` boundary. A finalized part-file is the
durable unit — so the orchestrator's existing write-ahead-flush discipline
(flush before every state commit) maps a completed chunk to a durable, readable
part-file with zero changes to the runner.
*Rejected:* single appended Parquet file — a Parquet file without a footer is
unreadable, so a crash mid-file would violate the ack-before-state contract.

- `truncate=true` (fresh full-refresh) removes any existing part-files under the
  stream directory before writing; `truncate=false` (resume/append) leaves them.
- Empty `Flush` (no rows pending) is a no-op — no empty part-files.
- Checksum accounting is the **same `hashRow` digest** used by NDJSON, so
  `WriteResult.Checksum` is byte-identical across destinations. Bytes = sum of
  finalized part-file sizes.

**Schema derivation:** build a `parquet.Schema` dynamically from
`model.Stream.Schema.Columns` plus the four `_ls_` metadata columns, via a
lake-type → Parquet-type map. Rows (`sdk.Row`, i.e. `map[string]any`) are
converted to `parquet.Row` in fixed column order. All columns optional
(nullable) for v0.1 robustness.

**Type mapping (v0.1):**

| Lake type | Parquet |
|---|---|
| string, text | BYTE_ARRAY (UTF8) |
| int32 | INT32 |
| int64, int | INT64 |
| float32 | FLOAT |
| float64, double | DOUBLE |
| bool | BOOLEAN |
| decimal | BYTE_ARRAY (UTF8) — exact string, documented v0.1 choice |
| timestamp, date, time | BYTE_ARRAY (UTF8, RFC3339) |
| bytes/blob | BYTE_ARRAY |
| json/array/object/unknown | BYTE_ARRAY (UTF8, JSON-encoded) |

`_ls_record_id`/`_ls_op` → UTF8, `_ls_ingested_at`/`_ls_cdc_ts` → UTF8 (RFC3339).

Wire into `OpenWriter` (`case "parquet"`), update `DestinationConfig` doc
comments, and remove the "unknown type" hint that says parquet is unimplemented.

## Milestone 2.7 — `lsengine verify`

Re-checks counts + order-independent checksums, source vs destination, per
stream, and drills down to offending PK ranges + sample mismatched PKs on
mismatch. Emits the already-defined `events.VerifyResult` / `events.Range`.

**Flow (per selected stream):**
1. **Whole-stream digest, both sides (streaming, cheap).** Source: re-read via
   `FullLoader.ReadChunk` over all chunks, folding `hashRow` into a `digest`.
   Destination: the new Reader (current-state), same `hashRow`.
2. **Match?** aggregate + row count equal → emit `verify_result{match:true}`, done.
3. **Mismatch → PK-range bisection.** Starting from the stream's full PK range,
   recursively split in two; compute streaming digests per sub-range on both
   sides (source via a synthesized `state.Chunk` with the range bounds;
   destination via the Reader with `PKRange`); descend only into sub-ranges whose
   digest or count differs. Stop when a range holds ≤ `verifySampleThreshold`
   (default 100) rows on either side, then enumerate both sides of that range and
   diff by record id to collect: missing-on-dest, extra-on-dest, and hash-differs
   PKs. Emit `verify_result{match:false, mismatched_ranges, sample_pks}` (sample
   capped, default 20).

Memory-bounded (only one small range materialized at a time). Requires a stream
to have a primary key for bisection; without a PK, fall back to a whole-stream
row-multiset diff and a `warning{code:"verify_no_pk"}`.

**CLI:** `verify --config … --destination … --catalog … [--state …]`. Reuses the
common data-path flags. Emits `engine_info{command:"verify"}` then one
`verify_result` per stream; process exit 0 when all match, 1 on any mismatch (so
CI/`make verify` can gate on it).

## Milestone 2.8 — `lsengine backfill`

Re-syncs a bounded slice of one stream without a full reload, merging
idempotently into the destination, never corrupting ongoing CDC/cursor state.

**Merge strategy (brainstorm — scored speed / correctness / reliability / fit):**
- (A) overwrite-partition — 6: needs a partition scheme we don't have.
- **(B) merge-on-PK via append + resolve-latest-on-read — 9 (chosen):** append
  corrected rows carrying a newer `_ls_ingested_at`; the Reader already resolves
  latest-per-record_id, so no file rewrite, reuses injected metadata, matches
  Iceberg/Hudi merge-on-read.
- (C) delete+insert window (tombstones) — 7: more machinery than B needs here.

**Selection:** one stream, bounded by exactly one of:
`--pk-min/--pk-max` (PK range) or `--since <cursor-field>=<value>` (timestamp/
changed-since window). PK range reads via a synthesized `state.Chunk` through
`FullLoader.ReadChunk`; the `--since` window reads via
`IncrementalReader.ReadIncrement`.

**State safety:** backfill runs on its own code path. It **never** calls
`State.SetGlobalPosition`, `CompleteChunk`, or `SetCursor` — it only reads schema
/ chunk plans. So a backfill can run alongside CDC progress without moving any
watermark. It opens the destination writer with `truncate=false` (append), writes
the slice via the normal `handleRow` path (record ids + fresh `_ls_ingested_at`),
flushes, emits `chunk_completed` + `state_advanced{scope:"backfill"}` + both-side
`checksum_computed`, and finishes by running the 2.7 verify over the affected
range so the diff badge goes green again.

**CLI:** `backfill --config … --destination … --catalog … --stream ns.name
(--pk-min X --pk-max Y | --since field=value)`. Emits
`engine_info{command:"backfill"}`, the progress/checksum stream, and a closing
`verify_result` for the range.

## Testing (Rule 6 — correctness gets the deepest tests)

Table-driven, `-race`, via fake connectors + real SQLite. Every item is a gate.

1. **Parquet round-trip:** full-load a fake stream to Parquet; read back via the
   Reader; source and destination checksums match; part-files are valid Parquet.
2. **Parquet crash-resume:** kill after N chunks (part-files present), resume with
   the same state → no dup/loss, final checksums match (mirrors the NDJSON
   crash-resume test).
3. **Parquet type coverage:** a stream exercising every lake type in the mapping
   round-trips with a matching checksum.
4. **verify happy path:** a clean sync → `verify` reports `match:true`, exit 0.
5. **verify catches corruption:** drop one destination row / mutate one → `verify`
   reports `match:false`, bisects to the correct PK range, lists the offending PK.
   The bisection descends only into mismatching halves (assert range count is
   logarithmic, not full-scan).
6. **verify no-PK fallback:** stream without a PK → multiset diff + warning.
7. **backfill restores correctness:** corrupt a destination window → `verify`
   fails → `backfill` the window → `verify` passes; CDC global position in state
   is byte-identical before and after (state-safety proof).
8. **merge-on-read resolution:** append an updated row (newer `_ls_ingested_at`)
   and a delete for another PK → the Reader yields the updated value and omits
   the deleted PK; `verify` matches a source reflecting those changes.

`make check` (lint 0, vet, `-race`) green after each milestone; a new
`scripts/verify-migration.sh` assertion exercises the Parquet path end-to-end.

## Rollout / commit plan

One commit per milestone (plus this design doc), updating `PROGRESS.md` (2.5 →
2.7 → 2.8 boxes, Decisions Log entries for the Parquet layout and the merge
brainstorm) each time — the project's resumability protocol. Work lands on branch
`track-a-engine-correctness`; opened as a PR to `main` when the three milestones
and `make check`/`make verify` are green.
