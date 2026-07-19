# Reference Analysis — Other Sources (MongoDB, MSSQL, Oracle, DB2, Kafka, S3)

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.
> All sources share the abstract chunk/cursor/CDC skeleton described in `engine-protocol.md` and
> `state-and-recovery.md`; chunks target ~2 GB of source data (≈ one 256 MiB Parquet file at ~8×).

## MongoDB (LakeSense Tier B)

- **Full load — `_id`-range chunking, three strategies:** (1) default for ObjectID `_id`s: the
  server's split-vector admin command yields boundaries, coalesced into larger chunks; (2) on
  auth failure or non-ObjectID ids: bucket-auto aggregation with bucket count derived from
  collection storage size; (3) timestamp strategy: exploit ObjectID's embedded timestamp — slice
  the time span between extreme ids and synthesize boundary ObjectIDs (timestamp bytes set, rest
  zeroed). Chunk reads = range-match + sort on `_id` aggregations with disk use allowed.
- **Incremental:** cursor-greater-than find; secondary cursor as null-fallback OR.
- **CDC:** per-collection change streams (insert/update/replace→update/delete), full-document
  lookup on update, before-image when available (deletes fall back to the document key). The
  resume token is the state cursor AND rides on each record as a metadata column. Bounded run:
  decode the resume token's optime and stop once caught up to the cluster's operation time;
  post-batch tokens checkpoint progress even in event-free periods.
- **Typing gotchas:** ObjectID→hex, Decimal128→string, Binary→hex, dates clamped to valid UTC,
  NaN/±Inf doubles nulled. **Mixed-type `_id` collections: only ObjectID docs sync** (documented
  loss — LakeSense must at minimum count + report skipped docs; candidate fix: per-BSON-type
  sub-chunks). Discovery samples documents from both ends of the collection.
- **[BRAINSTORM] BSON→lake mapping deferred to Phase 2.4c** (options: flatten / JSON-column /
  hybrid) — decide when building, with these facts in hand.

## SQL Server (Tier B)

- **Full load ladder:** (1) PK present → sample-based boundaries (table-sample rows, evenly
  spaced picks); (2) fallback → iterative "next boundary after N rows" PK seeks (composite PKs as
  concatenated strings); (3) no key → physical allocation-page walk: read allocated page IDs from
  the DMV (version/permission-gated), encode file/page/slot as sortable physical-location
  boundaries; (4) final fallback → iterative physical-location walking. Chunk size from stats;
  unpopulated statistics is a hard error. Reads at READ COMMITTED.
- **CDC = native change-table polling** between the stored LSN and the current max LSN,
  incrementing the lower bound each pass; op codes map to insert/update/delete (before-update
  images skipped); LSN + sequence values emitted hex as metadata columns. Two subtle guards:
  (a) don't trust max-LSN until the async capture agent shows a non-throttled scan session
  (replica-safe timeout); (b) optional capture-instance management for schema drift — pick the
  newest instance valid at the cursor, clamp the target LSN at a newer instance's start, rotate
  instances on DDL (2-instance server limit).
- **Types:** datetime2/datetimeoffset→timestamp-micro; uniqueidentifier's mixed-endian bytes
  reformatted to canonical UUID; time/rowversion/geo/hierarchyid→string; decimal→float64 (same
  lossiness caveat; LakeSense prefers decimal128). Boundary values normalized to SQL-literal
  strings for JSON round-tripping.

## Oracle (Tier C)

- **Full load:** ROWID-range chunks. Preferred: the server-side parallel-execute package creates
  block-based chunks; fallbacks: block-sampling of ROWIDs with evenly spaced boundaries, then an
  iterative min/max ROWID walk. Scans are ROWID range predicates. Missing stats → assumed row size.
- **Incremental:** shared watermark model + careful timezone normalization (TZ-aware types → UTC
  instant; TZ-naive → wall clock preserved).
- **CDC: not implemented in the reference** (stubs only, no SCN/LogMiner). Matches our honest
  Tier C "incremental only, LogMiner roadmap" badge.
- **Types:** NUMBER(p,0)→int32/int64 by precision else float64; TIMESTAMP variants→timestamp-micro;
  INTERVAL/RAW/LOB/XML→string.

## DB2 (Tier C)

- **Full load:** PK-walking sized by catalog stats (RUNSTATS required — hard error otherwise);
  keyless tables use arithmetic RID ranges from page stats. Perf trick worth noting: array/block
  fetch (~200 rows) with typed column buffers in a producer/consumer pipeline instead of
  row-by-row scanning.
- **Incremental:** shared model; DB2 timestamps are TZ-naive so state stores them in a dedicated
  format WITHOUT UTC conversion (conversion would corrupt values).
- **CDC:** not implemented (stub). **Types:** DECFLOAT→string (exceeds float64), TIME→string,
  XML/GRAPHIC/BLOB→string.

## Kafka (Tier B)

- Consumption only in strict-CDC style — no backfill/incremental. Consumer-group readers
  (franz-go class library), optionally one per partition; generated group ID persisted in global
  state (state beats config). **Bounded snapshot semantics:** capture end offsets at start; a
  reader exits when all assigned partitions reach them (or poll timeout).
- **Offsets are the state**, committed back to the broker only after destination commit. Recovery
  reconciliation: destination metadata ahead of broker → replay metadata offsets to broker and
  skip; broker ahead of metadata, or metadata past end offsets (topic recreated) → hard error
  demanding destination wipe.
- Confluent wire format detected by magic byte → schema-registry Avro/JSON by schema ID, else
  plain JSON; partition/offset/key/timestamp injected as metadata columns; schema inferred by
  sampling per partition.
- LakeSense deviation: our spec (2.4e) wants *bounded-offset incremental* — the reference's
  end-offset-bounded group consumption is exactly that mechanism; we additionally persist
  per-partition offsets in OUR state (not only broker-side) for auditability.

## S3 / Object storage (Tier C, one connector + presets)

- **Discovery:** paginated full listing under a prefix, filtered by regex + format extension
  (csv/json/jsonl/parquet, gzip variants). Streams = first folder level below the prefix.
- **Full load:** bin-pack files into ~2 GB chunks (chunk min = file-key list); oversized files get
  their own chunk; files deleted between list and read are skipped, not fatal.
- **Schema inference from the first file only** (reference weakness — LakeSense samples up to N
  files): CSV type-votes over sampled rows, JSON auto-detects array/JSONL/object, Parquet reads
  footer metadata with optional range-request streaming.
- **Incremental:** last-modified watermark; an injected last-modified column doubles as the
  cursor; modified files re-read in full with upsert dedup absorbing duplicates. Per-file (not
  per-row) granularity — document honestly.

## Cross-cutting LakeSense takeaways

1. Every source fits the same skeleton: chunk-set full load + watermark incremental + bounded
   CDC session — validates our SDK shape (2.2) hard.
2. Stats-dependence (MySQL/MSSQL/DB2 erroring on stale stats) needs first-class UX: a
   `stats_missing` engine event with the exact fix command, surfaced in the UI.
3. Every "not implemented in reference" (Oracle/DB2 CDC) matches our roadmap badges — our honesty
   matrix is defensible.
4. Decimal/precision lossiness is systemic in the reference (float64 everywhere) — decimal128
   support is a real, demonstrable correctness differentiator for LakeSense's diff/verify story.
