# Reference Analysis — Engine Protocol & Connector Lifecycle

> Clean-room bridge document. Concepts and flows in our own words; no code from the
> reference may be copied. LakeSense implements FROM THIS DOC, never from reference source.

## 1. CLI surface & file-based I/O contract

The reference engine is a single binary per driver built around a `spec | check | discover | sync | clear`
command set. Everything flows through **files passed as flags**, not stdin:

- `--config` — source connection config (JSON)
- `--destination` — destination/writer config (JSON)
- `--streams` (aka `--catalog`) — the stream catalog with user selections (JSON)
- `--state` — sync state (JSON); rewritten in place as sync progresses

Derived defaults: when paths are omitted, state/streams files default to sibling paths in the config
folder. A `--no-save` flag suppresses artifact files. Other notable flags: destination buffer/batch
size, max discovery threads, timeout override, encryption key for at-rest config secrets, and a
"diff two catalogs" mode that emits a difference file (used by their UI to detect schema drift
between discover runs).

**Command semantics:**
- `spec` — prints the JSON-schema of the source (and destination) config so a UI can render forms.
- `check` — validates connectivity/config, prints a status message.
- `discover` — connects, lists streams, then concurrently produces a schema per stream
  (bounded worker pool, per-stream retry), and emits the full catalog.
- `sync` — the main run; details below.
- `clear` — clears state and destination artifacts for chosen streams (used before re-sync).

**LakeSense decision:** keep the same five-verb pattern (industry standard, not copyrightable) but
add `backfill` and `verify` verbs, and emit a **structured JSONL event stream on stdout** as the
primary machine interface (the reference primarily logs human text + writes files; its UI must
scrape/poll). Our control plane consumes events natively — this is a deliberate improvement.

## 2. Catalog & stream-selection model

The catalog has two layers:

1. **All discovered streams** — each with: namespace + name (ID = `namespace.name`), a schema
   (column → type set, nullable), supported sync modes, source-defined primary key, and a chosen
   `sync_mode`.
2. **Selected streams** — a per-namespace map of user selections carrying per-stream metadata:
   partition regex (destination partitioning), append-mode toggle, normalization toggle,
   **column selection** (list + a `sync_new_columns` flag deciding whether newly appearing columns
   flow through or are dropped), and an optional **row filter** (up to two conditions joined by a
   logical operator; each condition's column/value is validated against the schema type before the
   stream is accepted).

At sync start the engine re-runs a light discovery, then classifies every selected stream into one
of three read paths by its sync mode: **CDC** (or "strict CDC" = CDC without initial backfill),
**incremental**, or **full-refresh**. Full-refresh streams get their state cleared and destination
tables dropped before reading. Invalid streams (missing from source, bad filter, unknown cursor)
are skipped with warnings, not fatal errors.

Discovery injects **engine-owned metadata columns** into every stream schema: a synthetic record ID
(hash of primary-key values — the idempotency key for merges), an ingestion timestamp, an operation
type (`r`ead/`c`reate/`i`nsert-upsert/`u`pdate/`d`elete), and a CDC event timestamp where relevant.
Default sync-mode preference on discovery: CDC if supported, else incremental, else full refresh.

**LakeSense decision:** same two-layer catalog concept (it cleanly separates "what exists" from
"what the user chose"), same metadata-column idea with `_ls_` prefixed names, plus a per-connector
**capability declaration** (`supports: cdc|incremental|full_load`) surfaced in discover output —
that declaration drives UI badges and docs matrices automatically.

## 3. Driver abstraction — the shared sync skeleton

The key architectural insight: an **abstract orchestration layer** owns all cross-cutting sync
logic, and each source driver implements only a narrow interface. The abstract layer provides:

- **Chunked backfill**: asks the driver to split a stream into `{min,max}` chunks ONCE, persists
  the chunk set in state, runs chunks through a bounded concurrent worker pool with retries, and
  removes each chunk from state as it completes. Resume after crash = re-run only remaining chunks.
- **Incremental**: reads the previous cursor from state; captures the source's current **max cursor
  value first**, runs the chunked backfill bounded by it, then streams rows beyond the stored
  cursor. Supports a primary + optional secondary cursor. Cursor values are normalized to strings
  in state and re-typecast against the schema on load.
- **CDC**: a three-phase flow — driver-specific "pre" step (create/validate slot, capture start
  position), per-stream backfill (skipped for streams already backfilled or in strict-CDC mode),
  then change streaming. Drivers declare one of three execution shapes: *sequential* (one global
  log, e.g. Postgres WAL), *parallel* (N independent readers, e.g. Kafka partitions), or
  *concurrent* (each stream's CDC starts the moment its backfill finishes, e.g. MongoDB change
  streams). A driver-specific "post" step persists final positions.
- **Backfill/CDC overlap correctness**: during the window where backfill and CDC overlap, CDC
  inserts are emitted as *upserts* (dedup against backfilled rows via the PK-hash ID); once in
  steady state they downgrade to plain appends. Op-type letters distinguish the cases.
- Concurrency is managed with two bounded context groups (a connection-limited one and a global
  one), retry-with-backoff wrappers, panic recovery in workers, and SIGINT/SIGTERM wired into the
  root context so cancellation reaches every reader/writer.

The driver interface, conceptually: config ref + validation, spec, type/name, setup (connect),
state injection, connection/retry limits, stream-name listing, per-stream schema production,
chunk-splitting + per-chunk iteration, max-cursor fetch + incremental streaming, CDC support flag +
execution shape + pre/stream/post hooks.

**LakeSense decision:** adopt the same layering (SDK interface ≈ thin per-source methods; engine
core owns chunk orchestration, cursor logic, CDC phasing, retries, concurrency). Our interface adds
`Capabilities()` and per-stream count/checksum accumulation hooks (feeds data-diff, which the
reference lacks entirely).

## 4. Config / state / event shapes (conceptual)

- **Source config**: flat JSON per driver (host/port/credentials/database + driver-specific tuning
  like max threads, retry count, SSH/SSL blocks).
- **Writer config**: `{type, writer: {...}}` — e.g. Parquet with local path or S3 bucket/region,
  Iceberg with catalog config.
- **State**: see `state-and-recovery.md`.
- **Events**: the reference has no machine-readable event protocol on stdout — logs are for humans;
  its stats logger periodically prints running counts (threads, records read, target estimate).
  Telemetry (sync started/completed with stream counts) goes to an analytics backend, not to the
  operator.

**LakeSense decision (event schema is OURS, designed in Phase 2.1):** JSONL on stdout, one object
per line: `{ts, event, sync_id, stream?, payload}` with event kinds for job lifecycle
(started/finished/failed), per-stream progress (rows, bytes, rate), chunk completion, cursor/state
advancement, schema changes (added/dropped/retyped columns), per-column source→destination mapping,
checksum/count results, and errors with structured cause. The control plane ingests this verbatim.

## 5. Notable gaps in the reference (LakeSense opportunities)

- No first-class machine event stream (UI polls logs/files) → our JSONL protocol.
- No verification/diff of source vs destination → our checksum instrumentation + `verify`.
- No bounded backfill verb (full re-sync or nothing at the CLI level) → our `backfill` slice resync.
- Sync-mode capability is implicit in code, not a declared artifact → our capability declarations.
