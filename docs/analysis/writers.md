# Reference Analysis — Writers (Parquet & Iceberg)

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.

## 1. Parquet destination (reference behavior)

- **Libraries:** the reference row-writes via `parquet-go/parquet-go`, with a second legacy
  library for local file handles, and a *third* stack (arrow-go) for its Iceberg path — an
  accretion we should avoid: LakeSense picks ONE library (`parquet-go/parquet-go`) for the
  Parquet writer.
- **Batching:** buffering lives above the destination — a writer thread buffers N records
  (configurable, default 10k), then flushes the copied batch on a single async worker so the next
  batch fills while the previous flushes. Exactly one in-flight flush per thread = natural
  backpressure; flush errors surface on the next push.
- **Rotation/sizing:** no size-based rotation inside the sink; file size is governed upstream by
  chunk planning (chunk ≈ one target file). Files stay open per writer thread; new files open only
  per partition path or on schema evolution.
- **Layout:** `{base}/{database}/{table}/…` plus optional user partition pattern
  `{column, fallback, granularity}` (hour/day/week/month/year; ingestion-time option). Filename =
  UTC timestamp + ULID. Compression: Snappy, hardcoded.
- **Schema evolution mid-sync:** drift detected during batch flatten; the shared per-stream schema
  (RWMutex) widens, and every active partition path opens a *new* file with the widened schema —
  earlier files keep the old schema (readers must merge). A non-normalized mode writes one JSON
  string column + metadata columns, sidestepping evolution.
- **S3/GCS/MinIO:** always write locally first, multipart-upload on close, delete local copy;
  on error/cancel, delete without uploading. **No atomic rename step and no destination-side 2PC
  for plain Parquet** (explicitly unfinished in the reference): a crash between upload and state
  save can duplicate rows.

**LakeSense decisions:** single parquet library; size-based rotation inside the sink as well
(cap file size regardless of chunk math); zstd default with snappy option; same
partition-pattern idea; **fix the Parquet 2PC gap** — write to a temp key and finalize with a
manifest-style commit marker (per-thread JSON sidecar listing committed files + state payload),
which is what our `verify`/diff also reads. That gives Parquet the same
destination-as-source-of-truth recovery the reference only has for Iceberg.

## 2. Iceberg destination (reference behavior)

- **Java sidecar is real and load-bearing:** a shared JVM (forked Debezium-Iceberg writer) is
  spawned per sync and spoken to over localhost gRPC. Its stated reason: Go's Iceberg ecosystem
  lacks **equality-delete (upsert) writes**. The sidecar owns catalogs (Glue/JDBC/Hive/REST — REST
  covering S3 Tables SigV4, Unity, OAuth2, Nessie), schema-union evolution commits, and
  transactional snapshot commits.
- **Two write paths:** a legacy path where Java builds the Parquet, and an arrow path where **Go
  already builds the finished Parquet files** (data + equality-delete + positional-delete files,
  correct Iceberg field IDs incl. reserved positional-delete column IDs, embedded schema
  metadata) and Java is reduced to: allocate output locations, stream bytes through the table's
  FileIO, and commit.
- **Commit flow:** setup = get-or-create table (schema, identifier field = PK-hash column,
  partition spec transforms). Close = one registration call listing all files (equality-deletes →
  data → positional-deletes), wrapped by Java in ONE Iceberg transaction that also updates a
  commit-state table property ("2PC" state) — data visibility and state recording are atomic.
  Rolling: data files ~512 MB, delete files ~64 MB.
- **CDC merge semantics:** upserts = equality delete on the PK-hash + write; steady-state inserts
  skip the delete; hard deletes emit delete-only. Within a thread, repeat sightings of the same
  key become positional deletes against the earlier row (keep-latest). The backfill/CDC overlap
  window is opened/closed by a flag stored in table state.
- **Schema evolution:** Go-side promotion lattice (int→long→…→string); genuinely-new drift
  triggers a union-by-name evolution commit, otherwise just a schema refresh.

## 3. Pure-Go feasibility assessment

What Java actually provides that pure Go (apache/iceberg-go, mid-2026) does not yet:
RowDelta commits (equality-delete registration), delete-file write support in the table API,
multi-operation transactional commits, Hive metastore, full Glue/JDBC parity. What pure Go CAN
do today: REST/Glue/SQL catalog clients, table creation, append-only commits, plus everything on
the file side (the reference itself proves Parquet-with-Iceberg-field-IDs is fully doable in Go).

## 4. [BRAINSTORM] Iceberg write strategy for LakeSense v0.1

| Option | Speed | Wow | Reliability | Skill showcase | Total |
|---|---|---|---|---|---|
| A. `iceberg-go` full integration (append + attempt upserts) | 4 — fighting missing RowDelta upstream | 7 | 4 — the exact gap the reference built a JVM to avoid | 6 | 21 |
| B. Parquet-only v0.1, Iceberg v0.2 on roadmap | 9 | 4 — "another parquet dumper" | 9 | 4 | 26 |
| C. **Parquet writer + Iceberg REST-catalog append commits in pure Go** (append-only Iceberg: write Parquet with Iceberg field IDs, then commit via REST catalog against Lakekeeper/Polaris/Nessie; CDC-upsert Iceberg explicitly roadmapped v0.2) | 7 — REST commit path is bounded, well-specified | 8 — "pure-Go Iceberg, no JVM" is a real differentiator | 7 — append-only avoids the delete-file swamp; falls back to Parquet on catalog errors | 9 — REST catalog + manifest handling shows real engineering | **31** |

**Decision: Option C.** v0.1 ships a rock-solid Parquet writer (with our 2PC fix) as the default
destination, plus **append-mode Iceberg via a REST catalog in pure Go** — full loads and
append-style incremental commit natively to Iceberg with zero JVM; CDC-upsert-into-Iceberg
(equality deletes) is honestly labeled v0.2 roadmap (upsert lands in Parquet mode meanwhile).
Every Iceberg failure degrades gracefully to Parquet files + a clear event. Marketing line is
honest: "Iceberg, no JVM required — append today, merge-on-read next."

## 5. Writer interface (concepts LakeSense adopts)

One destination interface: check (probe with real test write), per-thread setup returning prior
commit state, flatten+drift detection, evolve-schema (serialized per stream), batch write, drop,
close (commit or discard). A pool owns destination singletons and per-stream schema cells; each
reader thread gets its own writer thread with deterministic ID; commit state rides inside the
destination commit (see `state-and-recovery.md`). Our addition: every writer thread accumulates
row counts + order-independent checksums per stream and reports them in its commit sidecar/event.
