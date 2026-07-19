# Reference Analysis — PostgreSQL Connector Concepts

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.

## 1. Full-load chunking

Two strategies, selected by config:

- **CTID (physical) chunking — the default.** Read the DB block size and the table's page count
  from catalog stats; compute a page budget per chunk sized so one chunk ≈ one target Parquet file
  (the reference assumes ~8× compression over a 256 MiB file target). Chunks are half-open page
  ranges expressed as ctid literals; the final chunk is open-ended (max page sentinel) to catch
  rows appended during planning. Partitioned tables need special math: per-partition page counts
  via the partition-tree catalog (version-gated queries), padded ~5%, with the page budget split
  across partitions. Requires no PK at all — its key virtue.
- **Keyset (logical) chunking** when a chunk column is configured (must belong to the PK). Numeric
  columns step arithmetically between MIN and MAX; other types walk boundaries with repeated
  "max of the next N ordered values past the previous boundary" queries; last chunk open-ended.
  Known reference weakness: MIN/MAX-based splitting breaks on UUID columns.

Each chunk is read in its own **repeatable-read, read-only transaction** with a simple range
predicate. There is deliberately no global consistent snapshot across chunks — cross-chunk drift is
converged by CDC replay from a pre-backfill anchor (DBLog-style), with the overlap-window upsert
semantics described in `state-and-recovery.md`. For incremental first-runs, chunk reads carry a
"threshold filter" (cursor ≤ pre-captured max, with explicit NULL branch) so the later incremental
pass doesn't double-read.

## 2. CDC via logical replication

- **Slot posture:** the reference requires a pre-created logical slot (config names slot +
  publication); setup validates existence/type. The decoding plugin is discovered *from the slot*:
  pgoutput (needs a publication, proto v1) or wal2json fallback (with per-table filtering and
  LSN/timestamp inclusion options).
- **Snapshot coordination:** on first CDC run, read the server's current WAL LSN, store it as
  global state, and **advance the slot to that LSN before backfill starts**. The slot then retains
  everything after the anchor while backfill runs; replay closes the gap; PK-hash upserts kill
  overlap duplicates.
- **Bounded streaming (micro-batch, not daemon):** each sync opens a replication-protocol
  connection, targets the server's current WAL position at start, streams from the slot's
  confirmed-flush LSN, and exits when the client position reaches the target (pgoutput waits for a
  commit boundary). A configurable idle wait aborts if no traffic arrives.
- **Ack discipline (the critical correctness trick):** keepalive replies during the run report the
  *old* confirmed LSN — the slot is never advanced mid-sync. Only the post-CDC step sends a real
  standby status update, then **polls the slot until confirmed_flush_lsn actually matches** before
  writing the state file. Ack-before-state ordering means a crash leaves the slot behind (safe
  replay), never ahead (data loss).
- **Consistency guard:** before streaming, state LSN must equal the slot's confirmed-flush LSN
  exactly; mismatch aborts with an explicit "clear destination" error rather than risking silent
  gaps or duplicates.
- **Crash recovery via destination metadata:** if the lake's committed LSN for a stream is ahead of
  the state file, a bounded recovery replay runs only for lagging streams, pinned exactly to the
  lake-committed LSN so the slot ack can't overshoot.

## 3. Resume semantics

Global state = `{lsn}` + set of stream IDs whose backfill completed under that anchor. Stream
state = remaining chunk set during backfill, cursor values for incremental. Resume rules: chunks
present → finish them; chunks empty + stream in global set → CDC-only from stored LSN; blank
global state → re-anchor and full re-snapshot. Destination-side committed chunk IDs / LSNs are the
durable cross-check throughout.

## 4. Type mapping (conceptual, reference behavior)

| Postgres | Lake type | Notes |
|---|---|---|
| int2/int4 (+serial) | int32 | |
| int8/bigserial | int64 | |
| float4 / float8 | float32 / float64 | |
| numeric/decimal | float64 | **lossy** — reference has no decimal type |
| boolean | bool | |
| date, timestamp, timestamptz | timestamp (precision variants) | unparseable values silently fall back to epoch (a flaw) |
| time/timetz/interval | string | |
| uuid, json/jsonb, xml, bytea, money, inet/cidr/macaddr, geometric, tsvector, hstore, enum, bit, pg_lsn | string | jsonb kept as raw text |
| arrays (any element) | array | scalars encountered where arrays expected get wrapped |
| unknown | string | logged fallback |

pgoutput delivers values as text; an OID→name table routes them through the same mapping.

**LakeSense decisions:** keep CTID-default + keyset-optional split; support UUID keyset via
lexicographic walk (fix their gap); map `numeric` to decimal128 in Parquet rather than lossy
float64 where scale/precision are known; never silently fall back to epoch on unparseable
timestamps — emit a typed error event instead; same slot-ack discipline (it is correct); document
`wal_level=logical`, publication, and REPLICA IDENTITY FULL guidance in connector docs. We create
the slot/publication for the user when permissions allow (reference makes users do it manually —
onboarding friction we can remove), with an explicit opt-out.

## 5. Edge cases to carry into our test battery

- Unchanged TOAST columns in updates: recoverable only under REPLICA IDENTITY FULL; otherwise the
  reference substitutes an explicit "unavailable" sentinel (never null) and warns per relation.
- wal2json deletes carry only identity columns.
- Tables with no PK (CTID handles); composite PKs; partitioned tables; empty partitions.
- LSN mismatch state-vs-slot; slot-lag growth when syncs are infrequent.
- Discovery: skip system schemas, respect privileges, include materialized/foreign/partitioned
  relations; degrade unknown column types to string rather than failing discovery.
