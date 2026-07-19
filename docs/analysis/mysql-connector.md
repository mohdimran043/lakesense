# Reference Analysis — MySQL Connector Concepts

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.

## 1. Full-load chunking (adaptive, four strategies)

Chunk planning starts from catalog statistics (row count, data length → average row bytes) and
derives a target rows-per-chunk so each chunk ≈ one target Parquet file (same 256 MiB × ~8×
compression heuristic as Postgres). Zero stats on a non-empty table is a hard stop with an
"ANALYZE TABLE" hint. Strategy selection, in preference order:

1. **Arithmetic integer split** — single integer PK (or configured chunk column) whose density
   factor `(max−min+1)/rowcount` is within sane bounds: boundaries are computed purely in math, no
   table scans. Open-ended sentinel chunks below min and above max catch rows inserted during
   planning.
2. **String-key numeric-space split** — char/varchar PKs: pad values to column width, map into a
   large-base integer space, compute candidate boundaries numerically, then snap each candidate to
   a real PK value with collation-aware "first key ≥ candidate" queries; accept only if enough of
   the expected chunks materialize, else fall through.
3. **Keyset walk** — general fallback incl. composite PKs: repeatedly select the boundary row at
   `ORDER BY pk LIMIT 1 OFFSET chunk_size` past the current boundary, using tuple-comparison
   predicates expanded to OR/AND chains.
4. **LIMIT/OFFSET** — last resort for PK-less tables; chunk bounds are row offsets (fragile under
   concurrent writes — acceptable only because CDC replay converges).

Each chunk reads in its own repeatable-read read-only transaction. **No global lock, no single
consistent snapshot**: like Postgres, consistency is DBLog-style — capture the binlog position
*before* backfill, replay CDC from it afterward with overlap-window upserts.

Resume: chunk set persisted up front, chunks removed as their writer commits, destination-side
committed thread IDs as the second guard (see `state-and-recovery.md`).

## 2. CDC via binlog

- **Library approach:** a maintained Go binlog replication library is consumed as a go.mod
  dependency (the `go-mysql-org/go-mysql` family — syncer + position types). LakeSense will do the
  same (libraries are fine; application code is not).
- **Positioning:** binlog file + offset only; GTID sets are *not* used for positioning. GTID
  events are consumed solely to harvest the microsecond-precision original-commit timestamp
  (MySQL ≥ 8.0.1) for the CDC-timestamp column; otherwise the coarser event-header timestamp.
- **Snapshot handoff:** pre-CDC (before backfill) captures the current master position via
  SHOW MASTER STATUS (SHOW BINARY LOG STATUS on ≥ 8.4; MariaDB's variant special-cased) plus a
  pseudo-random server ID, both persisted in global state. Backfill runs; streaming then replays
  from the pre-backfill position so snapshot-window changes are re-applied idempotently.
- **Bounded micro-batch:** each run captures the then-current end position and stops when
  consumption reaches it; an idle wait (default ~10 s) exits quietly on no traffic.
- **Decoding:** only rotate events (file rollover), GTID events (timestamps), and row events are
  processed. Row events are matched to streams via the table-map metadata; updates emit
  after-images only. Column names, enum/set member lists, signedness bitmaps, and collations all
  come from table-map metadata — hence **`binlog_row_metadata=FULL` is a hard prerequisite**.
  Enums resolve index→label (index 0 = MySQL's invalid sentinel → empty string); sets resolve
  bitmask→comma-joined labels; string/blob bytes re-decoded per collation. Events carry binlog
  file + position as extra metadata columns.
- **DDL:** query events ignored — safe because per-event table-map metadata always reflects the
  current shape; evolution is the writer's job.
- **Crash recovery:** destination metadata stores last committed position per stream; if it's
  ahead of state, a bounded recovery replay runs for lagging streams only, then fast-forwards.

## 3. State

Global: `{server_id, position:{file, offset}}`. Per-stream: remaining chunk set (backfill) or
cursor values (incremental). Resume rules mirror Postgres. Post-CDC persists the advanced
position only on clean (uncancelled) exit.

## 4. Type mapping (reference behavior)

| MySQL | Lake type | Notes |
|---|---|---|
| tinyint/smallint/mediumint/int (+unsigned tiny/small/medium) | int32 | signedness re-interpreted via table-map bitmap in CDC |
| bigint, unsigned int/bigint, year | int64 | unsigned bigint can exceed int64 — reference weakness |
| bit | int32 | |
| float | float32 | |
| double, decimal, numeric | float64 | lossy ≥16–17 significant digits |
| date, datetime, timestamp | timestamp | see timezone note |
| time | string | |
| char/varchar/text*, binary/blob* | string | collation-aware decode |
| json, enum, set | string | enum/set as labels in CDC |
| geometry | string (WKT) | WKB→WKT conversion |

**Timezone handling:** an effective timezone is resolved once (explicit override → session →
global → system, skipping "SYSTEM"; offset syntax parsed; UTC fallback) and applied to binlog
timestamp decoding so TIMESTAMP (stored UTC, rendered per-session) agrees between full load and
CDC. DATETIME is zone-less and passes through untouched.

## 5. Gotchas for our implementation & tests

- Prerequisites checked at setup via SHOW VARIABLES: `log_bin=ON`, `binlog_format=ROW`,
  `binlog_row_metadata=FULL`; failure downgrades to non-CDC modes with warnings (we'll surface
  this as a capability event, not just a log line).
- Server-ID collisions: time-derived ID persisted in state so reruns reuse identity. We'll also
  namespace ours per pipeline.
- **Binlog retention expiry mid-sync is unhandled in the reference** (purged-log error surfaces
  as a fatal generic failure). LakeSense: detect the purged-log server error explicitly, emit a
  `cdc_position_lost` event, and require an operator-confirmed re-snapshot — never silently.
- MySQL 8.4 command changes and MariaDB status-output differences must be version-gated.
- unsigned bigint: map to decimal/string rather than overflowing int64.
- decimal→float64 lossiness: same decision as Postgres — use decimal128 where precision known.
- Heartbeat + checksum verification enabled on the syncer; SSH/TLS reach both the SQL connection
  and the binlog dialer.
