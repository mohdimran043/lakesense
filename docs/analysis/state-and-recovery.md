# Reference Analysis — State, Resume & Exactly-Once-ish Delivery

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.

## 1. State model

State is a single JSON document, versioned for backward compatibility, with a declared shape type:

- **STREAM-scoped**: a list of per-stream state objects — `{stream, namespace, sync_mode, state:{...}}`
  where the inner map holds arbitrary keys: cursor values, and a reserved `chunks` key.
- **GLOBAL-scoped**: one shared state (e.g., a WAL position for Postgres, binlog coordinates for
  MySQL — one log serves all streams) **plus** the set of stream IDs that participated in the last
  sync. Tracking participating streams matters: it tells the engine which streams already completed
  their initial backfill under this global position, and lets a stream migrate from incremental to
  CDC without a fresh full load (the incremental cursor doubles as a recovery cursor).
- **MIXED**: reserved for sources needing both (declared but unused in the reference).

Serialization details that matter conceptually: stream states that hold no values are dropped on
save (a "holds value" dirty flag); the state file is **rewritten after every mutation** (cursor
set, chunk removal, global update) so the on-disk file always reflects latest progress; chunk sets
survive a JSON round-trip via custom (de)serialization.

## 2. Resumable full load (chunk protocol)

1. First run: driver computes a full partition of the table into `{min,max}` chunks (strategy is
   driver-specific — see per-connector docs). The complete chunk set is written into state
   **before** any chunk is read.
2. Chunks are processed by a bounded concurrent pool with per-chunk retry. Each completed chunk is
   **removed from the state's chunk set** (and the file rewritten). A chunk is only marked complete
   if its context wasn't cancelled — a killed process never falsely completes a chunk.
3. Resume: if state contains a non-empty chunk set, chunking is skipped and only remaining chunks
   run. An **empty-but-present** chunk set means "backfill finished" (also recorded, for
   global-state sources, by stream membership in the global set).

Consequence: full-load delivery is *at-least-once per chunk*; a chunk interrupted mid-read is
re-read entirely. Dedup happens at the destination via the PK-hash record ID (upsert/merge), which
is what upgrades at-least-once reads into effectively-once destination rows.

## 3. Incremental resume & the destination-metadata trick

Incremental order of operations is deliberate:

1. Read previous cursor from state (typecast against schema — state stores normalized strings;
   timestamps in a fixed UTC format, object IDs as hex).
2. Ask the source for the **current max cursor** value(s) up front; write them into state before
   reading. The backfill then covers ≤ max; the incremental pass covers > previous cursor. A new
   max is only fetched when no chunks are pending — otherwise pending chunks (computed under the
   old max) would leak rows.
3. Stream rows newer than the previous cursor, tracking the running max observed in-data.
4. On writer close, the cursor value is committed **into the destination as writer metadata**,
   keyed by a deterministic thread ID derived from the stream + cursor bounds; only after that is
   the state file updated.

Crash recovery uses that ordering: on restart, the engine asks the destination for the last
committed writer metadata. If the destination committed but the state file missed the update
(crash in the gap), the destination's stored cursor wins and state is repaired — no re-read, no
loss. The same mechanism stores a list of **already-committed thread IDs** for full-refresh
chunks, so a resumed run skips chunks whose destination commit succeeded even if state didn't
record it.

This is the reference's "exactly-once-ish" backbone: **destination commit is the source of truth;
the state file is a cache of it.** Deterministic thread IDs (stream + chunk bounds / cursor
values) are what make the lookup possible.

## 4. CDC resume

- A "pre" phase establishes or validates the replication anchor (slot / binlog coordinates /
  resume token) and records it in global state *before* backfill starts — everything from that
  anchor forward is replayable, so no change can fall in a gap between snapshot and stream.
- Change events carry source positions as extra metadata columns; the "post" phase persists the
  final consumed position into state.
- During the backfill/CDC overlap window, CDC inserts are applied as **upserts** (dedup by record
  ID); after overlap, plain appends. Updates/deletes always merge by record ID.
- Interruption mid-stream: positions saved at the last checkpoint replay forward; replayed events
  are idempotent at the destination because of merge-by-record-ID.

## 5. Failure-handling machinery worth copying (as concepts)

- SIGINT/SIGTERM cancel a root context observed by every reader/writer; second signal
  hard-terminates.
- Retries wrap whole logical units (a chunk, a CDC session) with driver-configurable attempts and
  a non-retryable error sentinel for permanent failures.
- Workers recover panics into errors; any worker error cancels the sibling group.
- A separate context per logical unit isolates retry cancellation from the main run.

## 6. LakeSense decisions

- Adopt: chunk-set-in-state protocol; capture-max-cursor-first incremental; destination-metadata
  as recovery source of truth; deterministic thread/writer IDs; global vs stream state split;
  overlap-window upsert semantics; only-complete-on-clean-context rule.
- Improve: state mutations also emit JSONL `state_advanced` events (so the control plane tracks
  progress without reading files); state schema versioned from day one with explicit migration;
  checksum accumulators ride along with chunk completion so verification data exists even for
  interrupted runs; `backfill` (bounded resync) reuses the same chunk protocol with an explicit
  chunk-set namespace so it can never clobber CDC positions.
