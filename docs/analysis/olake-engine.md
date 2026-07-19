# Reference Analysis — OLake Engine (`reference/olake`)

*Analyzed 2026-07-19 from a shallow clone of `datazip-inc/olake` (Apache 2.0). Read-only reference; no code copied.*

## What it is

A Go CLI data-replication engine: one binary per source driver (`postgres`, `mysql`, `mongodb`, `oracle`, `db2`, `mssql`, `kafka`, `s3`), each registering itself via `connector.RegisterDriver()` into a shared cobra command tree (`protocol/`). Destinations (`iceberg`, `parquet`) self-register via side-effect imports. No server, no scheduler — pure run-to-completion CLI invoked per sync.

## CLI protocol

Commands (from `protocol/root.go`): **`spec`**, **`check`**, **`discover`**, **`sync`**, **`clear-destination`**.

Persistent flags that matter to us:

| Flag | Purpose |
|---|---|
| `--config` | source connection config JSON |
| `--destination` | writer (destination) config JSON |
| `--catalog` / `--streams` | streams.json — selected streams + sync modes |
| `--state` | state JSON path (CDC cursors, chunks) |
| `--destination-buffer-size` | batch size (default 10000) |
| `--no-save` | skip writing artifact files |
| `--difference` | emit `difference_streams.json` (schema diff between streams files — useful for schema-change detection) |

## Sync lifecycle (`protocol/sync.go`)

1. Unmarshal configs, compute `syncID = hash(configPath, destinationConfigPath)`.
2. Logs `Running sync with state: {...}` at start.
3. `Discover` → classify streams into FullLoad / Incremental / CDC.
4. Full-refresh streams: state cleared + destination dropped first.
5. Writer pool starts; `logger.StatsLogger` ticks **every 2 s**.
6. `connector.Read(...)` does the work; on finish logs `Total records read: %d`.
7. Telemetry marks sync started/completed (success boolean + read count).

## Artifacts on disk (all in the config folder — the integration goldmine)

| File | Content |
|---|---|
| `streams.json` | discovered/selected streams with schemas |
| `state.json` | `{type: STREAM\|GLOBAL\|MIXED, version, global?, streams: [{stream, namespace, sync_mode, state}]}` — CDC cursors, chunk progress |
| `stats.json` | **rewritten every 2 s during sync**: `Writer Threads`, `Synced Records`, `Memory` (mb), `Speed` (`"%.2f rps"`), `Seconds Elapsed`, `Estimated Remaining Time` |
| `difference_streams.json` | schema diff when `--difference` is used |
| `logs/sync_<UTC ts>/olake.log` | rotating (lumberjack, 100 MB) log file |

## Log format

zerolog with a `ConsoleWriter` → **human-readable lines, not JSON**, duplicated to stdout and `olake.log`:

```
<dim>2026-07-19 10:42:01</dim> <color>INFO</color> Running sync with state: {...}
2026-07-19 10:52:13 ERROR error occurred while reading records: ...
```

Timestamp format `2006-01-02 15:04:05` (UTC), ANSI color codes around level and timestamp on the console; the file writer gets the same formatted line. Parsing requires an ANSI-stripping regex + `^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(\w+)\s+(.*)$`.

Signal lines worth extracting: `Running sync with state:`, `Total records read: N`, `Clearing state for full refresh streams`, any `ERROR`/`FATAL`/`WARN` line, `Sync completed, wait 5 seconds cleanup in progress...`.

## Failure semantics

- Fatal errors exit non-zero with an `ERROR`-level final line (`error occurred while reading records: %s`).
- SIGINT/SIGTERM cancels via context (graceful) — a cancelled run is distinguishable from a crash only by exit path/logs.
- No retry logic at engine level; retries are an orchestrator concern (olake-ui/Temporal).

## Implications for LakeSense

1. **The engine emits no events and calls no webhooks.** All observability must be derived from: exit codes, log lines, `stats.json` snapshots, `state.json` deltas, and record counts.
2. `stats.json`'s 2-second cadence gives near-real-time rows/sec — ideal anomaly-detection input *if* we share the volume.
3. `Total records read: N` in logs gives per-run volume even without stats sampling.
4. Schema change detection maps to `difference_streams.json` / discover output diffing.
5. CDC lag must be derived from `state.json` cursor values vs wall clock (driver-specific cursor semantics — v2 territory).
