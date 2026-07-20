# LakeSense Benchmarks

**Honest, reproducible, and your own.** Every number here is produced by
`scripts/benchmark.sh` on the hardware named below — never borrowed from
another project. Re-run it on your machine and you'll get your machine's
numbers.

## Method

- **Workload:** full-load replication of a `1,000,000`-row table
  (`id, user_id, kind, amount, note (~200 B), created_at`) to the shipping
  NDJSON writer, which JSON-encodes and fsyncs every row to disk.
- **Throughput** is from the engine's own `sync_finished` accounting
  (rows & bytes written / engine duration); wall clock is also shown.
- **Postgres** uses keyset chunking; the v0.1 orchestrator reads chunks
  **sequentially** (parallel chunk readers are on the roadmap and will lift
  Postgres throughput well above this floor). **SQLite** is a single-reader,
  server-less file. In v0.1 both are bounded by the NDJSON writer.
- Measured on: **20 cores, 15Gi RAM**, Linux 7.0.0-28-generic x86_64, go1.26.1.

## Results

| Source | Rows | Data | Engine time | Rows/s | MB/s |
|---|--:|--:|--:|--:|--:|
| postgres | 1,000,000 | 391 MB | 10.75s | **92,985** | **36.4** |
| sqlite | 1,000,000 | 384 MB | 10.18s | **98,278** | **37.7** |

> NDJSON is the v0.1 writer (rock-solid, dependency-free). Parquet + Iceberg
> land in v0.2 and will raise these numbers — this is the conservative floor.

Reproduce: `ROWS=1000000 scripts/benchmark.sh`
