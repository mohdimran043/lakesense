# Quality Monitors (Design)

**Date:** 2026-07-21
**Spec refs:** `lakesense-final-prompt.md` §4.6 (Data-Quality Monitors), §2.6 (per-column stats)
**Status:** approved, ready to implement (on `main`)

## Goal

Close the last live-intelligence gap: make quality monitors actually run. The
`quality` package (freshness / volume / null-rate / distribution evaluators) is
built and tested but starved of data — the engine emits no per-column stats, so
`column_stats` is empty. This wires the full loop: **engine emits column stats →
collector persists → quality worker evaluates monitors → breaches flow down the
same ingest→rules→incident→alert path** as everything else.

## Non-goals (deferred, honest)

- **Distribution-drift / PSI monitors** — need a numeric histogram sketch the
  engine does not yet compute. The evaluator exists; wiring waits for the
  histogram (a clean follow-up). The worker skips `distribution` monitors with a
  note rather than half-evaluating.
- Auto-learning baselines from history — v1 uses the baseline stored on the
  monitor (set at creation / manual tune). Rolling baselines are a later refinement.

## Decomposition

- **Q1 — Engine emits column stats.** The runner accumulates cheap per-column
  counters as rows stream and emits one `column_stats` event per stream at finish.
- **Q2 — Collector persists them.** `Sink.RecordColumnStats` + ingester handling
  → `column_stats` rows.
- **Q3 — Quality worker.** Evaluates enabled monitors against the latest stats +
  stored baseline, records a `quality_result`, and on breach emits a
  `quality_breach` event (down the rules path).
- **Q4 — Monitor management.** `POST /quality-monitors` + `GET /quality-monitors`.

## Q1 — engine column stats

New event kind + payload in `engine/internal/events`:
```go
KindColumnStats Kind = "column_stats"
type ColumnStat struct {
    Column   string `json:"column"`
    Rows     int64  `json:"rows"`
    Nulls    int64  `json:"nulls"`
    Distinct int64  `json:"distinct"` // exact up to a cap, then capped estimate
    Min      string `json:"min,omitempty"`
    Max      string `json:"max,omitempty"`
}
type ColumnStats struct { Columns []ColumnStat `json:"columns"` }
```

The runner keeps `colStats map[string]*streamColStats` keyed by stream id.
`handleRow` folds each data column into that stream's accumulator (increment
rows; increment nulls when the value is nil; track min/max by string compare;
count distinct in a set capped at `distinctCap = 10000`, past which `Distinct` is
reported as the cap — an honest ">= cap" signal, no memory blowup).
`closeAndReport` emits `column_stats` for the stream from its accumulator, next
to the existing checksum events. Accumulation is O(1) per cell — negligible vs
the existing per-row hash.

## Q2 — collector persistence

`collector.ColumnStats` payload struct (consumer-side mirror) + a `Sink` method
`RecordColumnStats(ctx, pipelineID int64, stream string, syncID string, ts time.Time, cols []ColumnStat) error`.
`PgSink` inserts one `column_stats` row per column. The ingester's `derive`
handles `KindColumnStats`.

## Q3 — quality worker

New `quality.Worker` with seams:
```go
type Store interface {
    // Monitors returns enabled monitors (optionally for one pipeline).
    Monitors(ctx) ([]Monitor, error)
    // LatestStat returns the most recent column_stats row for a monitor's
    // (pipeline, stream, column); ok=false when none yet.
    LatestStat(ctx, pipelineID int64, stream, column string) (Stat, bool, error)
    // Freshness returns time since a pipeline's last sync.
    SinceLastSync(ctx, pipelineID int64) (time.Duration, bool, error)
    // RecordResult persists a quality_result.
    RecordResult(ctx, monitorID int64, syncID string, r Result, ts time.Time) error
}
type Monitor struct {
    ID int64; PipelineID int64; Stream, Column, Kind string
    Config map[string]float64  // e.g. {"threshold_seconds":3600} / {"max_deviation":0.5} / {"max_increase":0.1}
    Baseline map[string]float64 // e.g. {"mean":1000} / {"null_rate":0.02}
}
type Stat struct { Rows, Nulls int64; SyncID string; TS time.Time }
type Emit func(ctx, pipelineID int64, m Monitor, r Result) error
```
`Worker.Tick(ctx)` loads monitors and, per monitor kind, calls the matching pure
evaluator with the latest stat + config/baseline:
- **freshness** → `Freshness(sinceLastSync, config.threshold_seconds)`
- **volume** → `Volume(latest.Rows, baseline.mean, config.max_deviation)`
- **null_rate** → `NullRate(latest.Nulls/latest.Rows, baseline.null_rate, config.max_increase)`
- **distribution** → skipped in v1 (logged), pending the histogram.
Every evaluation records a `quality_result`; a breach also calls `Emit`, which
builds a `quality_breach` event and runs it through the ingester (so a rule on
`quality_breach` alerts, exactly like `anomaly_detected`). Wired in `run()` as a
periodic ticker (e.g. 2m) alongside the anomaly worker.

## Q4 — monitor management

`POST /quality-monitors` `{pipeline_id, stream, column?, kind, config, baseline}`
→ insert; `GET /quality-monitors` → list. Audited like other admin writes. (A UI
section can follow; not required for the loop to work.)

## Testing

- **Q1:** engine unit test — accumulate over a fake stream with known nulls/min/
  max/distinct; assert the emitted `column_stats`. `-race`, lint 0.
- **Q2:** gated PG test — ingest a `column_stats` event → rows land in `column_stats`.
- **Q3:** worker unit test (fake Store + fake Emit) — a null-rate monitor whose
  latest stat exceeds baseline+increase trips → `quality_breach` emitted and a
  result recorded; a within-threshold monitor does not.
- **Q4:** gated PG test — create + list a monitor; audited.
`make check` green throughout.

## Rollout

Commits on `main`, one per Q-step. Updates `PROGRESS.md` (4.6) to done, Track B
to literally 100%, and lets the website honestly claim quality monitoring.
