# LakeSense Test Report

Generated from the committed suite. Every gate below is enforced by
`make check` and CI (`.github/workflows/ci.yml`) — nothing here is aspirational.

## Summary

| | |
|---|---|
| Test functions | **104** |
| Tested packages | **22** (9 engine · 13 backend) |
| Race detector | ✅ all suites run with `-race` |
| Lint | ✅ golangci-lint v2, 0 issues (both modules) |
| Vet | ✅ clean |
| Frontend | ✅ strict `tsc` + Vite build |
| Website | ✅ Vite build |

Run it: `make check`. Prove the product: `make verify`.

## Engine (correctness is the product — deepest testing)

| Package | Tests | What's proven |
|---|---:|---|
| `syncrun` | 4 | full-load with **matching source/dest checksums**, **crash-resume without duplicates or loss** (write-ahead flush), incremental-past-cursor, CDC backfill + op-type tagging |
| `connectors/postgres` | 18 | check/discover, CTID+keyset chunking, incremental, **pgoutput CDC** insert/update/delete + resume (env-gated integration), type mapping |
| `connectors/sqlite` | 6 | capability-declaration match, discovery/type-affinity, rowid-range full load across chunks, incremental-past-cursor, chunk-query bounds |
| `state` | 4 | chunk-set protocol, cursor watermarks, global CDC anchor, atomic persist |
| `events` | 3 | JSONL envelope v1, emitter, sync-id |
| `sdk` · `model` · `config` · `cli` | 10 | capability enforcement, catalog validation, atomic config/state I/O, CLI verb dispatch |

Beyond unit tests, `scripts/verify-migration.sh` runs a **real end-to-end
migration**: seed 500 known rows → full sync → assert source/destination
checksums match, counts exact, metadata injected (**9/9 green**).

## Control plane — the intelligence layer

| Package | Tests | What's proven |
|---|---:|---|
| `rules` | 7 | condition matching & scoping, incident dedup (one incident = one thread), thresholds, boolean conditions, **quiet-hours** mute-but-track, **maintenance** full-mute (fake clock) |
| `escalation` | 4 | step-then-schedule-next state machine, on-call rotation + override, policy exhaustion, acked-not-escalated (fake clock) |
| `channels` | 6 | Slack blocks / Telegram / SMTP / webhook delivery (httptest + fake sender), non-2xx → error, unknown-type → error |
| `anomaly` | 6 | cold-start suppression, no-storm on noise, **spike & collapse detected**, constant-history fallback, seasonal no cross-contamination |
| `quality` | 5 | freshness, volume, null-rate, **PSI distribution-drift** breach/no-breach |
| `enrich` | 6 | strict-JSON LLM parse (fence-tolerant), **mandatory graceful fallback**, API-error degradation, 5xx retry, postmortem fallback |
| `correlate` | 7 | storm collapse, per-connector separation, digit-normalization, fuzzy near-duplicate, window expiry |
| `collector` | 4 | JSONL ingest → diff-pair/metric derivation, malformed-line tolerance, error propagation |
| `configver` | 6 | canonical-YAML determinism, round-trip, no-op-skip, LCS diff, rollback |
| `audit` | 4 | before/after capture, nil create/delete, field-diff, order-independence |
| `envs` | 3 | credential-override promotion, source-immutability, missing-credential warning |
| `api` · `store` | 2 | health/version handlers, composite health scoring, migrate-URL scheme |

## Whole-product proof

`scripts/verify-features.sh` exercises the running stack through the public API
exactly as the dashboard does — control plane up, **3/3 pipelines verified
(1.53M rows)**, health scoring, transparent analytics, column lineage, incidents
and audit endpoints (**11/11 green**). Verified against the dockerized compose
stack.

## Known gaps (honest)

- **Integration flow** — the full seed→sync→kill/resume→corrupt→backfill→
  failure→alert→escalate chain is proven in pieces (engine crash-resume test,
  verify-migration checksum match, rules/escalation fake-clock suites) but not
  yet as one scripted run; that awaits the `verify`/`backfill` engine verbs
  (2.7/2.8) and the write-path API.
- **Live-DB connectors** beyond Postgres/SQLite run env-gated or are honest Beta
  stubs — no unverified "certified" badge.
- Frontend/website have build + typecheck gates, not component tests.
