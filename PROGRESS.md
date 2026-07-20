# LakeSense — Build Progress

> Source of truth for resumability. Canonical requirements: `lakesense-final-prompt.md` (v2).
> Protocol: read this file + `git log --oneline -5` on every session start, resume from **Next Action**.
> Prior history note: commits before `8ad0481` used the v1 prompt's phase numbering — ignore their numbering; v2 (this file) governs.

## Phase 0 — Workspace Setup
- [x] Project structure created (engine/, backend/, frontend/, website/, deploy/, docs/, docs-site/, scripts/, reference/)
- [x] git repo + .gitignore (reference/, .env, node_modules/, Go+Node build artifacts, .claude local)
- [x] Reference clones: `reference/olake`, `reference/olake-ui` (shallow, read-only, gitignored)
- [x] Initial PROGRESS.md with every phase/task — this file

## Phase 1 — Reference Analysis (clean-room bridge)
> These docs are the ONLY channel reference→LakeSense. Own words, concepts only, never code listings.
- [x] docs/analysis/engine-protocol.md — connector lifecycle, config/state JSON shapes, stream selection
- [x] docs/analysis/postgres-connector.md — chunking (CTID/keyset), pgoutput CDC, resume, type mapping
- [x] docs/analysis/mysql-connector.md — binlog CDC, snapshot+position coordination, gotchas
- [x] docs/analysis/other-sources.md — MongoDB, MSSQL, Oracle, DB2, Kafka, S3 (concept level)
- [x] docs/analysis/writers.md — Parquet, Iceberg commit flow, Java layer rationale, pure-Go alternatives
- [x] docs/analysis/state-and-recovery.md — resumable syncs, exactly-once-ish delivery
- [x] docs/analysis/control-plane.md — job/source/destination models, orchestration
- [x] [BRAINSTORM] Iceberg write strategy → DECIDED: Option C (see Decisions Log)
- [x] Close reference/ for engine build (re-open only via analysis-doc updates) — CLOSED 2026-07-19

## Phase 2 — LakeSense Engine
> Build order strictly: Tier A → A-Compatible → B → C. Badge = battery actually passed (code, not prose).
- [x] 2.1 Engine skeleton: lsengine CLI (spec/check/discover/sync/backfill/verify), JSON config/state I/O (atomic saves), JSONL event schema v1 in engine/internal/events (designed ONCE — envelope + 16 kinds + typed payloads); make check green (lint 0 issues, -race tests pass)
- [x] 2.2 Connector SDK: sdk.Connector + FullLoader/IncrementalReader/ChangeStreamer facets, capability declarations enforced in code (ValidateCapabilities), presets for wire-compatible variants, registry; model (lake types incl. decimal, two-layer catalog + validation); state (chunk-set protocol, cursors, global CDC anchor, atomic persist). make check green.
- [x] 2.3 Postgres connector (Tier A): CTID+keyset full load, incremental cursor, **pgoutput CDC** (ChangeStreamer: auto slot/publication, wal_level guard, ack-before-state slot discipline, TOAST-unavailable sentinel instead of silent null, resume). Unit tests + env-gated integration suite (`LAKESENSE_PG_IT=1`) green: check/discover, full-load exactly-once across chunks, incremental past-cursor, **end-to-end CDC insert/update/delete + resume**. make check 0 issues, -race clean.
- [ ] 2.4 MySQL connector (Tier A): full load + binlog CDC (maintained Go binlog library)
- [ ] 2.4b Family variants (A-Compatible): PG family (Aurora-PG, CockroachDB, TimescaleDB, AlloyDB, YugabyteDB), MySQL family (MariaDB, Aurora-MySQL, Percona, TiDB, Vitess) — presets, quirks, capability decls, smoke tests
- [ ] 2.4c MongoDB connector (Tier B): _id-range full load, incremental, change-stream CDC + [BRAINSTORM] BSON→lake mapping
- [ ] 2.4d MSSQL connector (Tier B): keyset full load, incremental, change-table polling CDC w/ fallback
- [ ] 2.4e Kafka source (Tier B): bounded-offset incremental, JSON/Avro decoding, offset state
- [ ] 2.4f Tier C factory via SUBAGENTS: Oracle, DB2, ClickHouse, Cassandra/Scylla, DynamoDB, Elasticsearch/OpenSearch, Redis, SQLite, Object Storage (S3/GCS/Azure/MinIO) — each reviewed vs make check + smoke tests before merge
- [ ] 2.5 Writers: Parquet first (rock solid); Iceberg per Phase 1 brainstorm decision
- [ ] 2.6 Schema handling: discovery, evolution events, per-column source→dest mapping (feeds lineage)
- [ ] 2.7 Checksum & count instrumentation + `lsengine verify` with PK-range bisection drill-down
- [ ] 2.8 Backfill/point-in-time resync (PK range / time window / changed-since-T), state-safe + [BRAINSTORM] merge strategy
- [ ] 2.9 Test harness: deploy/test-compose.yml, compose profiles, tiered batteries (A full / A-compat smoke / B / C), badge↔battery mapping in code
- [ ] 2.10 Benchmarks: script + honest measured numbers in docs/BENCHMARKS.md

## Phase 3 — Control Plane Spec & Scaffolding
- [ ] docs/SPEC.md — user story + acceptance criteria + MVP/v2 label per feature
- [ ] docs/ARCHITECTURE.md with mermaid diagram
- [ ] DB schema + migrations (environments, pipelines, pipeline_config_versions, events, metrics, column_stats, baselines, rules, incidents, alerts, escalation_policies, oncall_schedules, acks, channels, diff_runs, diff_findings, lineage_edges, quality_monitors, quality_results, audit_log, backfill_jobs)
- [ ] Scaffold Go backend (health endpoint, migration runner, Makefile run/test/lint/check) + React frontend; both run empty; commit per scaffold

## Phase 4 — Core Platform (strict numbered order)
- [ ] 4.1 Event Collector + demo/seed mode (multi-day simulated event stream)
- [ ] 4.2 Notification Rule Engine (conditions, severity, dedup, rate limit, quiet hours, maintenance windows)
- [ ] 4.3 Channel adapters: Slack, Telegram, SMTP, generic webhook behind `Notifier` interface
- [ ] 4.4 Escalation policies & on-call schedules (state machine worker, fake-clock tested, ack/snooze/resolve UI + chat buttons)
- [ ] 4.5 Anomaly engine (baselines, cold start, anomaly_detected events)
- [ ] 4.6 Data-Quality monitors (column_stats, freshness/volume/null-spike/drift, learned baselines)
- [ ] 4.7 LLM enrichment worker (strict JSON, retries, raw-alert fallback, postmortem drafts)
- [ ] 4.8 Alert correlation / storm suppression + [BRAINSTORM] clustering approach
- [ ] 4.9 Data-Diff UI (verified badge, on-demand verify, mismatch drill-down, history)
- [ ] 4.10 Audit log (append-only middleware, UI + CSV export)
- [ ] 4.11 Sync & cost analytics (rows/bytes/duration trends, cost model, monthly rollup)
- [ ] 4.12 Column-level lineage (lineage_edges, React Flow graph, schema-change impact highlighting)
- [ ] 4.13 Pipeline-as-code + config versioning (YAML canonical, git-style diffs, rollback, export/apply)
- [ ] 4.14 Environments with promotion (dev/staging/prod, credential overrides, audited)
- [ ] 4.15 Backfill UI (launch, progress, diff-badge feedback, audit + analytics integration)
- [ ] 4.16 UI/UX excellence pass:
  - [ ] 4.16a Design system + [BRAINSTORM] visual identity → Tailwind tokens + component library BEFORE pages
  - [ ] 4.16b First-run onboarding wizard (<3 min to first sync, demo-data path)
  - [ ] 4.16c Information design (health scores, sparklines, heatmap, live feed, progressive disclosure)
  - [ ] 4.16d Power-user layer (Cmd+K palette, shortcuts, global search, breadcrumbs, sticky filters)
  - [ ] 4.16e States & feedback (empty states, skeletons, typed confirmations, human errors)
  - [ ] 4.16f Craft details (micro-interactions, responsive, a11y, prefers-reduced-motion)
  - [ ] 4.16g Screens checklist (Dashboard, Pipelines, Pipeline detail tabs, Create wizard, Source picker, Incidents ×2, Alerts & Rules, Escalations, Channels, Analytics, Backfills, Environments, Audit, Settings)
- [ ] Secondary (stub "Coming soon" if time-boxed): NL rule creation, LLM digest, SLA prediction, schema-diff impact notes, PII flagging, status page

## Phase 5 — Dockerization
- [ ] Multi-stage Dockerfiles (lsengine static, backend, frontend+nginx)
- [ ] deploy/docker-compose.yml + .env.example; zero runtime dependency on reference/ (verified)
- [ ] One-command start + documented demo-seed

## Phase 6 — Testing
- [ ] Engine harness green, -race clean
- [ ] Backend unit suites (rules, dedup, correlation, escalation fake-clock, quality breach, diff bisection, config diff/rollback, promotion, audit, channel formatting)
- [ ] Anomaly/quality synthetic-data tests
- [ ] Full integration flow (seed→pipeline→sync→kill/resume→corrupt/verify→backfill→failure→alert→escalate→incident)
- [ ] Frontend build + smoke tests
- [ ] docs/TEST-REPORT.md

## Phase 6.5 — Solo-Founder Automation Suite
- [ ] scripts/verify-migration.sh <source-type> (+ `all`)
- [ ] scripts/verify-features.sh (whole-product API-level proof)
- [ ] scripts/verify-release.sh (clean-machine simulation)
- [ ] make verify / make verify-all
- [ ] .github/workflows/ci.yml, release.yml, nightly.yml
- [ ] Renovate/Dependabot config
- [ ] `lakesense doctor` CLI
- [ ] Canary pipeline (self-test nightly)
- [ ] scripts/backup-metadata.sh + restore docs
- [ ] Compose hardening (healthchecks, restart, limits, log rotation)
- [ ] Weekly LLM self-report

## Phase 7 — Documentation
- [ ] docs-site/ (Docusaurus): Getting Started → Concepts → Sources → Destinations → Platform → Operations → API → Roadmap → FAQ → Changelog
- [ ] Per-connector pages, all 25+ (subagent tasks, shared template, capability-decl consistency)
- [ ] Platform feature guides (10)
- [ ] Operations docs (deploy, upgrade, backup, runbook)
- [ ] README.md product-grade + [BRAINSTORM] license decision (proprietary vs open-core vs BSL)
- [ ] docs/API.md + OpenAPI spec
- [ ] docs/BUSINESS.md (wedge, pricing, competitor table, 6-month roadmap)

## Phase 8 — Marketing Website
- [ ] website/ Vite + React + React Three Fiber, static deploy
- [ ] Hero motion background (generated WebGL default + hero.mp4 video slot, no bundled video)
- [ ] [BRAINSTORM] art direction; scroll-driven 3D scenes, glassmorphism, counters
- [ ] Performance & degradation (lazy Three.js, reduced-motion poster, Lighthouse ≥85)
- [ ] All sections (hero, problem, Paywall Buster, 25+ sources band, showcase, architecture, matrix teaser, pricing, FAQ, credit-line footer)
- [ ] Build/deploy instructions

## Phase 9 — Final Pass
- [ ] verify-release.sh clean-machine pass; make verify-all green; fix gaps
- [ ] Final PROGRESS.md: 7-minute Demo Script + Solo Operations Runbook
- [ ] Tag v0.1.0

## Subagent Dispatch Log
- 2026-07-19: 5 read-only recon agents over reference/ (postgres, mysql, other-sources, writers, olake-ui). All 5 returned + distilled into analysis docs by main agent. None in flight.

## Decisions Log
- **2026-07-19 — v2 reset:** Working tree found with v1 artifacts deleted and `lakesense-final-prompt.md` (v2) added by the founder. Treated as intentional restart; committed as checkpoint `8ad0481`. v1 docs recoverable via git history but superseded — v2 phase structure governs.
- **2026-07-19 — [BRAINSTORM] Iceberg strategy (Phase 1):** (A) iceberg-go full integration 21 pts; (B) Parquet-only v0.1 26 pts; (C) Parquet + pure-Go append-mode Iceberg via REST catalog 31 pts. **Chose C**: default Parquet writer with our 2PC commit-marker fix; Iceberg append commits via REST catalog (Lakekeeper/Polaris class) in pure Go, no JVM; CDC-upsert Iceberg (equality deletes) honestly roadmapped v0.2; Iceberg failures degrade to Parquet + event. Full scoring table in docs/analysis/writers.md §4.
- **2026-07-19 — No Temporal:** control plane = one Go binary + Postgres; lsengine as supervised child process; cron scheduling in-process with fake-clock-testable worker. Rationale in docs/analysis/control-plane.md §6.

## Next Action
Engine sync orchestrator (engine/internal/syncrun): drive registry connector through full-load (chunk plan + resume via state pkg) → incremental → CDC phases, emit JSONL v1 events (sync_started/stream_*/chunk_completed/state_advanced/sync_finished), write rows through a consumer-side Writer interface. Ship a built-in NDJSON writer as the interim default (Parquet lands in 2.5, same interface). Then wire CLI verbs (spec→registry schema, check/discover→connector, sync→orchestrator) replacing the stubs in internal/cli/cli.go; update cli_test.go. make check green + commit.
