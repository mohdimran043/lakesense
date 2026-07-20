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
- [x] 2.x Sync orchestrator (engine/internal/syncrun): source-agnostic runner driving full-load (chunk plan + resume) → incremental (cursor watermark) → CDC group (anchor→backfill→stream) through the SDK facets; consumer-side `Writer`/`StreamWriter` interface with **write-ahead `Flush` discipline** (rows durable before every state-commit boundary — ack-before-state); interim **NDJSON writer** (Parquet lands in 2.5, same interface); emits JSONL v1 events (sync_started/stream_*/chunk_completed/state_advanced/checksum_computed×2/sync_finished) with source+dest row digests seeding the diff badge. CLI verbs wired to real impls: spec→registry Spec JSON, check→connector Check, discover→catalog JSON, sync→orchestrator (backfill/verify remain stubs → 2.7/2.8). Fake-connector unit suite proves full-load+matching-checksums, **crash-resume without dup/loss**, incremental-past-cursor, CDC backfill+op-types. make check green (lint 0, vet clean, -race pass).
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
- [x] docs/SPEC.md — user story + acceptance criteria + MVP/v2 label per feature
- [x] docs/ARCHITECTURE.md with mermaid diagram (system flow + data flow + decisions)
- [x] DB schema + migrations — all 21 domain tables in backend/internal/store/migrations/0001_init (embedded); applied cleanly against a real postgres:16 container (22 tables incl. schema_migrations); down migration in FK-safe order
- [x] Scaffold Go backend: cmd/lakesense (errgroup workers + graceful shutdown), internal/{config,store,api,buildinfo}; chi router w/ healthz/readyz/version; golang-migrate embedded runner; deploy/.env.example. Verified live: migrate → serve → healthz/readyz/version 200. backend added to `make check` (lint 0, vet, -race). [ ] React frontend scaffold still pending.

## Phase 4 — Core Platform (strict numbered order)
- [x] 4.1 Event Collector + demo/seed mode. Collector (backend/internal/collector): consumer-side event envelope (decoupled from engine module), Ingester parses JSONL → lands raw `events` + derives `metrics` (sync_finished), `diff_runs` (pairs source/dest checksum_computed → match badge), `lineage_edges` (column_mapping), pipeline last_sync; tolerates malformed lines; narrow Sink interface (fake-tested, 4 cases) + PgSink. Seed (`lakesense seed --days N`): synthesizes multi-day history (healthy/slowdown/volume-drop/failure/schema-change/mismatch) through the REAL ingestion path. Verified vs postgres:16: 3 pipelines, 478 events, 39 metrics, 52 diff_runs (48 verified/4 mismatched), 17 lineage edges, 3 failures, 4 schema changes. make check green.
- [x] 4.2 Notification Rule Engine (backend/internal/rules): condition predicate over event fields (eq/ne/gt/gte/lt/lte/contains/exists/is_true/is_false) + severity + channels + dedup/quiet-hours/maintenance; open-fingerprint incident dedup (one incident = one alert thread); quiet hours mute delivery but still track; maintenance fully mutes. Fake-clock/fake-store/fake-notifier tests. make check green.
- [x] 4.3 Channel adapters (backend/internal/channels): multiplexing rules.Notifier for Slack (blocks), Telegram (bot), SMTP email, generic webhook; injectable http.Client + SMTP sender → httptest + fake-sender tested; per-channel formatting, non-2xx→error.
- [x] 4.4 Escalation policies & on-call (backend/internal/escalation): pure state-machine Tick() over Store+clock; ordered steps w/ per-step channels + on-call schedule; weekly ISO-week rotation + time-boxed overrides; exhaustion stops recurrence; fake-clock tested. (UI ack/snooze + chat buttons → pending frontend.)
- [x] 4.5 Anomaly engine (backend/internal/anomaly): robust modified-z (median/MAD) + Welford fallback, cold-start suppression, weekday-hour SeasonalKey; synthetic tests (spike/collapse detected, noise no-storm, seasonal no cross-contam).
- [x] 4.6 Data-Quality monitors (backend/internal/quality): freshness/volume/null-rate/distribution-drift (PSI, epsilon-smoothed); pure evaluators, exhaustive breach tests.
- [x] 4.7 LLM enrichment worker (backend/internal/enrich): Anthropic Messages API via net/http (opus-4-8), strict JSON (fence-tolerant), retry/backoff, postmortem drafts; MANDATORY graceful fallback (deterministic, error-code→cause/fix table, labeled Source:"fallback"); httptest-tested incl. degradation + 5xx retry.
- [x] 4.8 Alert correlation / storm suppression (backend/internal/correlate): [BRAINSTORM] chose hybrid (exact normalized signature + token-Jaccard fallback), time-windowed; Assign()→(key,isNew) suppresses storm members; tested (collapse/separation/normalization/fuzzy/window-expiry).
- [x] 4.9 Data-Diff API (verified badge + history): collector derives diff_runs from checksum pairs; read API exposes per-pipeline diff badge (latest-sync scoped) + diff history + verified-row counts. UI drill-down/on-demand-verify → pending frontend + engine `verify` (2.7).
- [x] 4.10 Audit log (backend/internal/audit): append-only Recorder seam + Log() helper + PgRecorder (no update/delete path); before/after field-diff (order-independent). Schema table + read endpoint live. Tested. [ ] write-middleware wiring on mutations + CSV export UI pending (needs write endpoints).
- [x] 4.11 Sync & cost analytics: read API `/analytics` — per-pipeline rows/bytes/duration totals + transparent configurable cost model ($/GB + $/compute-hr); trend charts + monthly rollup UI → pending frontend.
- [x] 4.12 Column-level lineage (data): collector builds lineage_edges from column_mapping; read API `/pipelines/{id}/lineage`. React Flow graph + schema-change highlighting → pending frontend.
- [x] 4.13 Pipeline-as-code + config versioning (backend/internal/configver): canonical deterministic YAML (equal configs render byte-identically; no-op changes skip), numbered immutable versions, git-style LCS line diff, append-only rollback. Tested. [ ] export/apply API + versions UI pending.
- [x] 4.14 Environments with promotion (backend/internal/envs): clone config version to target env with per-target credential overrides, streams/schedule preserved, source immutable, MissingCredentials() guard. Tested. [ ] promotion API + UI pending.
- [ ] 4.15 Backfill UI (launch, progress, diff-badge feedback) — blocked on engine 2.8 backfill verb
- [~] 4.16 UI/UX excellence pass (React dashboard built + dockerized + verified end-to-end via nginx; make check runs frontend build+lint):
  - [x] 4.16a Design system + [BRAINSTORM] "abyssal depth-sounder" identity → CSS-var tokens (dark default + light swap) + component library BEFORE pages (Card/Button/Badge/Skeleton/EmptyState/Stat + signature HealthMeter/VerifiedBadge/SeverityPill/Sparkline). Fonts self-hosted Space Grotesk/Geist/Geist Mono.
  - [x] 4.16c Information design (health scores + depth meters, sparklines, verified badges prominent, incident feed, tabs). [ ] freshness heatmap.
  - [x] 4.16d Power-user layer (Cmd/Ctrl-K command palette → jump to page/pipeline, breadcrumbs). [ ] global search beyond palette, sticky filters.
  - [x] 4.16e States & feedback (designed empty/loading-skeleton/error states everywhere, human error copy). [ ] typed-confirmation destructive modals (no destructive actions in read-only build yet).
  - [x] 4.16f Craft (micro-interactions, responsive, a11y focus rings, prefers-reduced-motion respected).
  - [x] 4.16g Screens BUILT: Dashboard, Pipelines list, Pipeline detail (Overview/Diff/Lineage tabs), Incidents, Data-Diff board, Analytics/Costs, Audit. [ ] REMAINING screens (need write endpoints): Create-pipeline wizard, Source picker, Incident detail, Alerts & Rules builder, Escalations & On-call, Channels, Backfills, Environments, Settings.
  - [ ] 4.16b First-run onboarding wizard (needs create-pipeline write path)
- [ ] Secondary (stub "Coming soon" if time-boxed): NL rule creation, LLM digest, SLA prediction, schema-diff impact notes, PII flagging, status page

## Phase 5 — Dockerization
- [x] Multi-stage Dockerfiles: engine/Dockerfile (static CGo-free lsengine, distroless-nonroot); backend/Dockerfile (control plane + bundled lsengine, alpine non-root, embedded migrations); frontend/Dockerfile (Vite build → nginx serving SPA + proxying /api). deploy/nginx.conf.
- [x] deploy/docker-compose.yml + deploy/.env.example; healthchecks (`lakesense doctor`), restart policies, log rotation; migrations on start; zero runtime dependency on reference/ (verified).
- [x] One-command start VERIFIED: `docker compose up` → backend healthy → `compose run backend seed` → published API returns 3 pipelines health=100/verified (1.1M rows) + cost estimate. Also delivered `lakesense doctor` (Phase 6.5 item, done early).

## Phase 6 — Testing
- [ ] Engine harness green, -race clean
- [ ] Backend unit suites (rules, dedup, correlation, escalation fake-clock, quality breach, diff bisection, config diff/rollback, promotion, audit, channel formatting)
- [ ] Anomaly/quality synthetic-data tests
- [~] Full integration flow — proven in pieces (engine crash-resume test, verify-migration checksum match, rules/escalation fake-clock suites, verify-features 11/11); not yet one scripted chain (awaits engine verify/backfill verbs + write API).
- [x] Frontend build gate (strict tsc + Vite build in make check). [ ] component smoke tests.
- [x] docs/TEST-REPORT.md — grounded: 104 test funcs / 22 packages, -race clean, both verify scripts, honest gaps.

## Phase 6.5 — Solo-Founder Automation Suite
- [x] scripts/verify-migration.sh <source-type> (+ `all`) — real sqlite→ndjson e2e, asserts source/dest checksum MATCH + exact counts + metadata; VERIFIED 9/9. scripts/lib.sh colored PASS/FAIL table.
- [x] scripts/verify-features.sh (whole-product API proof) — VERIFIED 11/11 against live compose stack (diff badges 3/3, 1.53M rows, analytics, lineage, health). Write-path assertions honestly TODO until those endpoints ship.
- [x] scripts/verify-release.sh (clean-machine simulation) — `git archive HEAD` → fresh build → quickstart; VERIFIED 8/8: the whole product builds from committed files only, dashboard + API serve seeded data. `make release-check`.
- [x] make verify / make verify-all (verify-features auto-skips when no stack reachable)
- [x] .github/workflows/ci.yml (make check + proofs vs postgres service), release.yml (tag→GHCR images + binaries), nightly.yml (govulncheck + npm audit + proof, auto-files issue). YAML validated.
- [x] Dependabot config (gomod ×2, npm, actions; weekly)
- [x] `lakesense doctor` CLI (config/db/migrations/freshness, --json, exit 0/1; wired as compose healthcheck)
- [ ] Canary pipeline (self-test nightly)
- [x] scripts/backup-metadata.sh — pg_dump → timestamped .sql.gz with retention rotation + documented restore; VERIFIED against a seeded DB (12K dump with pipeline data).
- [x] Compose hardening (healthchecks, restart: unless-stopped, log rotation) — done in Phase 5
- [ ] Weekly LLM self-report

## Phase 7 — Documentation
- [ ] docs-site/ (Docusaurus): Getting Started → Concepts → Sources → Destinations → Platform → Operations → API → Roadmap → FAQ → Changelog
- [ ] Per-connector pages, all 25+ (subagent tasks, shared template, capability-decl consistency)
- [ ] Platform feature guides (10)
- [ ] Operations docs (deploy, upgrade, backup, runbook)
- [x] README.md product-grade + [BRAINSTORM] license → open-core (Apache-2.0 core, Pro=SSO/RBAC). Full LICENSE + NOTICE (honest OLake credit). Free/Paid wedge table, sources matrix, quickstart, mermaid arch, wordmark.svg.
- [ ] docs/API.md + OpenAPI spec
- [x] docs/BUSINESS.md (wedge, Free/Pro/Enterprise pricing, competitor matrix, 6-month roadmap)

## Phase 8 — Marketing Website
- [x] website/ Vite + React + React Three Fiber, static build (dist/). make check runs the build.
- [x] Hero motion background: generated WebGL scene (particle field spiraling into a crystalline lake) + documented hero.mp4 video slot (no bundled footage).
- [x] [BRAINSTORM] art direction → "abyssal bioluminescence" (see Decisions Log); glassmorphism, reveal-on-scroll, glow headline.
- [x] Performance & degradation: Three.js code-split/lazy (~65kB gzip initial, 235kB three chunk lazy); static poster fallback for reduced-motion / no-WebGL / mobile / ?still.
- [x] Sections: hero, problem, Paywall-Buster strip, 25+ sources band, product showcase (REAL dashboard screenshots in tilted glass frames), architecture, pricing, FAQ, credit-line footer.
- [x] Build/deploy instructions (website/README.md). [ ] Lighthouse ≥85 not measured here.

## Phase 9 — Final Pass
- [x] verify-release.sh clean-machine pass (8/8); make check + verify green
- [x] Demo Script + Solo Operations Runbook (below)
- [ ] Tag v0.1.0 — held until write-path UI + integration flow land (a defensible v0.1.0 of the current product could be cut now via `git tag v0.1.0`; release.yml builds+pushes images on tag)

### 🎬 Demo Script (≈7 minutes)
1. **Deploy** — `cd deploy && cp .env.example .env && docker compose up -d` → wait for `docker compose ps` backend `healthy` (via `lakesense doctor`).
2. **Seed** — `docker compose run --rm backend seed --days 14` (realistic multi-day history: healthy, slowdown, volume-drop, failure, schema-change, mismatch — no live DB needed).
3. **Dashboard** — open `http://localhost:3000`. Fleet overview: health scores + aqua "✓ verified" badges + cost + incident feed.
4. **Correctness** — Data-Diff board: "1.53M rows verified", 0 mismatches. Open a pipeline → Diff tab → per-sync source/dest checksums.
5. **Prove it live** — `make verify` → migration-correctness (source/dest checksum MATCH on a real 500-row sync) + whole-product feature proof (11/11).
6. **Engine e2e** — `bash scripts/verify-migration.sh sqlite` shows the raw JSONL event stream + checksum match.
7. **Pipeline detail** — Overview trend chart (spot the volume-drop dip), Lineage tab (source→dest columns), Analytics (transparent cost model).
8. **Command palette** — Cmd/Ctrl-K → jump to any pipeline. Toggle light/dark.
9. **The website** — `cd website && npm run dev` → the bioluminescent hero + Paywall-Buster strip.

### 🛠 Solo Operations Runbook (weekly, ~5 commands)
1. `make verify` — migration-correctness + whole-product proof (needs a stack up).
2. Check nightly CI — `.github/workflows/nightly.yml` auto-files a GitHub issue on failure (govulncheck + npm audit + migration proof).
3. `lakesense doctor` (or the compose healthcheck) — DB/migrations/freshness at a glance; `--json` for scripts.
4. Merge green Dependabot PRs (CI validates each).
5. Before tagging: `make release-check` (clean-machine build) → `git tag v0.1.x` (release.yml builds+pushes images + cuts the GitHub Release).
- Pending automation: canary pipeline (nightly self-test), `scripts/backup-metadata.sh` (pg_dump + retention), weekly LLM self-report.

## Subagent Dispatch Log
- 2026-07-19: 5 read-only recon agents over reference/ (postgres, mysql, other-sources, writers, olake-ui). All 5 returned + distilled into analysis docs by main agent. None in flight.

## Decisions Log
- **2026-07-19 — v2 reset:** Working tree found with v1 artifacts deleted and `lakesense-final-prompt.md` (v2) added by the founder. Treated as intentional restart; committed as checkpoint `8ad0481`. v1 docs recoverable via git history but superseded — v2 phase structure governs.
- **2026-07-19 — [BRAINSTORM] Iceberg strategy (Phase 1):** (A) iceberg-go full integration 21 pts; (B) Parquet-only v0.1 26 pts; (C) Parquet + pure-Go append-mode Iceberg via REST catalog 31 pts. **Chose C**: default Parquet writer with our 2PC commit-marker fix; Iceberg append commits via REST catalog (Lakekeeper/Polaris class) in pure Go, no JVM; CDC-upsert Iceberg (equality deletes) honestly roadmapped v0.2; Iceberg failures degrade to Parquet + event. Full scoring table in docs/analysis/writers.md §4.
- **2026-07-19 — No Temporal:** control plane = one Go binary + Postgres; lsengine as supervised child process; cron scheduling in-process with fake-clock-testable worker. Rationale in docs/analysis/control-plane.md §6.
- **2026-07-20 — Write-ahead flush (orchestrator correctness):** while wiring the sync orchestrator, found the interim NDJSON path could record a completed-chunk / advanced-cursor / advanced-CDC-position in state while the rows were still in the bufio buffer — a crash there would lose rows the state claims are durable (Rule 6 violation). Fix: added `StreamWriter.Flush(ctx)` (bufio flush + fsync) to the Writer contract and call it at **every** state-commit boundary BEFORE the state mutation (per-chunk in full-load, before SetCursor in incremental, after backfill and after StreamChanges in CDC). This is the general ack-before-state discipline the Postgres CDC slot logic already followed, now enforced in the source-agnostic layer for all connectors and all destinations. Proven by the crash-resume unit test.
- **2026-07-20 — [BRAINSTORM] Connector breadth vs product depth (prioritization):** environment has no live MySQL/Mongo/etc. and CDC for those can't be verified here, while the product's wedge (intelligence layer + UX) is fully testable with synthetic/seed data. Options: (A) grind all 13 DB connectors ~14 pts (blocked on live DBs, low marginal value, risks unverifiable "certified" badges — Rule 6 hazard); (B) working demoable dockerized product first, connectors as honest Beta/Coming-soon ~30 pts; (C) 50/50 split ~22 pts. **Chose B**: added SQLite (real, server-less → powers demo + canary), then Phase 3/4 control plane. Remaining DB connectors (MySQL Tier A included) ship as honest badges until a live env verifies them — a big matrix with clear badges beats a matrix of lies (per the connector-honesty principle). MySQL remains the top connector to finish when a MySQL env is available.
- **2026-07-20 — [BRAINSTORM] Website art direction (Phase 8):** (A) deep-space + cyan/violet glow 29 (generic neon default); (B) abyssal-water bioluminescence 32 — particle field streaming into a crystalline lake, on-brand, matches the prompt's own hero description, extends the dashboard aqua with a violet depth glow; (C) neon-grid/tron 25. **Chose B.** WebGL hero code-split + static poster fallback (reduced-motion/no-WebGL/mobile/?still). Distinct from but harmonized with the dashboard's dark depth-sounder theme (marketing = richer glow/glass; product = restrained instrument).
- **2026-07-20 — [BRAINSTORM] Frontend visual identity (4.16a):** grounded in LakeSense's world (a data *lake* that *senses* its depth). (A) "sonar depth-sounder" — abyssal teal-navy dark, luminous aqua signal accent, mono numerals, health=depth-meter, verified=sonar-pill: 8/8/9/8=33; (B) lakehouse blueprint (hairline grid, blue-on-off-white): 29 (drifts to broadsheet AI-default); (C) bioluminescent neon+glass: 29 (drifts to neon AI-default; saved for the marketing site). **Chose A.** Fonts Space Grotesk/Geist/Geist Mono (not Inter/Roboto). Dark default + light via CSS-var swap. The "✓ N rows verified" aqua badge is the signature — makes the product's correctness proof the hero.
- **2026-07-20 — Command output shapes:** spec/discover print a single JSON document (schema / catalog) on stdout; check prints a human status line; data-path commands (sync/backfill/verify) emit the JSONL event stream. Keeps each command's stdout coherent for its consumer. Discover does NOT inject `_ls_` metadata columns into the catalog — those are engine-internal and injected at write time (dataColumns excludes them from checksums anyway).

## Next Action
The whole intelligence layer (4.2–4.8), the read API (4.9/4.11/4.12 data), and the dockerized deploy (Phase 5) are built and verified. The single largest remaining item is the **React frontend (4.16 + the UI halves of 4.9/4.10/4.12/4.13/4.14/4.15)** — Vite+React+TS+Tailwind, needs `npm`. Start with 4.16a: brainstorm the visual identity, codify Tailwind design tokens + a small component library (Button/Card/Badge/StatusPill/DataTable/Drawer/Modal/Toast/EmptyState/Skeleton), THEN build screens against the existing `/api/v1/*` endpoints (pipelines/incidents/analytics/diffs/lineage all live and returning seeded data). Dashboard first (health scores + diff badges + live feed), then Pipelines list/detail. Add a frontend build target to `make check` and a frontend+nginx image + service to docker-compose.

Remaining after frontend: 4.10 audit write-middleware, 4.13 config versioning (YAML canonical + diff + rollback — pure Go, testable), 4.14 environments promotion, 4.15 backfill (needs engine 2.7/2.8 verify/backfill first), the wiring of rules/escalation/anomaly/quality/enrich/correlate into the live collector→worker path (currently unit-proven in isolation), Phase 6 integration test, 6.5 verify scripts + CI, Phase 7 docs-site, Phase 8 website. Engine connectors beyond Postgres/SQLite remain honest Beta/Coming-soon per the logged breadth-vs-depth decision (MySQL is the top one to finish when a MySQL env is available).
