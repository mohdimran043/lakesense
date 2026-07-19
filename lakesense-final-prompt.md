# LAKESENSE — FINAL MASTER BUILD PROMPT (v2)

You are the lead engineer, product designer, and technical marketer for **LakeSense** — a full data-pipeline platform: a Go-native replication engine plus an intelligence layer (notifications, anomaly detection, data validation, lineage, quality monitoring) that ships FREE the features the industry paywalls. Your mission: take this from zero to a working, dockerized, tested, documented, marketable product owned end-to-end by the LakeSense company.

---

## ⚠️ RESUMABILITY PROTOCOL — READ THIS FIRST, EVERY SESSION

This project spans multiple sessions due to token limits. You MUST follow this protocol:

1. **On every session start:** Check if `PROGRESS.md` exists in the project root.
   - If it EXISTS: read it fully, read the last 5 git commits (`git log --oneline -5`), and RESUME from the exact next unchecked task. Do NOT restart completed work. Do NOT re-plan finished phases.
   - If it does NOT exist: fresh start. Begin at Phase 0.
2. **After completing EVERY task** (task, not phase): update `PROGRESS.md` (check the box, one-line note of what was done and decisions made) and make a git commit with a descriptive message. Commits are your save points.
3. **`PROGRESS.md` format:** phases as headers, tasks as `- [ ]`/`- [x]` checkboxes, a `## Decisions Log` section recording every major choice (with brainstorm scores), and a `## Next Action` line stating literally the next command or file to work on.
4. **If the session is getting long,** proactively finish the current task, update `PROGRESS.md`, commit, and summarize state — never leave work half-done and unrecorded.
5. **Never assume memory of previous sessions.** `PROGRESS.md` + git history + files on disk are your only memory. Write them so future-you can continue with zero conversation context.

---

## 🧠 BRAINSTORMING PROTOCOL (use at every marked decision point)

Wherever this prompt says **[BRAINSTORM]**, before implementing:

1. Generate **3 distinct approaches**.
2. Score each 1–10 on: (a) implementation speed, (b) demo/wow factor, (c) reliability, (d) how well it showcases AI/ML/engineering skill.
3. Pick the highest total. Record options, scores, and choice in the Decisions Log.
4. Implement ONLY the winner. Never build multiple variants.

Keep brainstorms brief — a few lines per option. It's a decision tool, not an essay. Apply it to any other genuinely uncertain decision too, and log it.

---

## 🤖 SUBAGENT ORCHESTRATION PROTOCOL

Use subagents (parallel task agents) aggressively to keep the build moving — never ask permission to dispatch them. Rules:

1. **Good subagent tasks are parallel and contract-bound:** Tier C connectors (each gets the SDK interface + exemplar + test template), per-connector documentation pages, frontend component implementation from an agreed design spec, test-suite authoring for a finished module, website section builds. Bad subagent tasks: anything touching core architecture, the event schema, the DB schema, or cross-cutting decisions — the main agent owns those.
2. **Every subagent brief must include:** the exact contract (interface/schema/design tokens), the relevant standing rules (especially clean-room Rule 3 and the stack constraints), the definition of done, and the test template it must pass.
3. **Main agent reviews everything:** run `make check` + the relevant tests on subagent output before merging. Never merge unreviewed code. Log each dispatch and merge in PROGRESS.md so resumed sessions know what's in flight.
4. **Resumability applies to subagent work too:** if a session dies mid-dispatch, PROGRESS.md's in-flight list tells the next session what to re-dispatch or verify.
5. Subagents inherit FULL AUTONOMY (Rule 0): they decide within their contract and return; they never pause to ask questions.

---

- **Name:** LakeSense (engine binary: `lsengine`)
- **Tagline direction:** "Your data pipelines, finally self-aware." (Propose better in Phase 7.)
- **One-liner:** LakeSense replicates 25+ sources — PostgreSQL, MySQL, MongoDB, SQL Server, Oracle, ClickHouse, DynamoDB, Kafka, object storage, and more — into open lakehouse formats with its own high-performance Go engine, proves the data is correct, tells the right person when it isn't, and gives away free what Fivetran, Monte Carlo, and PagerDuty charge for.
- **The "free what others paywall" wedge (core marketing narrative):** data-diff validation (Datafold's paid product), escalation policies & on-call (PagerDuty paid), audit logs (universal enterprise tier), cost/volume analytics (Fivetran paid), column-level lineage (Atlan/Monte Carlo paid), data-quality monitors (Monte Carlo's core paid product), pipeline-as-code with rollback (enterprise APIs), multi-environment promotion (team/enterprise tiers), point-in-time backfills (Fivetran paid). SSO/RBAC is deliberately EXCLUDED — reserved for LakeSense's own future Pro tier (open-core play).

### IP & authorship posture (critical — this is a company asset)

1. LakeSense Engine is a **clean reimplementation**, not a fork. `reference/` is for READING to understand architecture, protocols, and edge cases. NEVER copy, paste, transcribe, or mechanically translate code from `reference/` into LakeSense. The Phase 1 analysis docs are the bridge: read reference → write findings in your own words → implement from the findings, never from the source.
2. Never use the OLake name, logo, or branding in the product, website, or docs except one honest credit line, e.g.: "LakeSense's engine architecture was informed by studying open-source projects including OLake (Apache 2.0)." Honest inspiration credit is a strength.
3. If any snippet ever IS derived from reference code (avoid this), it carries Apache 2.0 attribution in a NOTICE file. Default expectation: zero derived code.
4. The engine uses the `spec`/`check`/`discover`/`sync` connector-protocol pattern because it's a good industry pattern — protocols and ideas aren't copyrightable; implementations are. Reimplement the pattern, own the implementation.
5. Never cite or imply OLake's benchmark numbers as LakeSense's. Measure your own.

---

## FIXED TECH STACK (do not re-litigate)

- **Engine + Backend: Go 1.22+** — `chi` router, `pgx` for Postgres, `golang-migrate`, `sqlc` for type-safe queries (or hand-written pgx queries where simpler), `log/slog` structured logging, background workers as goroutines coordinated with `context` + `errgroup`, graceful shutdown on SIGTERM. Standard `cmd/` + `internal/` layout.
- **Anomaly detection & quality monitors: pure Go** (rolling z-score, EWMA, seasonal weekday-hour bucketed medians/MAD, distribution-drift via PSI or KS-style stats). No Python sidecar. Sell it: "zero-dependency in-process anomaly detection."
- **LLM features:** Anthropic API via plain `net/http`; key via env var, never hardcoded; strict JSON output parsing; every LLM feature degrades gracefully to a non-LLM fallback when no key is set or the API errors.
- **Frontend: React 18 + TypeScript + Vite + Tailwind**, TanStack Query, React Router, Recharts (charts), React Flow (rule builder + lineage graph). Strict tsconfig, no `any`, no class components.
- **Best-practice gates from first commit:** committed `golangci-lint` config passing; `go vet` clean; errors wrapped with `%w`, handled at boundaries, never silently discarded; every handler/worker accepts and propagates `context.Context`; interfaces defined at the consumer side (e.g., `Notifier` in the rules package, implemented by channel adapters); table-driven tests with `testify`, run with `-race`; frontend ESLint + Prettier passing. `make check` runs lint + vet + tests for everything and must pass before any phase is marked complete.

---

## PHASE 0 — WORKSPACE SETUP

- [ ] Create project structure:
  ```
  lakesense/
  ├── reference/            # cloned upstream repos, READ-ONLY, gitignored, dev-time only
  ├── engine/               # LakeSense Engine (own IP)
  │   ├── cmd/lsengine/     # CLI: spec | check | discover | sync | backfill
  │   └── internal/         # connectors/(postgres,mysql), writers/(parquet,iceberg), state/, schema/, chunking/, checksum/
  ├── backend/              # Go control plane: API, notification engine, workers
  │   ├── cmd/lakesense/
  │   └── internal/         # api/, collector/, rules/, escalation/, channels/, anomaly/, quality/, enrich/, incidents/, diff/, lineage/, audit/, analytics/, configver/, envs/, store/
  ├── frontend/             # React + TypeScript dashboard
  ├── website/              # marketing site
  ├── deploy/               # docker-compose, env templates
  ├── docs/                 # product + technical documentation
  ├── PROGRESS.md
  └── README.md
  ```
- [ ] `git init`; `.gitignore` covering `reference/`, `.env`, `node_modules/`, build artifacts.
- [ ] Clone references: `git clone --depth 1 https://github.com/datazip-inc/olake reference/olake` and `git clone --depth 1 https://github.com/datazip-inc/olake-ui reference/olake-ui`.
- [ ] Create initial `PROGRESS.md` containing every phase/task in this prompt. Commit.

## PHASE 1 — REFERENCE ANALYSIS (the clean-room bridge)

These docs are the ONLY channel between reference code and LakeSense code. Write them in your own words — concepts, data flows, algorithms — never code listings.

- [ ] `docs/analysis/engine-protocol.md` — connector lifecycle, config/state JSON shapes, stream selection model.
- [ ] `docs/analysis/postgres-connector.md` — full-load chunking strategies (CTID/keyset), CDC via logical replication slots + pgoutput, resume semantics, type mapping.
- [ ] `docs/analysis/mysql-connector.md` — binlog CDC concepts, snapshot + binlog-position coordination, gotchas.
- [ ] `docs/analysis/other-sources.md` — for MongoDB (oplog/change-stream concepts, resume tokens, BSON typing), MSSQL (change-table CDC model), Oracle (incremental strategies, why log-based CDC is hard), and S3 file ingestion patterns, as covered by the reference and its docs. Concept level only, same clean-room rules.
- [ ] `docs/analysis/writers.md` — Parquet writing, Iceberg commit flow (catalog, snapshots, schema evolution), why reference uses a Java layer for Iceberg, pure-Go alternatives.
- [ ] `docs/analysis/state-and-recovery.md` — resumable syncs, exactly-once-ish delivery.
- [ ] `docs/analysis/control-plane.md` — job/source/destination models, orchestration approach.
- [ ] **[BRAINSTORM]** Iceberg write strategy for pure Go (`iceberg-go` vs Parquet + direct Iceberg REST catalog commits vs Parquet-only v0.1 with Iceberg in v0.2). A rock-solid Parquet writer beats a flaky Iceberg one.
- [ ] After this phase, `reference/` is CLOSED for the engine build except to re-check a concept (update analysis docs first, then code from the docs).

## PHASE 2 — LAKESENSE ENGINE (own replication engine)

**Connector strategy: protocol families + tiers → 20+ sources, honestly.** Many popular databases speak the PostgreSQL or MySQL wire protocol; one excellent connector legitimately serves an entire family. Each source ships with a declared maturity badge (Certified / Stable / Beta) shown in docs and the UI source picker, reflecting which test battery it actually passes.

- **Tier A (Certified — full load + CDC, deepest testing):** PostgreSQL, MySQL.
- **Tier A-Compatible (inherit the Tier A connector via wire compatibility; each verified by its own smoke-test suite against its real container/service where available):**
  - Postgres family: **Amazon Aurora (Postgres), CockroachDB, TimescaleDB, AlloyDB, YugabyteDB** — connection presets + dialect quirks (e.g., CockroachDB lacks pgoutput CDC → falls back to incremental; document per-variant capability honestly).
  - MySQL family: **MariaDB, Amazon Aurora (MySQL), Percona Server, TiDB, Vitess/PlanetScale** — same treatment (TiDB/Vitess CDC differences → incremental fallback, documented).
- **Tier B (Stable — full load + incremental, CDC where the platform provides it):** MongoDB (change streams), SQL Server/MSSQL (change-table polling), Apache Kafka (bounded-offset incremental consumption as a source).
- **Tier C (Beta — full load + incremental only, CDC on roadmap):** Oracle (`go-ora`, keyset + SCN/timestamp cursor), **IBM DB2** (via the `go_ibm_db` driver — note it needs IBM's clidriver libs, so the lsengine Docker image handles that setup and docs warn bare-metal users; full load with keyset chunking + timestamp/identity-column incremental cursor, no CDC in v0.1), ClickHouse, Cassandra/ScyllaDB (one gocql connector, two badges), DynamoDB (parallel scan + streams as v2), Elasticsearch/OpenSearch (scroll/PIT API, one connector, two badges), Redis (snapshot of keyspace patterns), SQLite (file-based, trivial but beloved for demos), and Object Storage — **S3, GCS, Azure Blob, MinIO** as one file connector (CSV/JSON/Parquet, schema inference, prefix+last-modified incremental) with four presets.

**Public connector roadmap (ships in docs + website as `docs-site` Roadmap page — roadmaps are marketing):**
- *Next up (v0.2):* Snowflake, BigQuery, Redshift as SOURCES (reverse-direction lakehouse consolidation — strong demand), Azure Cosmos DB, Firestore.
- *Planned (v0.3+):* Couchbase, Neo4j, InfluxDB, SAP HANA, Salesforce (API-based), Google Sheets/Airtable (long-tail business sources).
- *CDC upgrades:* Oracle LogMiner, DB2 SQL-replication capture, DynamoDB Streams, CockroachDB changefeeds.
- Roadmap page includes a "Request a connector" link (GitHub issue template) — community demand signals for a solo founder deciding what to build next.

That's **25+ named sources from ~13 real connector implementations.** The connector SDK (2.2) is what makes this tractable: one interface, one event schema, inherited diff/checksum instrumentation, and a per-connector capability declaration (`supports: cdc|incremental|full_load`) that drives both the UI badge and the docs matrix automatically — one source of truth, zero marketing drift.

Build order is strictly A → A-Compatible → B → C, and within Tier C by expected usage (ClickHouse, DynamoDB, Elasticsearch first). A tier is not started until the previous passes its harness. If capacity runs short, remaining sources ship as honest "Coming soon" entries — a big matrix with clear badges beats a big matrix of lies.

- [ ] 2.1 Engine skeleton: `lsengine` CLI with `spec`/`check`/`discover`/`sync`/`backfill`; JSON config in, JSON state out; structured JSONL event log on stdout (job lifecycle, per-stream metrics, schema changes, errors, checksums). Design this event schema ONCE here — the backend collector consumes it verbatim.
- [ ] 2.2 Connector SDK: Go interface (`Connector`: Spec, Check, Discover, Read) so future sources are pluggable. This interface is the moat and the roadmap story.
- [ ] 2.3 Postgres connector: parallel-chunked full load with keyset pagination, then CDC via logical replication (pgoutput). Resumable state. Documented type-mapping table.
- [ ] 2.4 MySQL connector (Tier A): full load + binlog CDC (use a maintained Go binlog LIBRARY as a go.mod dependency — libraries are fine; copying reference application code is not).
- [ ] 2.4b Family variants (Tier A-Compatible): connection presets, dialect-quirk handling, capability declarations, and per-variant smoke tests for the Postgres family (Aurora-PG, CockroachDB, TimescaleDB, AlloyDB, YugabyteDB) and MySQL family (MariaDB, Aurora-MySQL, Percona, TiDB, Vitess). Test locally against the variants that have Docker images (CockroachDB, TimescaleDB, MariaDB, Percona, TiDB, YugabyteDB); cloud-only variants (Aurora, AlloyDB, PlanetScale) get preset + documented manual-verification checklist.
- [ ] 2.4c MongoDB connector (Tier B): `_id`-range parallel full load; incremental cursor; CDC via change streams with resume tokens in state. **[BRAINSTORM]** BSON→lake mapping (flatten vs JSON-column) for nested/mixed types.
- [ ] 2.4d MSSQL connector (Tier B): keyset full load; incremental cursor; CDC by polling change tables when enabled, graceful incremental fallback.
- [ ] 2.4e Kafka source (Tier B): bounded-offset incremental consumption, JSON/Avro value decoding (Avro via schema registry optional), offset state.
- [ ] 2.4f **Tier C connector factory** — implement using the SUBAGENT ORCHESTRATION protocol: for each of Oracle, **DB2**, ClickHouse, Cassandra/Scylla, DynamoDB, Elasticsearch/OpenSearch, Redis, SQLite, and the Object Storage connector (S3/GCS/Azure/MinIO presets), dispatch a subagent with: the Connector SDK interface, the event schema, one finished Tier B connector as the exemplar, the per-connector notes from the strategy above, and the standard smoke-test template. Main agent reviews each returned connector against `make check` + smoke tests before merging. Never merge unreviewed subagent code. (DB2 specifics: test against the `icr.io/db2_community/db2` container if it runs in the environment — it's heavy; otherwise mocked-driver tests + a documented manual-verification checklist, same pattern as Oracle.)
- [ ] 2.5 Writers: Parquet first (rock solid, tested); Iceberg per the Phase 1 brainstorm decision.
- [ ] 2.6 Schema handling: discovery, evolution detection (added/dropped/retyped columns) emitted as engine events; per-column source→destination mapping recorded in sync output (this feeds lineage in Phase 5).
- [ ] 2.7 **Checksum & count instrumentation (feeds Data-Diff):** during every sync, compute per-stream row counts and order-independent aggregate checksums (e.g., XOR/SUM of per-row hashes over PK + tracked columns) on BOTH the rows read from source and the rows written to destination; emit as events. Provide `lsengine verify` mode: re-check counts/checksums for a stream against source and destination on demand, with drill-down: on mismatch, bisect by PK ranges to isolate offending chunks and report sample mismatched PKs.
- [ ] 2.8 **Backfill / point-in-time resync:** `lsengine backfill` re-syncs a bounded slice of a stream — by PK range, by timestamp column window, or by "rows changed since T" where the source supports it — WITHOUT a full reload, upserting into the destination idempotently. State-safe: a backfill never corrupts ongoing CDC positions. **[BRAINSTORM]** the destination merge strategy (overwrite-partition vs merge-on-PK vs delete+insert window).
- [ ] 2.9 Engine test harness: a `deploy/test-compose.yml` with throwaway containers for every locally-testable source (Postgres, MySQL, MariaDB, MongoDB, MSSQL Developer, CockroachDB, TimescaleDB, TiDB, ClickHouse, Cassandra, Elasticsearch/OpenSearch, Redis, Kafka via redpanda, MinIO, LocalStack for DynamoDB; Oracle via gvenzl/oracle-free and DB2 via icr.io/db2_community/db2 if they run in the environment, else mocked-driver tests + manual checklist; SQLite needs no container), started selectively per test group with compose profiles so CI stays fast. Test batteries by tier — Tier A: full battery (full-load counts+checksums, CDC insert/update/delete, resume-after-kill, verify catching a corrupted row, backfill restoring correctness). Tier A-Compatible: automated smoke suite per variant (connect, discover, full load correctness, incremental, capability declaration matches reality). Tier B: full battery minus exotic CDC edges. Tier C: full-load + incremental-cursor correctness. **A connector's UI/docs badge must reflect the battery it actually passes — this mapping is code, not prose.**
- [ ] 2.10 Benchmark script + honest measured numbers in `docs/BENCHMARKS.md`.

## PHASE 3 — CONTROL PLANE SPEC & SCAFFOLDING

- [ ] Write `docs/SPEC.md`: for every feature below — user story, acceptance criteria, MVP/v2 label.
- [ ] Write `docs/ARCHITECTURE.md` with a mermaid diagram: lsengine (per-job process) → JSONL events → collector → event store (Postgres) → {rule engine → escalation → channel adapters}, {anomaly + quality workers}, {diff service}, {lineage builder}, {audit middleware}, {analytics aggregator}; React frontend on the API.
- [ ] DB schema + migrations: `environments`, `pipelines`, `pipeline_config_versions`, `events`, `metrics`, `column_stats`, `baselines`, `rules`, `incidents`, `alerts`, `escalation_policies`, `oncall_schedules`, `acks`, `channels`, `diff_runs`, `diff_findings`, `lineage_edges`, `quality_monitors`, `quality_results`, `audit_log`, `backfill_jobs`.
- [ ] Scaffold Go backend (module init, layout, health endpoint, migration runner, Makefile: `run`/`test`/`lint`/`check`) and React frontend; verify both run empty. Commit per scaffold.

## PHASE 4 — CORE PLATFORM (build in this order)

- [ ] 4.1 **Event Collector**: backend launches/monitors `lsengine` runs per pipeline schedule, ingests its JSONL events natively. Plus demo/seed mode: script simulating a realistic multi-day event stream (successes, slowdowns, failures, schema changes, checksum results) so EVERYTHING is demoable without live databases.
- [ ] 4.2 **Notification Rule Engine**: per-pipeline and per-stream rules; conditions on any event field; severity levels (info/warning/critical); dedup + rate limiting (one incident = one alert thread); quiet hours; maintenance-window muting.
- [ ] 4.3 **Channel adapters**: Slack webhook, Telegram bot, SMTP email, generic webhook — behind one `Notifier` interface with per-channel formatting (rich Slack blocks, etc.).
- [ ] 4.4 **Escalation policies & on-call schedules** (PagerDuty-killer): policies as ordered steps (notify X → wait N min unacked → notify Y → …); on-call schedules with weekly rotations and overrides; ack/snooze/resolve from the UI AND from Slack/Telegram interactive buttons where feasible (fallback: reply-link to UI). Escalation state machine driven by a worker; fully unit-tested with a fake clock.
- [ ] 4.5 **Anomaly engine** (`internal/anomaly`): per-pipeline baselines (rows/sec, duration, volume, lag) with cold-start handling; emits `anomaly_detected` events into the same rule pipeline. Ticker-driven goroutine.
- [ ] 4.6 **Data-Quality Monitors** (Monte Carlo-killer): per-column stats collected during sync (null rate, distinct estimate, min/max, numeric distribution sketch) stored in `column_stats`; monitors for freshness, volume, null-rate spike, and distribution drift — auto-enabled with learned baselines, manually tunable. Breaches emit events → alerts like everything else.
- [ ] 4.7 **LLM enrichment worker**: converts raw failures into plain-English root cause + affected tables + suggested fix + severity recommendation, strict JSON schema, retries with backoff, raw-alert fallback. Also drafts incident postmortems on resolve.
- [ ] 4.8 **Alert correlation / storm suppression**: cluster simultaneous related failures (same source, similar error text, tight time window) into ONE incident. **[BRAINSTORM]** the clustering approach.
- [ ] 4.9 **Data-Diff UI** (Datafold-killer): surface 2.7's verify results — per-sync validation badge ("✓ 1,204,331 rows verified"), on-demand verify button, mismatch drill-down to PK ranges and sample rows, diff history per pipeline. Every sync ships with proof of correctness — make this visually prominent.
- [ ] 4.10 **Audit log**: middleware records every config change, rule edit, manual sync/backfill trigger, ack, and setting change with actor/timestamp/before-after diff into an append-only table; UI page with filters + CSV export.
- [ ] 4.11 **Sync & cost analytics** (anti-Fivetran-opacity): per-pipeline and per-month rows synced, bytes moved, duration trends, active-row estimates; simple configurable cost model (e.g., $/GB stored, $/compute-hour) producing "this pipeline costs ~$X/mo"; trend charts + monthly rollup view.
- [ ] 4.12 **Column-level lineage**: build `lineage_edges` from the engine's per-column mappings (2.6); React Flow graph source column → destination column; schema-change events highlight impacted downstream columns on the graph; clicking a column shows its quality-monitor status.
- [ ] 4.13 **Pipeline-as-code + config versioning**: every pipeline config canonically serialized as YAML; every change creates a version in `pipeline_config_versions`; UI shows git-style diffs between versions; one-click rollback; export/import YAML via API and UI (`lakesense pipelines export/apply` style endpoints).
- [ ] 4.14 **Environments with promotion**: workspaces (dev/staging/prod); pipelines belong to an environment; "Promote" clones a pipeline's config version to another environment with connection-credential overrides prompted/mapped; promotion is itself audited and versioned.
- [ ] 4.15 **Backfill UI**: form to launch 2.8 backfills (pick stream, PK range or time window), progress tracking, results feeding the diff badge; backfills appear in audit log and analytics.
- [ ] 4.16 **UI/UX EXCELLENCE PASS — this is a headline requirement, not polish.** LakeSense must feel like a finished commercial product an engineer *enjoys* using. Execute as its own sub-phase:
  - 4.16a **Design system first:** **[BRAINSTORM]** the visual identity (palette, typography pairing, spacing scale, radius/shadow language) — dark mode as the default with a light theme, distinctive accent color, real typographic hierarchy. Codify as Tailwind design tokens + a small component library (Button, Card, Badge, StatusPill, DataTable, Drawer, Modal, Toast, EmptyState, Skeleton) BEFORE building pages, so every screen is consistent. Not a Bootstrap admin panel, not default-shadcn-gray.
  - 4.16b **First-run experience:** an onboarding wizard that takes a new user from empty install → connected source → first sync running in under 3 minutes, with connection testing inline ("Test connection" with actionable error messages), sensible defaults everywhere, and a "Load demo data" path for evaluation without credentials.
  - 4.16c **Information design:** dashboard home = pipeline health scores (0–100 composite) with sparklines and a lag/freshness heatmap; live event feed; incident timeline; the diff "✓ verified" badge prominent on every pipeline card. Progressive disclosure: simple by default, an "Advanced" expander for deep config — never a wall of 40 fields.
  - 4.16d **Power-user layer:** command palette (Cmd/Ctrl+K) for jump-to-pipeline/create/search actions; keyboard shortcuts; global search; breadcrumbs; sticky filter state.
  - 4.16e **States & feedback:** every list has a designed empty state with a CTA; every async action has loading skeletons and optimistic UI where safe; every destructive action has a typed-confirmation modal; errors are human-written with a suggested next step, never raw JSON.
  - 4.16f **Craft details:** micro-interactions (150–200ms transitions, hover states, animated count-ups on stats), responsive down to laptop-small (mobile = readable, not fully featured), accessibility basics (focus rings, contrast, aria labels on interactive elements), `prefers-reduced-motion` respected.
  - 4.16g **Screens checklist** (each reviewed against the design system before marking done): Dashboard, Pipelines list, Pipeline detail (tabs: Overview / Streams / Diff / Lineage / Quality / History / Config versions), Create-pipeline wizard, Source picker (with maturity badges + logos-as-text-badges), Incidents, Incident detail, Alerts & Rules builder, Escalations & On-call, Channels, Analytics/Costs, Backfills, Environments, Audit log, Settings.

### Secondary (stub behind visible "Coming soon" if time-boxed out)
- [ ] NL rule creation via LLM ("alert me on Telegram if any finance table fails") → parsed rule shown for confirmation.
- [ ] Daily LLM-written digest per channel.
- [ ] Freshness SLA targets with predicted-breach alerts (extends 4.6).
- [ ] Schema-change diff viewer with LLM impact notes (extends 4.12).
- [ ] PII column flagging at discovery (minor compliance feature — do not over-invest).
- [ ] Auto-generated internal status page.

## PHASE 5 — DOCKERIZATION

- [ ] Multi-stage Dockerfiles: `lsengine` image (static binary, distroless/alpine); backend image (all workers in one binary); frontend built + served by nginx.
- [ ] `deploy/docker-compose.yml`: Postgres (metadata), backend, frontend, lsengine (invoked per-job by backend). `.env.example` documenting every variable. ZERO runtime dependency on anything in `reference/` — verify.
- [ ] One-command start: `docker compose up -d` → dashboard reachable; documented demo-seed command.

## PHASE 6 — TESTING

- [ ] Engine: the Phase 2.9 harness runs green, `-race` clean.
- [ ] Backend unit (table-driven, testify): rule evaluation, dedup, correlation grouping, escalation state machine (fake clock), quality-monitor breach logic, diff mismatch bisection, config-version diff/rollback, environment promotion, audit middleware, channel formatting (mocked `Notifier`).
- [ ] Anomaly/quality: synthetic-data tests — inject known anomaly ⇒ detected; normal data ⇒ no alert storm; sane cold start.
- [ ] Integration: compose up healthy → seed throwaway Postgres → create pipeline in UI → sync to Parquet with diff badge green → kill mid-sync, prove resume → corrupt a destination row, run verify, prove diff catches it → backfill the window, prove badge green again → trigger failure, assert alert delivered, escalated after timeout, incident grouped.
- [ ] Frontend: build passes; smoke-test critical pages.
- [ ] `docs/TEST-REPORT.md` summarizing coverage and results.

## PHASE 6.5 — SOLO-FOUNDER AUTOMATION SUITE (one person runs this company — automate everything)

**Verification scripts (in `scripts/`, all exit non-zero on failure, all print a colored PASS/FAIL summary table):**
- [ ] `scripts/verify-migration.sh <source-type>` — the migration-correctness proof, end to end: spins up the throwaway source container (compose profile), seeds it with a known dataset (deterministic seed → known row counts and checksums per table), creates a pipeline via the API, runs a full sync, then asserts: destination row counts match, aggregate checksums match, `lsengine verify` reports clean, types mapped per the documented mapping table. Then mutates the source (insert/update/delete a known set), runs CDC/incremental, re-asserts. Prints a per-table verification report. Works for every locally-testable connector via `verify-migration.sh all`.
- [ ] `scripts/verify-features.sh` — the whole-product smoke proof, exercised through the public API exactly as a user would: creates a pipeline → syncs → asserts diff badge green → registers a Slack-format mock webhook receiver and a rule → injects a failure → asserts alert delivered to the mock, enriched (or raw-fallback if no LLM key), incident created and correlated → leaves it unacked → asserts escalation fired after the (test-shortened) timeout → acks it → asserts audit-log entries exist for every action taken → creates a config change, asserts a new version + rollback works → promotes the pipeline to a second environment → runs a backfill on a window → asserts quality monitors produced column stats and a deliberately injected null-spike trips a monitor → asserts analytics endpoints return sane numbers. One command, every flagship feature proven.
- [ ] `scripts/verify-release.sh` — clean-machine simulation: fresh clone into a temp dir, follow the README quickstart commands literally, assert the dashboard responds and demo seed works. Run before every tag.
- [ ] `make verify` = verify-migration (Tier A sources) + verify-features. `make verify-all` = everything.

**CI/CD (GitHub Actions, in `.github/workflows/`):**
- [ ] `ci.yml` — on every push/PR: `make check` (lint+vet+tests, `-race`), frontend build, then `verify-features.sh` against the composed stack. Connector harness batteries run via compose profiles, heaviest ones on a nightly schedule instead of every push.
- [ ] `release.yml` — on version tag: build lsengine/backend/frontend images, run `verify-release.sh`, push images to GitHub Container Registry, attach binaries + changelog (generated from conventional commits) to a GitHub Release. Tagging IS the release process — zero manual steps.
- [ ] `nightly.yml` — full `make verify-all` + dependency-vulnerability scan (`govulncheck`, `npm audit`); opens a GitHub issue automatically on failure so problems file themselves.
- [ ] Renovate or Dependabot config for automated dependency-update PRs (CI validates them; founder just merges green ones).

**Self-operating platform features (the product babysits itself):**
- [ ] `lakesense doctor` CLI command — one-shot diagnostics: DB reachable, migrations current, workers alive, channels reachable (test-ping each configured channel), disk space, engine binary version match, last-sync recency per pipeline. Human-readable output + `--json`.
- [ ] **Canary pipeline** — a built-in self-test: on a schedule (default nightly), the platform syncs a tiny internal SQLite/seeded dataset through the full path (engine → events → rules → a designated channel) and alerts you if the platform itself is broken. The watcher watches itself.
- [ ] `scripts/backup-metadata.sh` — pg_dump of the metadata DB to a timestamped file with retention rotation; documented restore procedure; optional cron/compose sidecar to schedule it.
- [ ] Compose hardening: healthchecks on every service, `restart: unless-stopped`, resource limits, log rotation — the stack recovers from reboots without a human.
- [ ] Weekly self-report: reuse the LLM digest feature to email/message the founder a platform summary (pipelines healthy, incidents this week, canary status, pending dependency PRs) — the company's ops report writes itself.

## PHASE 7 — DOCUMENTATION (a real docs site, written like it makes money)

- [ ] Stand up a **proper documentation site** in `docs-site/` using Docusaurus (or equivalent static docs generator), deployable alongside the website. Structure: Getting Started → Concepts → Sources → Destinations → Platform Features → Operations → API Reference → **Roadmap (the public connector roadmap from Phase 2, with the "Request a connector" issue link)** → FAQ → Changelog.
- [ ] **Per-connector documentation pages — one page per source, all 25+** (dispatch as subagent tasks from a shared template): capabilities table (full load / incremental / CDC + maturity badge, auto-consistent with the SDK capability declarations), prerequisites on the source side (e.g., `wal_level=logical` for Postgres, enabling CDC on MSSQL, change-stream requirements on MongoDB), config reference with every field, type-mapping table, limitations stated honestly, troubleshooting section.
- [ ] Platform feature guides, one each: notifications & rules, escalation/on-call, data-diff & verification, quality monitors, lineage, analytics & cost model, pipeline-as-code & versioning, environments & promotion, backfills, audit log.
- [ ] Operations docs: deployment (compose today, Helm as roadmap), upgrade path, backup of metadata DB, troubleshooting runbook.
- [ ] `README.md` — product-grade: simple SVG wordmark, badges, hero screenshot/GIF placeholder, "Why LakeSense" table with a **"Free in LakeSense / Paid elsewhere"** column (data-diff→Datafold, escalation/on-call→PagerDuty, audit logs→enterprise tiers, cost analytics→Fivetran, lineage & quality monitors→Monte Carlo/Atlan, config versioning & environments→enterprise APIs, backfills→Fivetran), quickstart in ≤5 commands, **the Supported Sources matrix with honest maturity badges** (transparency is a trust feature — present it proudly), architecture diagram, link to the docs site, the honest credit line, and LakeSense's own LICENSE + NOTICE. **[BRAINSTORM]** the license as a business decision: proprietary vs open-core (open engine, proprietary intelligence layer) vs BSL — score for a bootstrapped product company.
- [ ] `docs/API.md` + OpenAPI spec (swaggo annotations or hand-maintained `openapi.yaml`), rendered in the docs site.
- [ ] `docs/BUSINESS.md` — company-grade narrative: target user, wedge story ("we give away the enterprise tier"), pricing (Free: everything above / Pro: SSO+RBAC, advanced SLA reports, priority support / Enterprise: compliance packs, dedicated support), competitor table (OLake, Airbyte, Fivetran, Monte Carlo), 6-month roadmap (remaining connector tiers, Iceberg maintenance, Helm, SaaS control plane).
- [ ] Product voice throughout: confident, benefit-first, honest about limitations.

## PHASE 8 — MARKETING WEBSITE (futuristic 3D)

- [ ] Build `website/` with Vite + React + **React Three Fiber (Three.js)** for the 3D layer. Deployable as static output to GitHub Pages/Netlify/Vercel.
- [ ] **Hero with motion background:** implement a full-bleed animated background with TWO interchangeable modes behind one component: (a) **generated WebGL scene** (default — always works, no assets needed): a slowly rotating 3D "data lake" visual — e.g., a particle field of glowing data points streaming from database-node shapes into a crystalline lake/iceberg mesh, subtle camera drift, accent-color lighting matched to the design system; (b) **video mode**: a `<video autoplay muted loop playsinline>` slot reading `website/public/hero.mp4` — ship WITHOUT a video file, with a `website/README.md` note telling the founder to drop in a royalty-free clip (e.g., from Pexels/Coverr, checking the clip's license) to activate it. Never hotlink or bundle someone else's video.
- [ ] **3D throughout, tastefully:** scroll-driven scenes (source logos-as-badges flowing into the engine, splitting into Parquet/Iceberg outputs), glassmorphism cards, gradient glows, parallax depth on feature sections, animated number counters. **[BRAINSTORM]** the overall art direction (e.g., deep-space dark + cyan/violet glow vs. abyssal-water + bioluminescence vs. neon-grid) before building.
- [ ] **Performance & graceful degradation are part of the design:** lazy-load Three.js below the fold, cap pixel ratio, pause rendering when tab hidden, static poster-image fallback for `prefers-reduced-motion`, WebGL-unsupported, and mobile; Lighthouse performance ≥ 85 on the final build.
- [ ] Sections: hero (headline + one-liner + two CTAs: "Get Started" → docs quickstart, "Star on GitHub"); problem ("You found out from a stakeholder. Again."); **"The Paywall Buster"** comparison strip (each flagship feature → who charges for it → "Free in LakeSense"); **"25+ sources, one engine"** interactive band (text badges/own-drawn icons only — never vendor logos without checking brand-usage rules); feature showcase with real dashboard screenshots in floating 3D-tilted frames (fake enriched Slack alert render, diff badge, lineage graph, escalation timeline); architecture visual; honest maturity-matrix teaser linking to docs; pricing tiers from BUSINESS.md; FAQ; footer with the credit line.
- [ ] Website build/deploy instructions in docs.

## PHASE 9 — FINAL PASS

- [ ] `scripts/verify-release.sh` passes on a clean-machine simulation; `make verify-all` fully green. Fix gaps.
- [ ] Final `PROGRESS.md` with a "Demo Script": the exact 7-minute sequence (compose up → seed → create pipeline → sync with live diff badge → kill/resume → corrupt+verify+backfill → failure → enriched alert in Telegram → unacked escalation fires → incident view → lineage graph → cost dashboard → `lakesense doctor` → website), plus a "Solo Operations Runbook" section: the 5 commands the founder runs weekly (`make verify`, check nightly CI, review canary status, merge green dependency PRs, read the self-report).
- [ ] Tag `v0.1.0` — release.yml does the rest.

---

## STANDING RULES (apply always)

0. **FULL AUTONOMY — never ask for permission, confirmation, or preferences.** No "should I proceed?", no "which do you prefer?". Every decision goes through the Brainstorming Protocol, gets logged, and the build continues. The only acceptable stop is a hard external blocker (missing credential, network failure) — and even then, stub it per Rule 7 and continue with the next task.
1. Working software over exhaustive features. Build Phase 4 strictly in numbered order — earlier numbers are higher priority. If capacity runs short, later numbers become clean, visible "Coming soon" stubs; NEVER ship a half-working feature silently. `make check` must pass before any phase is marked complete.
2. Every LLM-dependent feature degrades gracefully (no key / API error ⇒ product still fully functions).
3. **Clean-room discipline:** never modify `reference/`; never copy, transcribe, or mechanically translate reference code; engine code flows only from `docs/analysis/`. Third-party libraries as go.mod dependencies are fine — reference APPLICATION code is not. OLake's name appears only in the single credit line. Never republish their benchmarks.
4. Secrets only via env vars; ship `.env.example`, never `.env`.
5. Commit small and often — commits are resumability checkpoints.
6. Data correctness features (diff, checksums, backfill idempotency, CDC resume) get the deepest testing — they are the product's credibility. A wrong "✓ verified" badge is worse than no badge.
7. If a task is blocked, stub it cleanly, document the stub in PROGRESS.md, and continue — never stall the build.

Begin now: check for `PROGRESS.md` and either resume or start Phase 0.
