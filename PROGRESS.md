# LakeSense — Build Progress

> **Resumability contract:** On every session start, read this file fully + `git log --oneline -5`,
> then resume from `## Next Action`. Never restart completed work. Update after EVERY task + commit.

## Phase 0 — Workspace Setup
- [x] Create project structure (reference/ backend/ ml/ frontend/ website/ deploy/ docs/) — done, building directly in repo root `LakeSense/`
- [x] `git init` + `.gitignore` (reference/, .env, node_modules, __pycache__, model artifacts)
- [x] Clone reference repos: olake + olake-ui into `reference/` (shallow, gitignored)
- [x] Create initial PROGRESS.md and commit

## Phase 1 — Reference Analysis
- [x] Study `reference/olake` → `docs/analysis/olake-engine.md` — key finds: stats.json rewritten every 2s during sync (rows/sec!), console-format zerolog lines (not JSON), `Total records read: N` line, state.json structure, no events/webhooks in engine
- [x] Study `reference/olake-ui` → `docs/analysis/olake-ui.md` — key finds: REST API on :8000, `/jobs/:jobid/tasks` = per-run history `{file_path,start_time,runtime,status}`, log fetch API, Temporal orchestration, alerting = ONE `webhook_alert_url` field (positioning confirmed)
- [x] [BRAINSTORM] Integration strategy → **Winner: OLake UI API polling** (see Decisions Log)

## Phase 2 — Product Spec
- [ ] Write `docs/SPEC.md`: all MVP features (event collector, rule engine, multi-channel delivery, ML anomaly [BRAINSTORM model], LLM enrichment, correlation [BRAINSTORM clustering], dashboard) + secondary features 8–13, each with user story / acceptance criteria / MVP-vs-v2 label
- [ ] Commit spec

## Phase 3 — Architecture & Scaffolding
- [ ] `docs/ARCHITECTURE.md` with mermaid diagram
- [ ] DB schema design: pipelines, events, rules, incidents, alerts, channels, metrics_baseline, acks + migrations
- [ ] Scaffold backend (FastAPI), verify runs empty, commit
- [ ] Scaffold ml service, verify runs empty, commit
- [ ] Scaffold frontend (React+Vite+Tailwind), verify runs empty, commit

## Phase 4 — Core Implementation
- [ ] 4.1 Event Collector + seed/demo mode (simulated OLake event stream)
- [ ] 4.2 Rule engine + severity + dedup/rate limiting
- [ ] 4.3 Channel adapters (Slack, Telegram, SMTP, generic webhook) behind one interface
- [ ] 4.4 ML anomaly service (baselines, scoring, cold start, emits anomaly_detected events)
- [ ] 4.5 LLM enrichment worker (strict JSON schema, retries, raw-alert fallback)
- [ ] 4.6 Correlation / incident grouping
- [ ] 4.7 Frontend: dashboard, events feed, incident view, rule builder, channels page [BRAINSTORM visual identity]
- [ ] 4.8 Secondary features 8–13 (build or stub behind "Coming soon" UI)

## Phase 5 — Dockerization
- [ ] Dockerfiles: backend, ml worker, frontend (multi-stage)
- [ ] `deploy/docker-compose.yml` (Postgres, backend, ml, frontend; profile-gated OLake UI) + `.env.example`
- [ ] One-command start verified: `docker compose up -d` → dashboard reachable, demo seedable

## Phase 6 — Testing
- [ ] Backend pytest: rule evaluation, dedup, correlation grouping, channel formatting (mocked sends)
- [ ] ML tests: baseline building + anomaly scoring on synthetic data (known anomaly detected, no storm on normal)
- [ ] Integration: compose healthy, seed → alert created → incident grouped
- [ ] Frontend: build passes + smoke test
- [ ] `docs/TEST-REPORT.md`

## Phase 7 — Documentation
- [ ] Product-grade `README.md` (SVG wordmark, badges, comparison table vs stock OLake, quickstart ≤5 commands, feature tour, architecture diagram)
- [ ] `docs/GETTING-STARTED.md`, `docs/CONFIGURATION.md`, `docs/API.md`, `docs/FAQ.md`
- [ ] `docs/BUSINESS.md` (target user, pricing tiers, 3 competitor comparisons)

## Phase 8 — Marketing Website
- [ ] [BRAINSTORM] Visual/brand direction (palette, typography, hero concept)
- [ ] Build `website/` static SPA (hero, problem, feature showcase w/ fake Slack alert, architecture, pricing, FAQ, footer)
- [ ] Real dashboard screenshots embedded
- [ ] Build/deploy instructions in docs

## Phase 9 — Final Pass
- [ ] Clean-machine dry run: fresh clone → README quickstart → everything works
- [ ] Final PROGRESS.md update + "Demo Script" section (5-min sequence)
- [ ] Tag `v0.1.0`

---

## Decisions Log

- **2026-07-19 — Project root layout:** Working dir is already `LakeSense/`; building the prescribed structure directly at repo root instead of a nested `lakesense/` subfolder. (No brainstorm needed — mechanical adaptation.)

- **2026-07-19 — [BRAINSTORM] Integration strategy** (speed / demo / reliability / AI-showcase, 1–10):
  - **A. OLake UI API polling** — poll `/jobs` + `/jobs/:jobid/tasks` on interval, diff task states into events, pull `/tasks/:taskid/logs` on failure for LLM enrichment. 8/7/8/7 = **30** ✅
  - B. Filesystem tailing of `/tmp/olake-config` (stats.json every 2s, olake.log, state.json) — richest metrics but requires co-mounted volume, hashed workflow dirs, ANSI log parsing. 6/8/5/7 = 26
  - C. Sync wrapper/sidecar — LakeSense invokes the OLake CLI itself, owns lifecycle. Reimplements orchestration; couples us to engine internals. 4/6/6/6 = 22
  - **Choice: A**, with two cheap add-ons kept in design (not MVP-blocking): accept OLake's own `webhook_alert_url` pointed at a LakeSense ingest endpoint (instant failure ping), and an optional stats.json volume reader as a v2 metrics booster. Collector is interface-driven so B can be added without touching the event pipeline. Demo mode (Phase 4.1 seed script) makes everything demoable with no OLake install at all.

---

## Next Action
Phase 2: write `docs/SPEC.md` (feature set with user stories, acceptance criteria, MVP/v2 labels; includes ML-model and correlation-clustering brainstorms), then commit.
