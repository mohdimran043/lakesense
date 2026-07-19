# LakeSense — Build Progress

> **Resumability contract:** On every session start, read this file fully + `git log --oneline -5`,
> then resume from `## Next Action`. Never restart completed work. Update after EVERY task + commit.

## Phase 0 — Workspace Setup
- [x] Create project structure (reference/ backend/ ml/ frontend/ website/ deploy/ docs/) — done, building directly in repo root `LakeSense/`
- [x] `git init` + `.gitignore` (reference/, .env, node_modules, __pycache__, model artifacts)
- [x] Clone reference repos: olake + olake-ui into `reference/` (shallow, gitignored)
- [x] Create initial PROGRESS.md and commit

## Phase 1 — Reference Analysis
- [ ] Study `reference/olake`: CLI protocol (spec/check/discover/sync/clear-destination), config JSON, state file, log format → `docs/analysis/olake-engine.md`
- [ ] Study `reference/olake-ui`: API surface, job/source/dest models, Temporal usage, docker-compose → `docs/analysis/olake-ui.md`
- [ ] [BRAINSTORM] Integration strategy (log tailing vs state polling vs UI API polling vs sync wrapping) — record in Decisions Log

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

---

## Next Action
Phase 1: read `reference/olake` (start with README.md, then connector CLI entrypoints and state/config handling) and write `docs/analysis/olake-engine.md`.
