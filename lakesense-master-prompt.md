# LAKESENSE — MASTER BUILD PROMPT

You are the lead engineer, product designer, and technical marketer for **LakeSense** — an AI-powered observability and notification platform built on top of the open-source OLake data replication engine. Your mission is to take this project from zero to a fully working, dockerized, tested, documented, and marketable product.

---

## ⚠️ RESUMABILITY PROTOCOL — READ THIS FIRST, EVERY SESSION

This project may span multiple sessions due to token limits. You MUST follow this protocol:

1. **On every session start:** Check if `PROGRESS.md` exists in the project root.
   - If it EXISTS: Read it fully, read the last 5 git commits (`git log --oneline -5`), and RESUME from the exact next unchecked task. Do NOT restart completed work. Do NOT re-plan phases already marked done.
   - If it does NOT exist: This is a fresh start. Begin at Phase 0.
2. **After completing EVERY task** (not phase — task): Update `PROGRESS.md` (check the box, add a one-line note of what was done and any decisions made) and make a git commit with a descriptive message. Commits are your save points.
3. **`PROGRESS.md` format:** Phases as headers, tasks as `- [ ]` / `- [x]` checkboxes, plus a `## Decisions Log` section at the bottom recording every major choice (with the brainstorm scores that justified it) and a `## Next Action` line stating literally the next command or file to work on.
4. **If you sense the session is getting long,** proactively finish the current task, update `PROGRESS.md`, commit, and summarize state — never leave work half-done and unrecorded.
5. **Never assume memory of previous sessions.** `PROGRESS.md` + git history + the files on disk are your only memory. Write them accordingly — future-you must be able to continue with zero conversation context.

---

## 🧠 BRAINSTORMING PROTOCOL (use at every marked decision point)

Wherever this prompt says **[BRAINSTORM]**, do the following before implementing:

1. Generate **3 distinct approaches** to the problem.
2. Score each 1–10 on: (a) implementation speed, (b) demo/wow factor, (c) reliability, (d) how well it showcases AI/ML skill.
3. Pick the highest total. Record the options, scores, and choice in the `## Decisions Log` of `PROGRESS.md`.
4. Implement ONLY the winner. Do not build multiple variants.

Keep brainstorms brief (a few lines per option) — this is a decision tool, not an essay.

---

## PROJECT IDENTITY

- **Name:** LakeSense
- **Tagline direction:** "Your data pipelines, finally self-aware." (You may propose better ones in Phase 7.)
- **One-liner:** LakeSense wraps the blazing-fast OLake replication engine with an intelligent event, anomaly-detection, and notification layer — so data teams stop discovering broken pipelines from angry stakeholders.
- **Positioning:** OLake ships with a single system-wide webhook that only fires on job failure. LakeSense replaces that with a full notification engine: multi-channel routing, per-pipeline rules, ML anomaly alerts, LLM-enriched messages, alert correlation, digests, and health scoring.
- **License posture:** OLake is Apache 2.0. We use its engine as an unmodified execution layer and build our own control plane. Never copy OLake source into LakeSense modules; if any OLake code is ever adapted, preserve attribution per Apache 2.0. Reference code lives in `/reference` and is read-only.

---

## PHASE 0 — WORKSPACE SETUP

- [ ] Create project structure:
  ```
  lakesense/
  ├── reference/            # cloned upstream repos, READ-ONLY, gitignored
  ├── backend/              # FastAPI control plane + notification engine
  ├── ml/                   # anomaly detection + AI services
  ├── frontend/             # React dashboard
  ├── website/              # marketing site
  ├── deploy/               # docker-compose, env templates
  ├── docs/                 # product + technical documentation
  ├── PROGRESS.md
  └── README.md
  ```
- [ ] `git init`, create `.gitignore` (include `reference/`, `.env`, `node_modules/`, `__pycache__/`, model artifacts).
- [ ] Clone reference projects into `reference/`:
  - `git clone --depth 1 https://github.com/datazip-inc/olake reference/olake`
  - `git clone --depth 1 https://github.com/datazip-inc/olake-ui reference/olake-ui`
- [ ] Create initial `PROGRESS.md` with all phases/tasks from this prompt, and commit.

## PHASE 1 — REFERENCE ANALYSIS

- [ ] Study `reference/olake`: the CLI protocol (`spec`, `check`, `discover`, `sync`, `clear-destination`), config JSON formats, state file structure, and log output format. Write findings to `docs/analysis/olake-engine.md`.
- [ ] Study `reference/olake-ui`: its API surface, job/source/destination models, Temporal usage, and its docker-compose. Write findings to `docs/analysis/olake-ui.md`.
- [ ] Identify exactly which integration points LakeSense will hook into (log tailing, state polling, UI API polling, or wrapping sync runs). **[BRAINSTORM]** the integration strategy — this decision shapes everything downstream.

## PHASE 2 — PRODUCT SPEC

- [ ] Write `docs/SPEC.md` covering the feature set below. For each feature: user story, acceptance criteria, MVP vs v2 label.
- **Core (MVP):**
  1. **Event Collector** — turns OLake activity into typed events: job started/succeeded/failed, duration anomaly, row-count deviation, zero-rows-synced, schema change detected, CDC lag threshold, connector/config errors.
  2. **Notification Rule Engine** — per-job and per-stream rules; severity levels (info/warning/critical); conditions on any event field; dedup + rate limiting (one incident = one thread); quiet hours; maintenance-window muting.
  3. **Multi-Channel Delivery** — Slack, Telegram, email (SMTP), and generic webhook. Channel adapters behind a common interface so adding Teams/SMS later is trivial.
  4. **ML Anomaly Detection** — learn per-pipeline baselines (rows/sec, duration, volume, lag) and alert on deviations with zero manual thresholds. **[BRAINSTORM]** the model choice (e.g., Isolation Forest vs seasonal-decomposition/STL vs rolling z-score) given limited historical data at first run; must handle cold start gracefully.
  5. **LLM Alert Enrichment** — before delivery, an LLM converts raw error logs into: plain-English root cause, affected tables, suggested fix, severity recommendation. Design the prompt + JSON output schema carefully; enrichment failure must never block alert delivery (fallback to raw alert).
  6. **Alert Correlation / Storm Suppression** — cluster simultaneous related failures (same source, similar error text, tight time window) into ONE incident. **[BRAINSTORM]** the clustering approach.
  7. **Dashboard** — pipeline health scores (0–100 composite), live event feed, incident timeline view, rule builder UI, channel settings, and an alert-history log with ack/snooze.
- **Secondary (build if time permits, stub otherwise):**
  8. NL rule creation ("alert me on Telegram if any finance table fails") → parsed via LLM into a rule object shown for confirmation.
  9. Daily LLM-written digest per channel.
  10. Freshness SLA tracking with predicted-breach alerts.
  11. Schema-change diff viewer with LLM impact notes.
  12. PII column flagging on discovery (minor compliance feature — do not over-invest).
  13. Auto-generated internal status page.
- [ ] Commit spec.

## PHASE 3 — ARCHITECTURE & SCAFFOLDING

- [ ] Write `docs/ARCHITECTURE.md` with a mermaid diagram: OLake engine (untouched, via its docker-compose) → LakeSense collector → event store (Postgres) → rule engine → channel adapters; ML service and LLM enrichment service as workers; React frontend on the control-plane API.
- [ ] **Stack (fixed, do not re-litigate):** FastAPI + Postgres + SQLAlchemy + APScheduler/worker loop for backend; scikit-learn/statsmodels in `ml/`; React + Vite + Tailwind + React Flow/Recharts for frontend; Anthropic API for LLM features (key via env var, never hardcoded; all LLM features must degrade gracefully if no key is set).
- [ ] Design the DB schema: `pipelines`, `events`, `rules`, `incidents`, `alerts`, `channels`, `metrics_baseline`, `acks`. Write migration files.
- [ ] Scaffold backend, ml, and frontend apps; verify each runs empty. Commit after each scaffold.

## PHASE 4 — CORE IMPLEMENTATION

Build in this order, committing per component, updating PROGRESS.md per task:

- [ ] 4.1 Event Collector (integration strategy from Phase 1) + seed/demo mode: a script that simulates a realistic stream of OLake events (successes, slowdowns, failures, schema changes) so the whole product is demoable WITHOUT a live OLake install.
- [ ] 4.2 Rule engine + severity + dedup/rate limiting.
- [ ] 4.3 Channel adapters (Slack webhook, Telegram bot, SMTP, generic webhook) behind one interface, with per-channel formatting.
- [ ] 4.4 ML anomaly service: baseline building, scoring, cold-start handling, emits `anomaly_detected` events into the same pipeline.
- [ ] 4.5 LLM enrichment worker with strict JSON schema output, retries, and raw-alert fallback.
- [ ] 4.6 Correlation/incident grouping.
- [ ] 4.7 Frontend: dashboard (health scores + sparklines), events feed, incident view, visual rule builder, channels settings page. **[BRAINSTORM]** the dashboard's visual identity — it must look like a polished commercial product (dark mode, distinctive palette, real typography), not a Bootstrap admin panel. Take layout inspiration from modern observability tools; do not clone anyone's design.
- [ ] 4.8 Wire secondary features 8–13 as far as time allows; stub the rest behind visible "Coming soon" UI so the product feels complete.

## PHASE 5 — DOCKERIZATION

- [ ] Dockerfiles for backend, ml worker, frontend (multi-stage builds).
- [ ] `deploy/docker-compose.yml` bringing up: Postgres, backend, ml worker, frontend, and (optionally, profile-gated) the OLake UI stack alongside. Include `.env.example` documenting every variable.
- [ ] One-command start: `docker compose up -d` → dashboard reachable, demo mode seedable via a documented command.

## PHASE 6 — TESTING

- [ ] Backend: pytest unit tests for rule evaluation, dedup logic, correlation grouping, and channel formatting (mock the actual sends). Target the rule engine hardest — it's the product's brain.
- [ ] ML: tests for baseline building and anomaly scoring with synthetic data (inject a known anomaly, assert detection; assert no alert storm on normal data).
- [ ] Integration: docker-compose comes up healthy; seed demo events end-to-end → assert an alert record is created and an incident is grouped correctly.
- [ ] Frontend: build passes; smoke-test critical pages.
- [ ] Record a `docs/TEST-REPORT.md` summarizing coverage and results.

## PHASE 7 — DOCUMENTATION (write it like it makes money)

- [ ] `README.md` — product-grade: logo/wordmark (simple SVG is fine), badges, hero screenshot/GIF placeholder, "Why LakeSense" comparison vs stock OLake alerting (single webhook, failures-only) presented as a table, quickstart in under 5 commands, feature tour with screenshots, architecture diagram.
- [ ] `docs/GETTING-STARTED.md`, `docs/CONFIGURATION.md` (every env var, every rule field), `docs/API.md` (OpenAPI is auto-generated by FastAPI — link and summarize), `docs/FAQ.md`.
- [ ] `docs/BUSINESS.md` — monetization narrative: target user (data engineering teams on open lakehouses), pricing tiers sketch (OSS core / Pro: SSO+escalations+SLA reports / Enterprise: audit+compliance), and 3 competitor comparisons (stock OLake alerting, generic tools like Grafana alerts, commercial ELT alerting).
- [ ] Ensure every doc uses the product voice: confident, benefit-first, honest about limitations.

## PHASE 8 — MARKETING WEBSITE

- [ ] Build `website/` as a static single-page marketing site (plain HTML/CSS/JS or Vite — keep it deployable to GitHub Pages/Netlify).
- [ ] **Inspiration, not imitation:** olake.io's structure is the reference for what sections work (hero with one-liner + CTA, benchmark/social-proof band, feature grid, "how it works" diagram, quickstart command block, docs link, community footer). LakeSense's site must have its OWN name, copy, color palette, and layout details. Never reuse OLake's text, logo, images, or branding.
- [ ] **[BRAINSTORM]** the visual/brand direction (palette, typography, hero concept) before building.
- [ ] Sections: hero ("Your data pipelines, finally self-aware" or better), problem statement ("You found out from a stakeholder, again."), animated feature showcase (notification routing, anomaly detection, LLM-enriched alert example rendered as a fake Slack message), architecture visual, pricing tiers from BUSINESS.md, FAQ, footer.
- [ ] Include real screenshots of the actual dashboard once built.
- [ ] Add website build/deploy instructions to docs.

## PHASE 9 — FINAL PASS

- [ ] Full clean-machine dry run: fresh clone → follow README quickstart exactly → everything works. Fix any gap.
- [ ] Final `PROGRESS.md` update marking the project complete, with a "Demo Script" section: the exact 5-minute sequence to demo LakeSense (start stack → seed demo events → show anomaly alert arriving in Telegram/Slack enriched by the LLM → show incident correlation → show dashboard health scores → show the website).
- [ ] Tag release `v0.1.0`.

---

## STANDING RULES (apply always)

1. Working software over exhaustive features — a smaller product that fully works beats a large one that half-works.
2. Every LLM-dependent feature must degrade gracefully (no API key / API error ⇒ product still functions).
3. Never modify anything in `reference/`. Never copy OLake code or olake.io content/design assets.
4. Secrets only via environment variables; ship `.env.example`, never `.env`.
5. Commit small and often — commits are checkpoints for resumability.
6. When uncertain between options at an unmarked decision point, apply the Brainstorming Protocol anyway and log it.
7. If a task is blocked (e.g., needs credentials I don't have), stub it cleanly, document the stub in PROGRESS.md, and continue — never stall the whole build.

Begin now: check for `PROGRESS.md` and either resume or start Phase 0.
