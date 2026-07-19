# LakeSense — Product Specification

> LakeSense wraps the OLake replication engine with an intelligent event, anomaly-detection, and
> notification layer — so data teams stop discovering broken pipelines from angry stakeholders.
>
> Stock OLake alerting = one system-wide webhook URL that fires on job failure. LakeSense replaces
> that with a full notification engine. OLake itself is never modified; we observe it via its UI API
> (see `docs/analysis/`, Decisions Log).

Severity vocabulary used throughout: `info` · `warning` · `critical`.
Event type vocabulary: `job_started`, `job_succeeded`, `job_failed`, `duration_anomaly`,
`volume_anomaly`, `zero_rows_synced`, `schema_changed`, `cdc_lag`, `config_error`, `anomaly_detected`.

---

## 1. Event Collector — **MVP**

**User story:** As a data engineer, I want every OLake job run turned into typed events automatically, so downstream rules and ML operate on structured data instead of log spelunking.

**Design:** A poller (APScheduler loop, default 15 s) hits the OLake UI API (`/jobs`, `/jobs/:jobid/tasks`), diffs against the last-seen task state per pipeline, and emits events into the `events` table. On failure it fetches the tail of the task log (`/tasks/:taskid/logs`) and stores the error excerpt on the event payload. A **demo mode** generator produces a realistic simulated stream (successes, slowdowns, failures, schema changes, zero-row runs) so the entire product works with no OLake install.

**Acceptance criteria:**
- Task state transitions produce exactly one event each (`running→completed` ⇒ `job_succeeded` with `duration_s`, `rows_synced` when parseable from logs).
- A failed run produces `job_failed` with `error_excerpt` (≤4 KB) in the payload.
- Zero-rows successful run produces both `job_succeeded` and `zero_rows_synced`.
- Collector is idempotent across restarts (no duplicate events for the same task run; keyed on task `file_path` + status).
- Demo seeder: one command seeds N days of history + a live drip mode; documented.
- Ingest webhook endpoint (`POST /ingest/olake-webhook`) accepts OLake's stock webhook as a bonus low-latency failure signal, deduped against polled events.

## 2. Notification Rule Engine — **MVP**

**User story:** As an on-call engineer, I want per-pipeline rules with severity, dedup, and quiet hours, so I get one actionable page instead of forty duplicate pings at 3 a.m.

**Design:** Rules = rows in `rules`: `{name, enabled, pipeline_id (nullable = all), event_types[], conditions (JSON list of {field, op, value} over event payload), severity, channel_ids[], cooldown_s, quiet_hours (start/end/tz), maintenance_until}`. Evaluated synchronously on every event insert.

**Acceptance criteria:**
- Condition ops: `eq, neq, gt, gte, lt, lte, contains, regex`; missing field ⇒ condition false, never an exception.
- Dedup: identical (rule, pipeline, incident) within `cooldown_s` produces one alert; suppressed count is recorded and shown.
- Quiet hours defer non-critical alerts until the window ends; `critical` bypasses quiet hours by default (per-rule flag).
- Maintenance window mutes everything for the pipeline, recorded as suppressed.
- Rule CRUD via REST + UI.

## 3. Multi-Channel Delivery — **MVP**

**User story:** As a team lead, I want alerts in Slack, Telegram, email, or any webhook, formatted natively for each, so alerts land where each team already lives.

**Design:** `ChannelAdapter` interface: `send(alert) -> DeliveryResult`; implementations: `SlackAdapter` (incoming webhook, Block Kit), `TelegramAdapter` (bot API, MarkdownV2), `EmailAdapter` (SMTP, HTML), `WebhookAdapter` (JSON POST, HMAC signature header). Channel configs in `channels` table (secrets referenced from env where possible). Failed sends retry with backoff (3 attempts), final failure recorded on the alert row.

**Acceptance criteria:**
- Adding a new adapter touches only one new file + registry entry.
- Each adapter has a `format()` unit-testable without network; sends are mocked in tests.
- Delivery status (`sent/failed/retrying`) visible per alert in UI.
- A `test send` endpoint per channel.

## 4. ML Anomaly Detection — **MVP**

**User story:** As a data engineer, I want LakeSense to learn each pipeline's normal duration/volume/throughput and alert on deviations, without me configuring a single threshold.

**[BRAINSTORM] Model choice** (speed/demo/reliability/AI-showcase):
- **A. Rolling robust z-score (median + MAD) per pipeline-metric** — 9/7/8/6 = **30** ✅
- B. Isolation Forest (multivariate per-run features) — 7/7/6/8 = 28 (needs history; noisy cold start)
- C. STL seasonal decomposition + residual threshold — 5/6/5/8 = 24 (sync runs too sparse/irregular for seasonality early on)

**Winner: A.** Robust z-score with MAD handles small samples, outlier-resistant baselines, trivially explainable ("duration 2.1× median, z=4.2"). Isolation Forest noted as v2 upgrade once ≥50 runs exist per pipeline (module interface designed so it drops in).

**Design:** `ml/` worker recomputes per-pipeline baselines (`metrics_baseline`: metric, median, mad, n, updated_at) after each terminal event; scores new runs; emits `anomaly_detected` events (which flow through the same rule engine — anomalies are just events).

**Acceptance criteria:**
- **Cold start:** no anomaly scoring until ≥ `MIN_SAMPLES` (default 5) runs; explicitly reported as "learning" in UI.
- Injected 3× duration spike on a stable synthetic history ⇒ detected (|z| ≥ threshold, default 3.5).
- 100 normal synthetic runs ⇒ 0 anomaly events (no storm).
- Baselines update incrementally; anomalous runs are excluded from baseline updates.
- Scored metrics: `duration_s`, `rows_synced`, `rows_per_s`.

## 5. LLM Alert Enrichment — **MVP**

**User story:** As an on-call engineer, I want the alert to tell me the probable root cause, affected tables, and suggested fix in plain English, so I triage in seconds, not minutes of log reading.

**Design:** Enrichment worker consumes alerts pre-delivery. Calls Anthropic API (model via env, default `claude-sonnet-5`) with the event payload + error excerpt; strict JSON output schema: `{root_cause, affected_tables[], suggested_fix, severity_recommendation, confidence}`. Two attempts with JSON validation; **any failure ⇒ alert delivers raw immediately** — enrichment is additive, never blocking (hard timeout 20 s). No API key set ⇒ worker no-ops and the whole product still functions.

**Acceptance criteria:**
- Malformed LLM output / API error / no key ⇒ raw alert delivered; `enrichment_status` recorded (`enriched/skipped/failed`).
- Enriched fields render in every channel adapter's format.
- Prompt includes pipeline context (recent events, baseline stats) and is versioned in code.

## 6. Alert Correlation / Storm Suppression — **MVP**

**User story:** As an on-call engineer, when a database goes down and 12 pipelines fail together, I want ONE incident with 12 members, not 12 pages.

**[BRAINSTORM] Clustering approach** (speed/demo/reliability/AI-showcase):
- **A. Heuristic signature bucketing** — normalize error text (strip digits/ids/paths → signature hash); group events sharing (same source type OR same signature) within a sliding window (default 300 s) into one incident — 9/7/8/5 = **29** ✅
- B. TF-IDF + DBSCAN over error text in window — 6/7/6/8 = 27
- C. LLM-assigned incident membership — 7/8/4/8 = 27 (violates "must work without API key" as a core path)

**Winner: A.** Deterministic, testable, fast, explainable. The LLM already adds narrative on top (incident title/summary via enrichment when available), so AI showcase isn't lost.

**Acceptance criteria:**
- 12 simultaneous failures sharing a source ⇒ 1 incident, 12 member events, 1 alert (+ counter updates).
- Unrelated failure (different source + different signature) in the same window ⇒ separate incident.
- Incidents have status `open/acknowledged/resolved`; auto-resolve when the pipeline's next run succeeds.

## 7. Dashboard — **MVP**

**User story:** As a team lead, I want a single screen with per-pipeline health, live events, and open incidents, so I know the state of my lakehouse at a glance.

**Design:** React + Vite + Tailwind. Pages: **Overview** (health score cards + sparklines + open incidents), **Events** (live feed, filters), **Incidents** (timeline view, ack/snooze/resolve), **Rules** (builder UI), **Channels** (settings + test send), **Alert history**. Health score 0–100 composite: success rate (40) + anomaly-free rate (25) + freshness vs schedule (20) + open-incident penalty (15), computed server-side, documented in CONFIGURATION.md. Visual identity decided by Phase 4.7 brainstorm.

**Acceptance criteria:**
- Health scores update after each event batch; sparkline of last 20 runs per pipeline.
- Events feed auto-refreshes (poll ≤10 s); incident view shows member events on a timeline with ack/snooze.
- Rule builder produces valid rules without hand-writing JSON; channels page has test-send.
- Looks like a commercial product (dark mode default, distinctive palette, real typography).

---

## Secondary features (8–13) — build if time permits, else visible "Coming soon" stubs

| # | Feature | Label | Plan |
|---|---------|-------|------|
| 8 | **NL rule creation** — "alert me on Telegram if any finance table fails" → LLM parses to a rule object, shown for confirmation before save | v1.5 | Build if time: single endpoint + modal in Rules UI; degrades to hidden button without API key |
| 9 | **Daily LLM digest** per channel (yesterday's runs, anomalies, health movements) | v1.5 | Build if time: APScheduler cron + digest prompt; falls back to plain-stats digest without key |
| 10 | **Freshness SLA tracking** with predicted-breach alerts | v2 | Stub: SLA field on pipeline + "Coming soon" panel |
| 11 | **Schema-change diff viewer** with LLM impact notes | v2 | Event + raw diff shown; LLM notes stubbed |
| 12 | **PII column flagging** on discovery (name/email/phone regex heuristics) | v2 | Stub: flag list endpoint + badge UI; do not over-invest |
| 13 | **Auto-generated status page** (public, read-only health summary) | v2 | Stub: route + "Coming soon" |

---

## Out of scope (v0.x)

Multi-tenant auth/RBAC, SSO, escalation policies, on-call schedules, mobile apps, metric storage beyond Postgres, modifying OLake itself.
