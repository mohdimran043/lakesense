# LakeSense Product Spec

Format per feature: **user story** · **acceptance criteria** · **scope**
(MVP = v0.1, v2 = later). Features are built in the numbered Phase-4 order;
lower numbers ship first, later ones become honest "Coming soon" stubs if
time-boxed out. LLM-dependent behaviour always has a non-LLM fallback.

---

## Replication (engine)

**Story.** As a data engineer I connect a source, pick streams, and get them
replicated into an open format with proof the copy is correct.

**Acceptance.**
- `spec`/`check`/`discover`/`sync` work file-in, JSONL-events-out.
- Full load resumes after a crash with no duplicated or lost rows.
- Incremental reads only rows past the stored cursor.
- CDC (where supported) captures insert/update/delete and resumes from its
  durable position.
- Every sync emits matching source & destination checksums when correct.

**Scope.** MVP: PostgreSQL (full+CDC), SQLite (full+incremental), NDJSON writer.
v2: MySQL CDC, Tier B/C connectors, Parquet & Iceberg writers.

---

## 4.1 Event Collector + demo seed

**Story.** As an operator I want the platform to run pipelines on a schedule and
capture everything they emit, and I want to evaluate the product without a live
database.

**Acceptance.** Collector launches `lsengine`, parses JSONL, and lands `events`
plus derived `metrics`/`column_stats`/`diff_runs`/`lineage_edges`. A `seed`
command generates a realistic multi-day event stream (successes, slowdowns,
failures, schema changes, checksum results) so every screen is demoable with no
external source. **MVP.**

## 4.2 Notification Rule Engine

**Story.** As on-call I want to be told when a pipeline breaks — once per
incident, not once per event — with severity and quiet hours respected.

**Acceptance.** Per-pipeline/per-stream rules with a condition predicate over any
event field; severity info/warning/critical; dedup + rate-limit so one incident
is one alert thread; quiet-hours and maintenance-window muting. Table-driven
tested. **MVP.**

## 4.3 Channel adapters

**Story.** As an admin I route alerts to Slack, Telegram, email, or a webhook.

**Acceptance.** One `Notifier` interface (defined in the rules package,
consumer-side) with per-channel formatting (rich Slack blocks, Telegram
markdown, HTML email). Mocked `Notifier` in tests. **MVP.**

## 4.4 Escalation & on-call

**Story.** As a team lead I want unacked criticals to escalate up a chain on a
rotation, and I want to ack/snooze/resolve from the UI or chat.

**Acceptance.** Policies as ordered steps (notify X → wait N min unacked →
notify Y); weekly on-call rotations with overrides; escalation driven by a
worker with a **fake clock** in tests; ack/snooze/resolve from UI and from chat
buttons (fallback: reply link). **MVP** (chat buttons v2 where infeasible).

## 4.5 Anomaly engine

**Story.** As on-call I want to hear about a pipeline that's subtly wrong (half
the usual rows, twice the usual duration), not just hard failures.

**Acceptance.** Pure-Go per-pipeline baselines (rows/sec, duration, volume, lag)
using rolling z-score / EWMA / weekday-hour bucketed median+MAD, with cold-start
handling; emits `anomaly_detected` into the rule pipeline; no alert storm on
normal data. **MVP.**

## 4.6 Data-Quality monitors

**Story.** As a data owner I want freshness/volume/null-rate/distribution
monitored per column with learned baselines, and to hear when they breach.

**Acceptance.** Per-column stats collected during sync (null rate, distinct
estimate, min/max, numeric sketch); monitors auto-enabled with learned
baselines, manually tunable; breaches emit events like everything else. **MVP.**

## 4.7 LLM enrichment

**Story.** As on-call I want a plain-English root cause and suggested fix, not a
stack trace — but the alert must still arrive if the LLM is down.

**Acceptance.** Converts raw failures to root cause + affected tables +
suggested fix + severity recommendation as strict JSON, with retries/backoff and
a **raw-alert fallback**; drafts postmortems on resolve. **MVP (fallback
mandatory).**

## 4.8 Alert correlation / storm suppression

**Story.** When a shared dependency dies I want one incident, not fifty.

**Acceptance.** Clusters simultaneous related failures (same source, similar
error text, tight window) into one incident. **MVP.**

## 4.9 Data-Diff UI

**Story.** As a stakeholder I want visible proof each sync is correct.

**Acceptance.** Per-sync "✓ N rows verified" badge; on-demand verify; mismatch
drill-down to PK ranges + sample rows (via engine `verify` bisection); diff
history per pipeline. **MVP** (badge + history); drill-down **MVP** where the
engine verify exists, else v2.

## 4.10 Audit log

**Story.** As an admin I want an append-only record of every change and who made
it. **Acceptance.** Middleware records config/rule/sync/backfill/ack/setting
changes with actor, timestamp, before/after diff; UI with filters + CSV export.
**MVP.**

## 4.11 Sync & cost analytics

**Story.** As a budget owner I want to know what each pipeline moves and roughly
costs. **Acceptance.** Rows/bytes/duration trends per pipeline and per month; a
configurable cost model ($/GB stored, $/compute-hour) → "~$X/mo"; monthly
rollup. **MVP.**

## 4.12 Column-level lineage

**Story.** As an analyst I want to see source column → destination column and
what a schema change breaks downstream. **Acceptance.** `lineage_edges` from the
engine's per-column mappings; React Flow graph; schema-change events highlight
impacted columns; clicking a column shows its quality status. **MVP** (impact
highlight v2).

## 4.13 Pipeline-as-code + versioning

**Story.** As a platform engineer I want pipelines as YAML with diffs and
rollback. **Acceptance.** Canonical YAML per pipeline; every change → a
`pipeline_config_versions` row; git-style diff between versions; one-click
rollback; export/apply via API + UI. **MVP.**

## 4.14 Environments & promotion

**Story.** As a release manager I promote a pipeline dev→staging→prod with
credential overrides. **Acceptance.** Environments own pipelines; "Promote"
clones a config version to another environment with credential mapping; the
promotion is itself audited and versioned. **MVP.**

## 4.15 Backfills

**Story.** As a data engineer I re-sync a bounded slice (PK range or time window)
without a full reload. **Acceptance.** UI form → engine `backfill`; idempotent
upsert; progress tracking; result feeds the diff badge; appears in audit +
analytics; never corrupts CDC position. **MVP** (engine backfill is a Phase-2.8
milestone).

## 4.16 UI/UX excellence

**Story.** As any user I want a product I *enjoy* using, not a Bootstrap admin
panel. **Acceptance.** A codified design system (tokens + component library)
before pages; <3-min first-run wizard with a demo-data path; dashboard health
scores + sparklines + freshness heatmap; Cmd/Ctrl-K palette; designed empty /
loading / error states; micro-interactions; a11y basics; responsive to
laptop-small. **MVP — headline requirement.**

---

## Deliberately excluded from Free (open-core)

SSO / RBAC are reserved for a future Pro tier. Everything above — including the
features competitors paywall (data-diff, escalation/on-call, audit logs, cost
analytics, lineage, quality monitors, config versioning, environments,
backfills) — ships Free.
