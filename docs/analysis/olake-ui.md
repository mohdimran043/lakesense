# Reference Analysis — OLake UI (`reference/olake-ui`)

*Analyzed 2026-07-19 from a shallow clone of `datazip-inc/olake-ui` (Apache/AGPL per repo LICENSE). Read-only reference; no code copied.*

## What it is

The self-hosted control plane for OLake: a Go (Beego-style) backend serving both REST API and the React frontend on **port 8000**, orchestrating sync runs through **Temporal**. A separate worker container (`olakego/ui-worker`) executes OLake CLI syncs inside Docker.

## docker-compose stack

`olake-ui` (:8000) · `olake-signup-init` (one-shot admin bootstrap) · `olake-temporal-worker` · `temporal-postgresql` (postgres:13) · `temporal` (auto-setup 1.22.3) · `temporal-ui` (:8081) · `temporal-elasticsearch`. Shared host directory for configs/logs.

## API surface (from `api-contract.md`)

- **Auth:** `POST /login` (session cookie), `POST /signup`, `GET /auth`.
- **Sources / Destinations:** CRUD under `/api/v1/project/:projectid/{sources,destinations}` + `/versions`, `/spec`, `/test` (connection check).
- **Jobs:** CRUD under `/api/v1/project/:projectid/jobs`. Job = `{id, name, source{name,type,version}, destination{...}, frequency (cron), streams_config, activate, last_run_time, last_run_state, last_run_type, created_by, updated_by}`.
- **Run control:** `POST /jobs/:id/sync` (trigger now), `GET /jobs/:jobid/cancel`, `POST /jobs/:id/activate`.
- **Task history:** `GET /jobs/:jobid/tasks` → `[{file_path, start_time, runtime, status, job_type}]` — **the per-run record LakeSense will poll.**
- **Logs:** `POST /jobs/:jobid/tasks/:taskid/logs` (cursor-paginated, needs `file_path` from tasks) and `GET /jobs/:id/logs/download` (tar.gz of `olake.log` + `state.json`).
- **Settings:** `GET/PUT /project/:projectid/settings`.
- Project id is literally `"olake"` for now (single-tenant).

## Execution model

Server → Temporal workflow → worker container → writes job config files into `/tmp/olake-config/<sha256(workflowID)>/` (async commands hash the workflow id; see `services/temporal/filesystem.go`) → runs the OLake CLI docker image → logs land under that directory (`logs/.../olake.log`), state in `state.json`. Task `file_path` returned by the API points into this tree.

## Alerting today (the gap LakeSense fills)

The **entire** notification system is one field: `webhook_alert_url` (512-char column in project settings, example value is a Slack webhook). One URL per project, fired by the worker on job failure. Confirmed by grep across the server: no channels table, no rules, no severity, no dedup, no digest, nothing per-job.

## Integration points ranked for LakeSense

1. **UI REST API polling** — `/jobs` + `/jobs/:jobid/tasks` give run states, durations, timestamps; `/tasks/:taskid/logs` gives error text for LLM enrichment. Auth via `/login` cookie. Stable, versioned-ish, works over the network with zero deployment coupling.
2. **Shared volume reads** — `/tmp/olake-config/...` gives `stats.json` (2 s metrics) and `state.json`. Requires co-mounting the OLake config volume; richer but deployment-coupled.
3. **Webhook receiver** — point OLake's own `webhook_alert_url` at LakeSense as a bonus signal for instant failure pings (failures only, no payload richness).

## Constraints

- No event push, no websocket; polling cadence bounds our detection latency (API is cheap; 10–30 s polling is fine).
- `status` values on tasks observed in contract: string (running/completed/failed vocabulary — collector must normalize defensively).
- Log fetch requires the `file_path` from the task record — collector must persist it per run.
