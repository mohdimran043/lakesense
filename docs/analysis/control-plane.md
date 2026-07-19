# Reference Analysis — Control Plane (olake-ui)

> Clean-room bridge document. Concepts in our own words; LakeSense implements from this doc only.

## 1. Data model (conceptual)

Postgres-backed entities: **users** (flat, no roles), **sources** and **destinations** (name +
connector type + version + opaque JSON config validated against connector-emitted JSON Schema),
**jobs** (the pipeline: one source + one destination + selected-streams catalog blob + state blob
+ schedule frequency + active flag), per-project **settings** (a single webhook alert URL), a
connector **spec cache**, and server-side **sessions**. Soft deletes + created/updated-by columns
everywhere. "Project" is a hardcoded string threaded through routes — multitenancy scaffolding
with no table behind it. **Runs are not persisted at all**: execution history lives solely in the
orchestrator's own store and disappears with its retention window.

## 2. Orchestration

Execution runs on **Temporal**: server + separate worker container (both mounting the Docker
socket), the worker launching each connector run as a Docker container chosen by connector
type+version. Scheduling = Temporal cron schedules per job (overlap policy SKIP ⇒ implicit
one-run-per-job concurrency). "Run now" triggers the schedule; activate/deactivate pauses it;
cancel = workflow cancellation. Run history is queried from Temporal by workflow-ID prefix; logs
are per-run files on a shared volume, read with byte-offset pagination and downloadable as
archives. The engine contract is file-based (configs written to a shared volume, output JSON
files read back); state updates and sync telemetry return through **unauthenticated internal
callback endpoints**. The "clear destination" flow temporarily rewrites the schedule's action and
restores it after — fragile enough to need a dedicated recovery endpoint.

## 3. API shape (conceptual)

Session-cookie auth; uniform `{success, message, data}` envelope; per-project route groups for
sources/destinations (CRUD + test + discover-streams + versions + spec), jobs (CRUD + sync +
activate + cancel + runs + paginated logs + clear-destination + catalog-diff), users, settings.
Hygiene issues worth not repeating: GETs with side effects and request bodies, no server-side
pagination/filtering on lists.

## 4. Frontend

React + TS + Vite, AntD + Tailwind, TanStack Query (server state) + Zustand (client state),
feature-first layout. Screens: login, jobs list, multi-step job wizard (source → destination →
streams → schedule), job history + log tailing viewer, spec-driven source/destination forms,
settings. Worth adopting: JSON-Schema-driven config forms with version selection; the
"stream diff" modal warning which streams a config edit will clear; cursor-paginated log tailing;
per-feature query/mutation hook separation. Weaknesses: no dashboard/overview, polling-only
status, job wizard inlining source/destination creation awkwardly.

## 5. Gaps (= LakeSense's product surface)

No RBAC/roles; no real multitenancy; alerting = one webhook URL (no rules, channels, history,
escalation); **no runs table → no metrics, throughput, or cost data**; no lineage, quality
monitoring, anomaly detection, audit trail, config versioning, environments, verification/diff,
or backfill UI. Internal endpoints trust the network. Nearly the entire LakeSense intelligence
layer is absent — this is the wedge.

## 6. LakeSense architectural decisions (contrasts)

- **No Temporal, no Docker-socket orchestration.** LakeSense's backend launches `lsengine` as a
  child process per run (goroutine-supervised, context-cancelled, restart-with-backoff). One
  binary + Postgres is the whole control plane — vastly simpler to self-host, and scheduling
  (cron per pipeline), overlap policy, and cancellation are ~200 lines of Go with a worker pool,
  fully unit-testable with a fake clock. Trade-off accepted: no distributed execution in v0.1
  (single-node runner; a remote-runner protocol is roadmap).
- **Events over log-scraping:** lsengine's JSONL stdout is ingested natively by the collector;
  runs, metrics, per-stream stats, schema changes, and checksums land in OUR `events`/`metrics`
  tables — run history is first-class and survives forever (fixes their biggest gap).
- **Runs persisted, paginated APIs, no side-effect GETs, internal calls authenticated** (shared
  token), configs encrypted at rest with a key from env.
- Adopt: spec-driven config forms, stream-diff warning UX, cursor log tailing, catalog diff
  concept, JSON-Schema spec caching per connector version.
