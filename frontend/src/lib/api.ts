// Typed client for the LakeSense control-plane API. Mirrors the JSON the Go
// handlers return (backend/internal/api/handlers.go). Same-origin /api/v1.

const base = "/api/v1";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(base + path, { headers: { accept: "application/json" } });
  return handle<T>(res);
}

// send performs a mutating request. Actor identity rides the X-Actor header (no
// auth yet — the backend records it in the audit log); SSO/RBAC is the future
// Pro tier.
async function send<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(base + path, {
    method,
    headers: { "content-type": "application/json", "x-actor": "web-ui", accept: "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return handle<T>(res);
}

// handle resolves a response, surfacing the backend's human error copy. A 204 /
// empty body resolves to undefined (callers that expect void ignore it).
async function handle<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let msg = `Request failed (${res.status})`;
    try {
      const errBody = await res.json();
      if (errBody?.error) msg = errBody.error;
    } catch {
      /* keep the status message */
    }
    throw new Error(msg);
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

export const post = <T>(path: string, body?: unknown) => send<T>("POST", path, body);
export const patch = <T>(path: string, body?: unknown) => send<T>("PATCH", path, body);
export const del = (path: string) => send<void>("DELETE", path);

export interface Pipeline {
  id: number;
  name: string;
  slug: string;
  environment: string;
  source_type: string;
  destination_type: string;
  status: string;
  schedule: string;
  last_sync_at: string | null;
  health_score: number;
  diff_verified: boolean;
  verified_rows: number;
  open_incidents: number;
}

export interface Metric {
  ts: string;
  rows_written: number;
  bytes_written: number;
  duration_seconds: number;
}

export interface DiffRun {
  stream: string;
  sync_id: string;
  source_rows: number;
  dest_rows: number;
  source_checksum: string;
  dest_checksum: string;
  match: boolean;
  created_at: string;
}

export interface LineageEdge {
  source_stream: string;
  source_column: string;
  source_type: string;
  dest_table: string;
  dest_column: string;
  dest_type: string;
}

export interface Incident {
  id: number;
  pipeline: string;
  title: string;
  severity: "info" | "warning" | "critical";
  status: "open" | "acked" | "snoozed" | "resolved";
  event_count: number;
  summary: string;
  opened_at: string;
  acked_at: string | null;
  resolved_at: string | null;
}

export interface Analytics {
  pipelines: { pipeline: string; rows: number; bytes: number; seconds: number; est_cost_usd: number }[];
  total_est_cost_usd: number;
  cost_per_gb: number;
  cost_per_hour: number;
}

export interface AuditEntry {
  actor: string;
  action: string;
  entity_type: string;
  entity_id: string;
  created_at: string;
}

export interface Channel {
  id: number;
  name: string;
  type: "slack" | "telegram" | "email" | "webhook";
  enabled: boolean;
}

export interface Rule {
  id: number;
  pipeline_id: number | null;
  pipeline: string;
  name: string;
  condition: { event?: string; field?: string; op?: string; value?: unknown };
  severity: "info" | "warning" | "critical";
  enabled: boolean;
  channel_ids: number[];
}

export interface CreateChannelRequest {
  name: string;
  type: string;
  config: Record<string, string>;
}

export interface CreateRuleRequest {
  name: string;
  condition: Record<string, unknown>;
  severity: string;
  channel_ids: number[];
  pipeline_id?: number;
}

// --- write-side request/response shapes (mirror backend/internal/pipelines) ---

export interface EndpointInput {
  type: string;
  settings: Record<string, string>;
}

export interface StreamInput {
  name: string;
  mode: string;
  cursor_field?: string;
}

export interface CreatePipelineRequest {
  name: string;
  environment: string;
  source: EndpointInput;
  destination: EndpointInput;
  schedule: string;
  streams: StreamInput[];
}

export interface CreatedPipeline {
  id: number;
  name: string;
  slug: string;
  environment: string;
  source_type: string;
  destination_type: string;
  status: string;
  schedule: string;
  current_version: number;
}

export interface BackfillRequest {
  stream: string;
  pk_min?: string;
  pk_max?: string;
  since_field?: string;
  since_value?: string;
}

export interface PromoteRequest {
  target_env: string;
  source_overrides?: Record<string, string>;
  destination_overrides?: Record<string, string>;
}

export const api = {
  pipelines: () => get<Pipeline[]>("/pipelines"),
  createPipeline: (req: CreatePipelineRequest) => post<CreatedPipeline>("/pipelines", req),
  runPipeline: (id: number) => post<{ status: string; pipeline_id: number }>(`/pipelines/${id}/run`),
  pausePipeline: (id: number) => post<{ status: string }>(`/pipelines/${id}/pause`),
  resumePipeline: (id: number) => post<{ status: string }>(`/pipelines/${id}/resume`),
  archivePipeline: (id: number) => del(`/pipelines/${id}`),
  backfillPipeline: (id: number, body: BackfillRequest) =>
    post<{ status: string; job_id: number }>(`/pipelines/${id}/backfill`, body),
  promotePipeline: (id: number, body: PromoteRequest) => post<CreatedPipeline>(`/pipelines/${id}/promote`, body),
  ackIncident: (id: number) => post<{ status: string }>(`/incidents/${id}/ack`),
  snoozeIncident: (id: number, until: string) => post<{ status: string }>(`/incidents/${id}/snooze`, { until }),
  resolveIncident: (id: number) => post<{ status: string }>(`/incidents/${id}/resolve`),
  channels: () => get<Channel[]>("/channels"),
  rules: () => get<Rule[]>("/rules"),
  createChannel: (req: CreateChannelRequest) => post<{ id: number }>("/channels", req),
  deleteChannel: (id: number) => del(`/channels/${id}`),
  createRule: (req: CreateRuleRequest) => post<{ id: number }>("/rules", req),
  deleteRule: (id: number) => del(`/rules/${id}`),
  pipeline: (id: number) => get<Pipeline>(`/pipelines/${id}`),
  metrics: (id: number) => get<Metric[]>(`/pipelines/${id}/metrics`),
  diffs: (id: number) => get<DiffRun[]>(`/pipelines/${id}/diffs`),
  lineage: (id: number) => get<LineageEdge[]>(`/pipelines/${id}/lineage`),
  incidents: () => get<Incident[]>("/incidents"),
  analytics: () => get<Analytics>("/analytics"),
  audit: () => get<AuditEntry[]>("/audit"),
};
