// Typed client for the LakeSense control-plane API. Mirrors the JSON the Go
// handlers return (backend/internal/api/handlers.go). Same-origin /api/v1.

const base = "/api/v1";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(base + path, { headers: { accept: "application/json" } });
  if (!res.ok) {
    let msg = `Request failed (${res.status})`;
    try {
      const body = await res.json();
      if (body?.error) msg = body.error;
    } catch {
      /* keep the status message */
    }
    throw new Error(msg);
  }
  return res.json() as Promise<T>;
}

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

export const api = {
  pipelines: () => get<Pipeline[]>("/pipelines"),
  pipeline: (id: number) => get<Pipeline>(`/pipelines/${id}`),
  metrics: (id: number) => get<Metric[]>(`/pipelines/${id}/metrics`),
  diffs: (id: number) => get<DiffRun[]>(`/pipelines/${id}/diffs`),
  lineage: (id: number) => get<LineageEdge[]>(`/pipelines/${id}/lineage`),
  incidents: () => get<Incident[]>("/incidents"),
  analytics: () => get<Analytics>("/analytics"),
  audit: () => get<AuditEntry[]>("/audit"),
};
