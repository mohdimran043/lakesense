import { useMutation, useQueryClient, type QueryClient } from "@tanstack/react-query";
import {
  api,
  type BackfillRequest,
  type CreateChannelRequest,
  type CreatePipelineRequest,
  type CreateRuleRequest,
} from "./api";

// Write-side hooks. Each invalidates the queries its mutation affects so the
// dashboard reflects the change without a manual refresh. This is the plumbing
// the write-path screens (create wizard now; actions/rules/channels next) share.

// refreshPipeline invalidates the list and one pipeline's detail views.
function refreshPipeline(qc: QueryClient, id: number) {
  void qc.invalidateQueries({ queryKey: ["pipelines"] });
  void qc.invalidateQueries({ queryKey: ["pipeline", id] });
}

export function useCreatePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreatePipelineRequest) => api.createPipeline(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}

// useRunPipeline triggers a run (async on the backend); the list/detail refresh
// so the new sync's metrics and diff badges appear as they land.
export function useRunPipeline(id: number) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.runPipeline(id),
    onSuccess: () => refreshPipeline(qc, id),
  });
}

export function useSetPipelineStatus(id: number) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (next: "paused" | "active") => (next === "paused" ? api.pausePipeline(id) : api.resumePipeline(id)),
    onSuccess: () => refreshPipeline(qc, id),
  });
}

export function useArchivePipeline(id: number) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.archivePipeline(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}

export function useBackfill(id: number) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: BackfillRequest) => api.backfillPipeline(id, body),
    onSuccess: () => refreshPipeline(qc, id),
  });
}

// Channel + rule management (the alerting builder).
export function useCreateChannel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateChannelRequest) => api.createChannel(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["channels"] }),
  });
}

export function useDeleteChannel() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => api.deleteChannel(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["channels"] }),
  });
}

export function useCreateRule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateRuleRequest) => api.createRule(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["rules"] }),
  });
}

export function useDeleteRule() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => api.deleteRule(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["rules"] }),
  });
}

// Incident actions. Each is scoped to one incident id so per-row buttons carry
// their own pending state; success refreshes the incident list.
export function useIncidentActions(id: number) {
  const qc = useQueryClient();
  const refresh = () => qc.invalidateQueries({ queryKey: ["incidents"] });
  const ack = useMutation({ mutationFn: () => api.ackIncident(id), onSuccess: refresh });
  const snooze = useMutation({ mutationFn: (until: string) => api.snoozeIncident(id, until), onSuccess: refresh });
  const resolve = useMutation({ mutationFn: () => api.resolveIncident(id), onSuccess: refresh });
  return { ack, snooze, resolve };
}
