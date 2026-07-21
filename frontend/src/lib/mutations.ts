import { useMutation, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { api, type BackfillRequest, type CreatePipelineRequest } from "./api";

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
