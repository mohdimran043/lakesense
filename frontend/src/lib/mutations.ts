import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api, type CreatePipelineRequest } from "./api";

// Write-side hooks. Each invalidates the queries its mutation affects so the
// dashboard reflects the change without a manual refresh. This is the plumbing
// the write-path screens (create wizard now; actions/rules/channels next) share.

export function useCreatePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreatePipelineRequest) => api.createPipeline(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}
