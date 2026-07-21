import { useQuery } from "@tanstack/react-query";
import { api } from "./api";
import { getCostModel } from "./settings";

// Thin query hooks. The API is read-only demo data today, so a 30s stale time
// keeps the live-feel without hammering the backend.
const opts = { staleTime: 30_000 };

export const usePipelines = () => useQuery({ queryKey: ["pipelines"], queryFn: api.pipelines, ...opts });
export const usePipeline = (id: number) =>
  useQuery({ queryKey: ["pipeline", id], queryFn: () => api.pipeline(id), ...opts });
export const useMetrics = (id: number) =>
  useQuery({ queryKey: ["metrics", id], queryFn: () => api.metrics(id), ...opts });
export const useDiffs = (id: number) =>
  useQuery({ queryKey: ["diffs", id], queryFn: () => api.diffs(id), ...opts });
export const useLineage = (id: number) =>
  useQuery({ queryKey: ["lineage", id], queryFn: () => api.lineage(id), ...opts });
export const useIncidents = () => useQuery({ queryKey: ["incidents"], queryFn: api.incidents, ...opts });
export const useAnalytics = () => {
  const cost = getCostModel();
  return useQuery({
    queryKey: ["analytics", cost.costPerGB, cost.costPerHour],
    queryFn: () => api.analytics(cost),
    ...opts,
  });
};
export const useAudit = () => useQuery({ queryKey: ["audit"], queryFn: api.audit, ...opts });
export const useChannels = () => useQuery({ queryKey: ["channels"], queryFn: api.channels, ...opts });
export const useRules = () => useQuery({ queryKey: ["rules"], queryFn: api.rules, ...opts });
export const useEscalationPolicies = () =>
  useQuery({ queryKey: ["escalation-policies"], queryFn: api.escalationPolicies, ...opts });
export const useOncallSchedules = () =>
  useQuery({ queryKey: ["oncall-schedules"], queryFn: api.oncallSchedules, ...opts });
export const useBackfills = (id: number) =>
  useQuery({ queryKey: ["backfills", id], queryFn: () => api.backfills(id), ...opts });
