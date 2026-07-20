import { ShieldCheck } from "lucide-react";
import { useIncidents } from "../lib/hooks";
import { Card, EmptyState, Skeleton } from "../components/ui";
import { SeverityPill, StatusDot } from "../components/signals";
import { relTime } from "../lib/format";

export function Incidents() {
  const { data, isLoading, error } = useIncidents();
  const incidents = data ?? [];
  const open = incidents.filter((i) => i.status !== "resolved");

  return (
    <div className="space-y-5">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Incidents</h1>
        <p className="mt-1 text-sm text-muted">
          One incident is one alert thread — storms are correlated, repeats deduplicated.
        </p>
      </div>

      {isLoading ? (
        <Skeleton className="h-48" />
      ) : error ? (
        <EmptyState title="Can't reach the control plane" hint="Start the backend and seed demo data." />
      ) : incidents.length === 0 ? (
        <EmptyState
          icon={<ShieldCheck size={28} className="text-signal" />}
          title="No incidents"
          hint="When a rule fires or an anomaly is detected, incidents land here — enriched and correlated."
        />
      ) : (
        <>
          <div className="flex gap-4 text-xs text-muted">
            <span>
              <span className="tnum text-danger">{open.length}</span> open
            </span>
            <span>
              <span className="tnum text-text">{incidents.length}</span> total
            </span>
          </div>
          <Card className="divide-y divide-line">
            {incidents.map((i) => (
              <div key={i.id} className="flex items-start gap-3 px-4 py-3">
                <StatusDot status={i.status} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="truncate text-sm text-text">{i.title}</span>
                  </div>
                  <div className="text-xs text-faint">
                    {i.pipeline || "global"} · opened {relTime(i.opened_at)} · status {i.status}
                    {i.event_count > 1 && ` · ${i.event_count} events grouped`}
                  </div>
                </div>
                <SeverityPill severity={i.severity} />
              </div>
            ))}
          </Card>
        </>
      )}
    </div>
  );
}
