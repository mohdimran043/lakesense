import { ShieldCheck } from "lucide-react";
import { useIncidents } from "../lib/hooks";
import { useIncidentActions } from "../lib/mutations";
import { Button, Card, EmptyState, Skeleton } from "../components/ui";
import { SeverityPill, StatusDot } from "../components/signals";
import { relTime } from "../lib/format";
import type { Incident } from "../lib/api";

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
                <IncidentActions incident={i} />
              </div>
            ))}
          </Card>
        </>
      )}
    </div>
  );
}

// IncidentActions renders ack / snooze / resolve for a non-resolved incident.
// Each incident owns its mutation state, so a pending action only disables its
// own row.
function IncidentActions({ incident }: { incident: Incident }) {
  const { ack, snooze, resolve } = useIncidentActions(incident.id);
  if (incident.status === "resolved") return null;
  const busy = ack.isPending || snooze.isPending || resolve.isPending;
  const snooze1h = () => snooze.mutate(new Date(Date.now() + 3600_000).toISOString());

  return (
    <div className="flex items-center gap-1">
      {incident.status !== "acked" && (
        <Button variant="ghost" size="sm" disabled={busy} onClick={() => ack.mutate()}>
          Ack
        </Button>
      )}
      {incident.status !== "snoozed" && (
        <Button variant="ghost" size="sm" disabled={busy} onClick={snooze1h}>
          Snooze 1h
        </Button>
      )}
      <Button variant="ghost" size="sm" className="text-signal hover:text-signal" disabled={busy} onClick={() => resolve.mutate()}>
        Resolve
      </Button>
    </div>
  );
}
