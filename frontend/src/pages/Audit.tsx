import { ScrollText } from "lucide-react";
import { useAudit } from "../lib/hooks";
import { Card, EmptyState, Skeleton } from "../components/ui";
import { relTime } from "../lib/format";

export function Audit() {
  const { data, isLoading, error } = useAudit();
  const entries = data ?? [];

  return (
    <div className="space-y-5">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Audit log</h1>
        <p className="mt-1 text-sm text-muted">
          Append-only record of every config change, rule edit, ack, and manual run.
        </p>
      </div>

      {isLoading ? (
        <Skeleton className="h-40" />
      ) : error ? (
        <EmptyState title="Can't reach the control plane" hint="Start the backend and seed demo data." />
      ) : entries.length === 0 ? (
        <EmptyState
          icon={<ScrollText size={28} className="text-faint" />}
          title="No audited actions yet"
          hint="Config changes, rule edits, acks, and manual runs are recorded here with actor, timestamp, and before/after."
        />
      ) : (
        <Card className="divide-y divide-line">
          {entries.map((e, i) => (
            <div key={i} className="flex items-center gap-3 px-4 py-2.5 text-sm">
              <span className="tnum text-xs text-faint">{relTime(e.created_at)}</span>
              <span className="text-signal">{e.actor}</span>
              <span className="text-muted">{e.action}</span>
              <span className="text-text">
                {e.entity_type}
                {e.entity_id && ` #${e.entity_id}`}
              </span>
            </div>
          ))}
        </Card>
      )}
    </div>
  );
}
