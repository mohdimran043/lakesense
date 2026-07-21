import { Link } from "react-router-dom";
import { usePipelines } from "../lib/hooks";
import { Button, Card, EmptyState, Skeleton } from "../components/ui";
import { HealthMeter, HealthScore, VerifiedBadge } from "../components/signals";
import { relTime } from "../lib/format";

export function Pipelines() {
  const { data, isLoading, error } = usePipelines();

  return (
    <div className="space-y-5">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Pipelines</h1>
          <p className="mt-1 text-sm text-muted">Replication jobs and their live health sounding.</p>
        </div>
        <Link to="/pipelines/new">
          <Button variant="primary">+ New pipeline</Button>
        </Link>
      </div>

      {isLoading ? (
        <Skeleton className="h-64" />
      ) : error ? (
        <EmptyState title="Can't reach the control plane" hint="Start the backend and seed demo data." />
      ) : !data || data.length === 0 ? (
        <EmptyState
          title="No pipelines yet"
          hint="Create your first pipeline, or run `lakesense seed` to explore with demo data."
          action={
            <Link to="/pipelines/new">
              <Button variant="primary">+ New pipeline</Button>
            </Link>
          }
        />
      ) : (
        <Card className="overflow-hidden">
          <div className="grid grid-cols-[1fr,auto,auto,auto] items-center gap-4 border-b border-line px-4 py-2.5 text-[11px] uppercase tracking-wide text-faint">
            <div>Pipeline</div>
            <div className="w-40">Health</div>
            <div className="w-44">Correctness</div>
            <div className="w-24 text-right">Synced</div>
          </div>
          {data.map((p) => (
            <Link
              key={p.id}
              to={`/pipelines/${p.id}`}
              className="grid grid-cols-[1fr,auto,auto,auto] items-center gap-4 border-b border-line px-4 py-3 transition-colors last:border-0 hover:bg-line/25"
            >
              <div className="min-w-0">
                <div className="truncate text-sm text-text">{p.name}</div>
                <div className="tnum text-xs text-faint">
                  {p.source_type} → {p.destination_type} · {p.environment}
                </div>
              </div>
              <div className="flex w-40 items-center gap-2">
                <HealthMeter score={p.health_score} className="w-20" />
                <HealthScore score={p.health_score} />
              </div>
              <div className="w-44">
                <VerifiedBadge verified={p.diff_verified} rows={p.verified_rows} />
              </div>
              <div className="tnum w-24 text-right text-xs text-muted">{relTime(p.last_sync_at)}</div>
            </Link>
          ))}
        </Card>
      )}
    </div>
  );
}
