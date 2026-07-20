import { Link } from "react-router-dom";
import { ArrowUpRight, DollarSign, GitBranch, ShieldCheck, TriangleAlert } from "lucide-react";
import { useAnalytics, useIncidents, useMetrics, usePipelines } from "../lib/hooks";
import type { Pipeline } from "../lib/api";
import { Card, EmptyState, SectionTitle, Skeleton, Stat } from "../components/ui";
import { HealthMeter, HealthScore, SeverityPill, StatusDot, VerifiedBadge } from "../components/signals";
import { Sparkline } from "../components/Sparkline";
import { relTime, usd } from "../lib/format";

export function Dashboard() {
  const pipelines = usePipelines();
  const incidents = useIncidents();
  const analytics = useAnalytics();

  const list = pipelines.data ?? [];
  const avgHealth = list.length ? Math.round(list.reduce((a, p) => a + p.health_score, 0) / list.length) : 0;
  const verified = list.filter((p) => p.diff_verified).length;
  const openIncidents = (incidents.data ?? []).filter((i) => i.status !== "resolved");

  return (
    <div className="space-y-7">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Fleet overview</h1>
        <p className="mt-1 text-sm text-muted">
          Every sync ships with proof it's correct. Here's the sounding across your lakehouse.
        </p>
      </div>

      {/* Instrument row */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <Card className="p-4">
          <div className="flex items-start justify-between">
            <Stat label="Pipelines" value={pipelines.isLoading ? "—" : list.length} />
            <GitBranch size={16} className="text-faint" />
          </div>
        </Card>
        <Card className="p-4">
          <div className="flex items-start justify-between">
            <Stat label="Avg health" value={pipelines.isLoading ? "—" : avgHealth} sub="composite score" />
            <span className="mt-1">
              <HealthMeter score={avgHealth} className="w-14" />
            </span>
          </div>
        </Card>
        <Card className="p-4">
          <div className="flex items-start justify-between">
            <Stat
              label="Verified"
              value={pipelines.isLoading ? "—" : `${verified}/${list.length}`}
              sub="latest sync matched"
            />
            <ShieldCheck size={16} className="text-signal" />
          </div>
        </Card>
        <Card className="p-4">
          <div className="flex items-start justify-between">
            <Stat
              label="Est. cost"
              value={analytics.isLoading ? "—" : usd(analytics.data?.total_est_cost_usd ?? 0)}
              sub="transparent model"
            />
            <DollarSign size={16} className="text-faint" />
          </div>
        </Card>
      </div>

      {/* Pipeline health cards */}
      <div>
        <SectionTitle right={<Link to="/pipelines" className="text-xs text-signal hover:underline">All pipelines →</Link>}>
          Pipeline health
        </SectionTitle>
        {pipelines.isLoading ? (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {[0, 1, 2].map((i) => (
              <Skeleton key={i} className="h-40" />
            ))}
          </div>
        ) : pipelines.error ? (
          <EmptyState
            title="Can't reach the control plane"
            hint="Is the backend running? Try `docker compose up` and `lakesense seed`."
          />
        ) : list.length === 0 ? (
          <EmptyState title="No pipelines yet" hint="Run `lakesense seed` to load demo data, or connect a source." />
        ) : (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {list.map((p) => (
              <PipelineCard key={p.id} p={p} />
            ))}
          </div>
        )}
      </div>

      {/* Incident feed */}
      <div>
        <SectionTitle right={<Link to="/incidents" className="text-xs text-signal hover:underline">All incidents →</Link>}>
          Open incidents
        </SectionTitle>
        <Card className="divide-y divide-line">
          {incidents.isLoading ? (
            <div className="p-4">
              <Skeleton className="h-10" />
            </div>
          ) : openIncidents.length === 0 ? (
            <div className="flex items-center gap-2 p-5 text-sm text-muted">
              <ShieldCheck size={16} className="text-signal" /> All clear — no open incidents.
            </div>
          ) : (
            openIncidents.slice(0, 6).map((i) => (
              <div key={i.id} className="flex items-center gap-3 px-4 py-3">
                <StatusDot status={i.status} />
                <TriangleAlert size={15} className="text-warn" />
                <div className="min-w-0 flex-1">
                  <div className="truncate text-sm text-text">{i.title}</div>
                  <div className="text-xs text-faint">
                    {i.pipeline || "global"} · {relTime(i.opened_at)}
                    {i.event_count > 1 && ` · ${i.event_count} events`}
                  </div>
                </div>
                <SeverityPill severity={i.severity} />
              </div>
            ))
          )}
        </Card>
      </div>
    </div>
  );
}

function PipelineCard({ p }: { p: Pipeline }) {
  const metrics = useMetrics(p.id);
  const spark = (metrics.data ?? []).map((m) => m.rows_written);
  return (
    <Link to={`/pipelines/${p.id}`} className="group block">
      <Card className="p-4 transition-colors group-hover:border-signal/40">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <div className="truncate font-display text-sm font-medium text-text">{p.name}</div>
            <div className="tnum mt-0.5 text-xs text-faint">
              {p.source_type} → {p.destination_type}
            </div>
          </div>
          <HealthScore score={p.health_score} />
        </div>

        <div className="mt-3">
          <HealthMeter score={p.health_score} />
        </div>

        <div className="-mx-1 mt-3 h-9">
          <Sparkline data={spark} />
        </div>

        <div className="mt-3 flex items-center justify-between">
          <VerifiedBadge verified={p.diff_verified} rows={p.verified_rows} />
          <span className="flex items-center gap-1 text-xs text-faint opacity-0 transition-opacity group-hover:opacity-100">
            open <ArrowUpRight size={12} />
          </span>
        </div>
        <div className="mt-2 flex items-center gap-3 text-[11px] text-faint">
          <span>{p.environment}</span>
          <span>·</span>
          <span>synced {relTime(p.last_sync_at)}</span>
          {p.open_incidents > 0 && (
            <>
              <span>·</span>
              <span className="text-danger">
                {p.open_incidents} incident{p.open_incidents > 1 ? "s" : ""}
              </span>
            </>
          )}
        </div>
      </Card>
    </Link>
  );
}
