import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useDiffs, useLineage, useMetrics, usePipeline } from "../lib/hooks";
import { Badge, Card, EmptyState, Skeleton, Stat } from "../components/ui";
import { HealthMeter, HealthScore, VerifiedBadge } from "../components/signals";
import { bytes, compactNum, duration, fullNum, relTime } from "../lib/format";

const tabs = ["Overview", "Diff", "Lineage"] as const;
type Tab = (typeof tabs)[number];

export function PipelineDetail() {
  const id = Number(useParams().id);
  const { data: p, isLoading, error } = usePipeline(id);
  const [tab, setTab] = useState<Tab>("Overview");

  if (isLoading) return <Skeleton className="h-96" />;
  if (error || !p)
    return <EmptyState title="Pipeline not found" hint="It may have been removed, or the backend is unreachable." />;

  return (
    <div className="space-y-6">
      <div>
        <Link to="/pipelines" className="mb-3 inline-flex items-center gap-1 text-xs text-muted hover:text-text">
          <ArrowLeft size={13} /> Pipelines
        </Link>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="font-display text-2xl font-semibold tracking-tight text-text">{p.name}</h1>
            <div className="tnum mt-1 text-sm text-muted">
              {p.source_type} → {p.destination_type} · {p.environment} · {p.schedule || "manual"}
            </div>
          </div>
          <div className="flex items-center gap-3">
            <VerifiedBadge verified={p.diff_verified} rows={p.verified_rows} />
            <HealthScore score={p.health_score} />
          </div>
        </div>
        <div className="mt-3 max-w-md">
          <HealthMeter score={p.health_score} />
        </div>
      </div>

      <div className="flex gap-1 border-b border-line">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={
              "relative px-4 py-2 text-sm transition-colors " +
              (tab === t ? "text-signal" : "text-muted hover:text-text")
            }
          >
            {t}
            {tab === t && <span className="absolute inset-x-2 -bottom-px h-0.5 rounded-full bg-signal" />}
          </button>
        ))}
      </div>

      {tab === "Overview" && <OverviewTab id={id} lastSync={p.last_sync_at} />}
      {tab === "Diff" && <DiffTab id={id} />}
      {tab === "Lineage" && <LineageTab id={id} />}
    </div>
  );
}

function OverviewTab({ id, lastSync }: { id: number; lastSync: string | null }) {
  const { data, isLoading } = useMetrics(id);
  if (isLoading) return <Skeleton className="h-64" />;
  const metrics = data ?? [];
  const totalRows = metrics.reduce((a, m) => a + m.rows_written, 0);
  const totalBytes = metrics.reduce((a, m) => a + m.bytes_written, 0);
  const avgDur = metrics.length ? metrics.reduce((a, m) => a + m.duration_seconds, 0) / metrics.length : 0;
  const chart = metrics.map((m) => ({
    t: new Date(m.ts).toLocaleDateString("en-US", { month: "short", day: "numeric" }),
    rows: m.rows_written,
  }));

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <Card className="p-4">
          <Stat label="Rows / run" value={compactNum(Math.round(totalRows / Math.max(metrics.length, 1)))} />
        </Card>
        <Card className="p-4">
          <Stat label="Total rows" value={compactNum(totalRows)} sub={`${metrics.length} runs`} />
        </Card>
        <Card className="p-4">
          <Stat label="Data moved" value={bytes(totalBytes)} />
        </Card>
        <Card className="p-4">
          <Stat label="Avg duration" value={duration(avgDur)} sub={`synced ${relTime(lastSync)}`} />
        </Card>
      </div>

      <Card className="p-4">
        <div className="mb-3 text-[11px] uppercase tracking-wide text-faint">Rows written per run</div>
        {chart.length === 0 ? (
          <EmptyState title="No runs yet" />
        ) : (
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={chart} margin={{ top: 8, right: 8, left: -8, bottom: 0 }}>
              <CartesianGrid stroke="rgb(var(--line))" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="t" tick={{ fill: "rgb(var(--faint))", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "rgb(var(--line))" }} />
              <YAxis tickFormatter={compactNum} tick={{ fill: "rgb(var(--faint))", fontSize: 11 }} tickLine={false} axisLine={false} width={44} />
              <Tooltip
                contentStyle={{ background: "rgb(var(--raised))", border: "1px solid rgb(var(--line))", borderRadius: 8, fontSize: 12 }}
                labelStyle={{ color: "rgb(var(--muted))" }}
                formatter={(v) => [fullNum(Number(v)), "rows"]}
              />
              <Line type="monotone" dataKey="rows" stroke="rgb(var(--signal))" strokeWidth={2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        )}
      </Card>
    </div>
  );
}

function DiffTab({ id }: { id: number }) {
  const { data, isLoading } = useDiffs(id);
  if (isLoading) return <Skeleton className="h-64" />;
  const diffs = data ?? [];
  if (diffs.length === 0) return <EmptyState title="No verification runs yet" />;

  return (
    <Card className="overflow-hidden">
      <div className="grid grid-cols-[1fr,auto,auto,auto] gap-4 border-b border-line px-4 py-2.5 text-[11px] uppercase tracking-wide text-faint">
        <div>Stream · sync</div>
        <div className="w-24 text-right">Source</div>
        <div className="w-24 text-right">Dest</div>
        <div className="w-36 text-right">Result</div>
      </div>
      {diffs.map((d, i) => (
        <div key={i} className="grid grid-cols-[1fr,auto,auto,auto] items-center gap-4 border-b border-line px-4 py-2.5 last:border-0">
          <div className="min-w-0">
            <div className="truncate text-sm text-text">{d.stream}</div>
            <div className="tnum text-xs text-faint">{d.sync_id}</div>
          </div>
          <div className="tnum w-24 text-right text-sm text-muted">{compactNum(d.source_rows)}</div>
          <div className="tnum w-24 text-right text-sm text-muted">{compactNum(d.dest_rows)}</div>
          <div className="flex w-36 justify-end">
            {d.match ? (
              <Badge tone="signal">✓ verified</Badge>
            ) : (
              <Badge tone="danger">✗ {fullNum(d.source_rows - d.dest_rows)} rows off</Badge>
            )}
          </div>
        </div>
      ))}
    </Card>
  );
}

function LineageTab({ id }: { id: number }) {
  const { data, isLoading } = useLineage(id);
  if (isLoading) return <Skeleton className="h-64" />;
  const edges = data ?? [];
  if (edges.length === 0) return <EmptyState title="No column lineage yet" hint="Lineage is recorded from the engine's per-column mappings on each sync." />;

  return (
    <Card className="p-4">
      <div className="mb-3 text-[11px] uppercase tracking-wide text-faint">Source column → destination column</div>
      <div className="space-y-1.5">
        {edges.map((e, i) => (
          <div key={i} className="grid grid-cols-[1fr,auto,1fr] items-center gap-3 rounded-control px-3 py-2 hover:bg-line/25">
            <div className="tnum flex items-center gap-2 text-sm">
              <span className="text-faint">{e.source_stream}.</span>
              <span className="text-text">{e.source_column}</span>
              <Badge>{e.source_type}</Badge>
            </div>
            <ArrowRight size={14} className="text-signal" />
            <div className="tnum flex items-center gap-2 text-sm">
              <span className="text-faint">{e.dest_table}.</span>
              <span className="text-text">{e.dest_column}</span>
              <Badge>{e.dest_type}</Badge>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}
