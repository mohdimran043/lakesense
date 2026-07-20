import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useAnalytics } from "../lib/hooks";
import { Card, EmptyState, Skeleton, Stat } from "../components/ui";
import { bytes, compactNum, usd } from "../lib/format";

export function Analytics() {
  const { data, isLoading, error } = useAnalytics();

  return (
    <div className="space-y-5">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Analytics & cost</h1>
        <p className="mt-1 text-sm text-muted">
          No opaque credits. A transparent model over what you actually moved and stored.
        </p>
      </div>

      {isLoading ? (
        <Skeleton className="h-72" />
      ) : error || !data ? (
        <EmptyState title="Can't reach the control plane" hint="Start the backend and seed demo data." />
      ) : (
        <>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3">
            <Card className="p-4">
              <Stat label="Est. monthly cost" value={usd(data.total_est_cost_usd)} />
            </Card>
            <Card className="p-4">
              <Stat label="Storage rate" value={usd(data.cost_per_gb)} sub="per GB" />
            </Card>
            <Card className="p-4">
              <Stat label="Compute rate" value={usd(data.cost_per_hour)} sub="per compute-hour" />
            </Card>
          </div>

          <Card className="p-4">
            <div className="mb-3 text-[11px] uppercase tracking-wide text-faint">Estimated cost by pipeline</div>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={data.pipelines} margin={{ top: 8, right: 8, left: -8, bottom: 0 }}>
                <CartesianGrid stroke="rgb(var(--line))" strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="pipeline" tick={{ fill: "rgb(var(--faint))", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "rgb(var(--line))" }} />
                <YAxis tickFormatter={(v) => usd(v)} tick={{ fill: "rgb(var(--faint))", fontSize: 11 }} tickLine={false} axisLine={false} width={52} />
                <Tooltip
                  cursor={{ fill: "rgb(var(--line) / 0.4)" }}
                  contentStyle={{ background: "rgb(var(--raised))", border: "1px solid rgb(var(--line))", borderRadius: 8, fontSize: 12 }}
                  formatter={(v) => [usd(Number(v)), "est. cost"]}
                />
                <Bar dataKey="est_cost_usd" radius={[4, 4, 0, 0]}>
                  {data.pipelines.map((_, i) => (
                    <Cell key={i} fill="rgb(var(--signal))" fillOpacity={0.7} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </Card>

          <Card className="overflow-hidden">
            <div className="grid grid-cols-[1fr,auto,auto,auto] gap-4 border-b border-line px-4 py-2.5 text-[11px] uppercase tracking-wide text-faint">
              <div>Pipeline</div>
              <div className="w-24 text-right">Rows</div>
              <div className="w-28 text-right">Data moved</div>
              <div className="w-24 text-right">Est. cost</div>
            </div>
            {data.pipelines.map((p) => (
              <div key={p.pipeline} className="grid grid-cols-[1fr,auto,auto,auto] items-center gap-4 border-b border-line px-4 py-2.5 last:border-0">
                <div className="truncate text-sm text-text">{p.pipeline}</div>
                <div className="tnum w-24 text-right text-sm text-muted">{compactNum(p.rows)}</div>
                <div className="tnum w-28 text-right text-sm text-muted">{bytes(p.bytes)}</div>
                <div className="tnum w-24 text-right text-sm text-signal">{usd(p.est_cost_usd)}</div>
              </div>
            ))}
          </Card>
        </>
      )}
    </div>
  );
}
