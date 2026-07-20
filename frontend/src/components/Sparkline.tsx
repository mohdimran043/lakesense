import { Area, AreaChart, ResponsiveContainer, YAxis } from "recharts";

// A tiny trend line for a stat card — no axes, no chrome, just the shape of the
// data. Uses the signal color with a faint fill.
export function Sparkline({ data, height = 36 }: { data: number[]; height?: number }) {
  if (data.length === 0) return <div style={{ height }} />;
  const series = data.map((v, i) => ({ i, v }));
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={series} margin={{ top: 2, bottom: 2, left: 0, right: 0 }}>
        <defs>
          <linearGradient id="spark" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="rgb(var(--signal))" stopOpacity={0.35} />
            <stop offset="100%" stopColor="rgb(var(--signal))" stopOpacity={0} />
          </linearGradient>
        </defs>
        <YAxis hide domain={["dataMin", "dataMax"]} />
        <Area
          type="monotone"
          dataKey="v"
          stroke="rgb(var(--signal))"
          strokeWidth={1.5}
          fill="url(#spark)"
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
