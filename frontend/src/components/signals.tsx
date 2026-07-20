import { Check, ShieldAlert } from "lucide-react";
import { cn } from "./ui";
import { compactNum } from "../lib/format";

// HealthMeter — the signature element: a sonar "depth sounding" bar. The fill is
// the composite health score; its hue shifts from signal (healthy) through amber
// to coral as the score drops, and a live ping sits at the reading.
export function HealthMeter({ score, className }: { score: number; className?: string }) {
  const tone = score >= 85 ? "signal" : score >= 60 ? "warn" : "danger";
  const color = { signal: "var(--signal)", warn: "var(--warn)", danger: "var(--danger)" }[tone];
  return (
    <div className={cn("w-full", className)}>
      <div className="relative h-2 overflow-hidden rounded-full bg-line/60">
        <div
          className="h-full rounded-full transition-[width] duration-500"
          style={{ width: `${score}%`, background: `rgb(${color})`, boxShadow: `0 0 12px rgb(${color} / 0.6)` }}
        />
      </div>
    </div>
  );
}

export function HealthScore({ score }: { score: number }) {
  const tone = score >= 85 ? "text-signal" : score >= 60 ? "text-warn" : "text-danger";
  return (
    <span className={cn("tnum text-xl font-medium", tone)}>
      {score}
      <span className="ml-0.5 text-xs text-faint">/100</span>
    </span>
  );
}

// VerifiedBadge — the product's correctness signal, rendered as a sonar-confirmed
// pill. A matched sync reads as an instrument confirmation; a mismatch is loud.
export function VerifiedBadge({ verified, rows }: { verified: boolean; rows?: number }) {
  if (verified) {
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full border border-signal/40 bg-signal/10 px-2.5 py-1 text-[11px] font-medium text-signal">
        <span className="relative flex h-1.5 w-1.5">
          <span className="absolute inline-flex h-full w-full animate-ping-slow rounded-full bg-signal opacity-75" />
          <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-signal" />
        </span>
        <Check size={12} strokeWidth={3} />
        {rows != null ? `${compactNum(rows)} rows verified` : "Verified"}
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-danger/40 bg-danger/10 px-2.5 py-1 text-[11px] font-medium text-danger">
      <ShieldAlert size={12} strokeWidth={2.5} />
      Mismatch detected
    </span>
  );
}

export function SeverityPill({ severity }: { severity: string }) {
  const map: Record<string, { c: string; label: string }> = {
    critical: { c: "border-danger/40 text-danger bg-danger/10", label: "Critical" },
    warning: { c: "border-warn/40 text-warn bg-warn/10", label: "Warning" },
    info: { c: "border-info/40 text-info bg-info/10", label: "Info" },
  };
  const s = map[severity] ?? map.info;
  return <span className={cn("rounded-full border px-2 py-0.5 text-[11px] font-medium", s.c)}>{s.label}</span>;
}

export function StatusDot({ status }: { status: string }) {
  const c =
    status === "resolved"
      ? "bg-signal"
      : status === "acked" || status === "snoozed"
        ? "bg-warn"
        : "bg-danger";
  return <span className={cn("inline-block h-2 w-2 rounded-full", c)} />;
}
