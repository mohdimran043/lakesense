import { Link } from "react-router-dom";
import { BadgeCheck } from "lucide-react";
import { usePipelines } from "../lib/hooks";
import { Card, EmptyState, Skeleton, Stat } from "../components/ui";
import { VerifiedBadge } from "../components/signals";
import { compactNum } from "../lib/format";

// The correctness board — the product's Datafold-killer, front and center:
// every pipeline's latest-sync verification at a glance.
export function Diff() {
  const { data, isLoading, error } = usePipelines();
  const list = data ?? [];
  const verified = list.filter((p) => p.diff_verified);
  const totalRows = verified.reduce((a, p) => a + p.verified_rows, 0);

  return (
    <div className="space-y-5">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Data-Diff</h1>
        <p className="mt-1 text-sm text-muted">
          Order-independent checksums on both sides of every sync — proof the copy is correct, free.
        </p>
      </div>

      {isLoading ? (
        <Skeleton className="h-56" />
      ) : error ? (
        <EmptyState title="Can't reach the control plane" hint="Start the backend and seed demo data." />
      ) : list.length === 0 ? (
        <EmptyState icon={<BadgeCheck size={28} className="text-signal" />} title="No syncs verified yet" />
      ) : (
        <>
          <div className="grid grid-cols-2 gap-4 md:grid-cols-3">
            <Card className="p-4">
              <Stat label="Verified pipelines" value={`${verified.length}/${list.length}`} />
            </Card>
            <Card className="p-4">
              <Stat label="Rows verified" value={compactNum(totalRows)} sub="latest syncs" />
            </Card>
            <Card className="p-4">
              <Stat
                label="Mismatches"
                value={list.length - verified.length}
                sub={list.length === verified.length ? "all clear" : "needs attention"}
              />
            </Card>
          </div>

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {list.map((p) => (
              <Link key={p.id} to={`/pipelines/${p.id}`} className="group block">
                <Card className="flex items-center justify-between p-4 transition-colors group-hover:border-signal/40">
                  <div className="min-w-0">
                    <div className="truncate text-sm text-text">{p.name}</div>
                    <div className="tnum text-xs text-faint">{p.source_type}</div>
                  </div>
                  <VerifiedBadge verified={p.diff_verified} rows={p.verified_rows} />
                </Card>
              </Link>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
