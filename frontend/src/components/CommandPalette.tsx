import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { CornerDownLeft, Search } from "lucide-react";
import { usePipelines } from "../lib/hooks";
import { cn } from "./ui";

type Item = { label: string; sub?: string; to: string };

// Cmd/Ctrl+K palette: jump to any page or pipeline. Keyboard-first.
export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  const nav = useNavigate();
  const { data: pipelines } = usePipelines();
  const [q, setQ] = useState("");
  const [sel, setSel] = useState(0);

  const items = useMemo<Item[]>(() => {
    const base: Item[] = [
      { label: "Dashboard", to: "/" },
      { label: "Pipelines", to: "/pipelines" },
      { label: "Incidents", to: "/incidents" },
      { label: "Data-Diff", to: "/diff" },
      { label: "Analytics & cost", to: "/analytics" },
      { label: "Audit log", to: "/audit" },
    ];
    const pipes: Item[] = (pipelines ?? []).map((p) => ({
      label: p.name,
      sub: `${p.source_type} → ${p.destination_type}`,
      to: `/pipelines/${p.id}`,
    }));
    const all = [...base, ...pipes];
    if (!q) return all;
    const needle = q.toLowerCase();
    return all.filter((i) => i.label.toLowerCase().includes(needle) || i.sub?.toLowerCase().includes(needle));
  }, [pipelines, q]);

  useEffect(() => {
    if (open) {
      setQ("");
      setSel(0);
    }
  }, [open]);
  useEffect(() => setSel(0), [q]);

  if (!open) return null;

  const go = (item: Item) => {
    nav(item.to);
    onClose();
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/50 p-4 pt-[12vh] backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="w-full max-w-xl overflow-hidden rounded-card border border-line bg-raised shadow-glow"
        onClick={(e) => e.stopPropagation()}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown") setSel((s) => Math.min(s + 1, items.length - 1));
          if (e.key === "ArrowUp") setSel((s) => Math.max(s - 1, 0));
          if (e.key === "Enter" && items[sel]) go(items[sel]);
          if (e.key === "Escape") onClose();
        }}
      >
        <div className="flex items-center gap-3 border-b border-line px-4">
          <Search size={16} className="text-faint" />
          {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
          <input
            autoFocus
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Jump to a page or pipeline…"
            className="h-12 flex-1 bg-transparent text-sm text-text outline-none placeholder:text-faint"
          />
        </div>
        <ul className="max-h-80 overflow-y-auto p-2">
          {items.length === 0 && <li className="px-3 py-6 text-center text-sm text-faint">No matches</li>}
          {items.map((item, i) => (
            <li key={item.to}>
              <button
                onMouseEnter={() => setSel(i)}
                onClick={() => go(item)}
                className={cn(
                  "flex w-full items-center justify-between rounded-control px-3 py-2 text-left text-sm",
                  i === sel ? "bg-signal/10 text-signal" : "text-text hover:bg-line/40",
                )}
              >
                <span className="flex flex-col">
                  <span>{item.label}</span>
                  {item.sub && <span className="text-xs text-faint">{item.sub}</span>}
                </span>
                {i === sel && <CornerDownLeft size={13} className="text-faint" />}
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
