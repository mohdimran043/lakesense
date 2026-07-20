import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  Activity,
  BadgeCheck,
  Bell,
  Command,
  DollarSign,
  GitBranch,
  LayoutDashboard,
  Moon,
  ScrollText,
  Sun,
  Waves,
} from "lucide-react";
import { cn } from "./ui";
import { applyTheme, getTheme, type Theme } from "./theme";
import { CommandPalette } from "./CommandPalette";

const nav = [
  { to: "/", label: "Dashboard", icon: LayoutDashboard, end: true },
  { to: "/pipelines", label: "Pipelines", icon: GitBranch },
  { to: "/incidents", label: "Incidents", icon: Bell },
  { to: "/diff", label: "Data-Diff", icon: BadgeCheck },
  { to: "/analytics", label: "Analytics", icon: DollarSign },
  { to: "/audit", label: "Audit log", icon: ScrollText },
];

export function AppShell({ children }: { children: React.ReactNode }) {
  const [theme, setTheme] = useState<Theme>(getTheme);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const loc = useLocation();

  useEffect(() => applyTheme(theme), [theme]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((v) => !v);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const crumbs = loc.pathname === "/" ? ["Dashboard"] : loc.pathname.split("/").filter(Boolean);

  return (
    <div className="flex h-full">
      {/* Sidebar */}
      <aside className="hidden w-56 shrink-0 flex-col border-r border-line bg-surface md:flex">
        <div className="flex h-14 items-center gap-2 border-b border-line px-4">
          <div className="grid h-7 w-7 place-items-center rounded-md bg-signal/15 text-signal">
            <Waves size={16} strokeWidth={2.5} />
          </div>
          <div className="font-display text-[15px] font-semibold tracking-tight text-text">
            Lake<span className="text-signal">Sense</span>
          </div>
        </div>
        <nav className="flex-1 space-y-0.5 p-2">
          {nav.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.end}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-3 rounded-control px-3 py-2 text-sm transition-colors",
                  isActive
                    ? "bg-signal/10 text-signal"
                    : "text-muted hover:bg-line/40 hover:text-text",
                )
              }
            >
              <n.icon size={16} />
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="border-t border-line p-3 text-[11px] text-faint">
          <div className="flex items-center gap-1.5">
            <Activity size={12} className="text-signal" />
            Your data pipelines, self-aware.
          </div>
        </div>
      </aside>

      {/* Main column */}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center justify-between border-b border-line bg-surface/80 px-5 backdrop-blur">
          <div className="flex items-center gap-2 text-sm text-muted">
            {crumbs.map((c, i) => (
              <span key={i} className="flex items-center gap-2">
                {i > 0 && <span className="text-faint">/</span>}
                <span className={cn("capitalize", i === crumbs.length - 1 && "text-text")}>{c}</span>
              </span>
            ))}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPaletteOpen(true)}
              className="hidden items-center gap-2 rounded-control border border-line bg-bg px-2.5 py-1.5 text-xs text-muted transition-colors hover:border-signal/50 hover:text-text sm:flex"
            >
              <Command size={13} /> Search
              <kbd className="tnum rounded border border-line bg-surface px-1 text-[10px]">⌘K</kbd>
            </button>
            <button
              onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
              aria-label="Toggle theme"
              className="grid h-8 w-8 place-items-center rounded-control border border-line text-muted hover:text-text"
            >
              {theme === "dark" ? <Sun size={15} /> : <Moon size={15} />}
            </button>
          </div>
        </header>

        <main className="depth-grid flex-1 overflow-y-auto">
          <div className="mx-auto max-w-7xl animate-rise p-5 md:p-7">{children}</div>
        </main>
      </div>

      <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} />
    </div>
  );
}
