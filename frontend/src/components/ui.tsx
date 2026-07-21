import type {
  ButtonHTMLAttributes,
  HTMLAttributes,
  InputHTMLAttributes,
  ReactNode,
  SelectHTMLAttributes,
  TextareaHTMLAttributes,
} from "react";

export function cn(...parts: (string | false | null | undefined)[]): string {
  return parts.filter(Boolean).join(" ");
}

// Card — a raised surface with the waterline highlight.
export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "rounded-card border border-line bg-raised shadow-raised",
        className,
      )}
      {...props}
    />
  );
}

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "ghost" | "outline";
  size?: "sm" | "md";
};

export function Button({ variant = "outline", size = "md", className, ...props }: ButtonProps) {
  const variants = {
    primary: "bg-signal text-bg font-medium hover:brightness-110 border border-transparent",
    outline: "border border-line bg-surface text-text hover:border-signal/60 hover:text-signal",
    ghost: "text-muted hover:text-text hover:bg-line/40 border border-transparent",
  };
  const sizes = { sm: "h-8 px-3 text-xs", md: "h-9 px-4 text-sm" };
  return (
    <button
      className={cn(
        "inline-flex items-center gap-2 rounded-control transition-colors disabled:opacity-50",
        variants[variant],
        sizes[size],
        className,
      )}
      {...props}
    />
  );
}

// Badge — a small labeled chip.
export function Badge({
  children,
  tone = "neutral",
  className,
}: {
  children: ReactNode;
  tone?: "neutral" | "signal" | "warn" | "danger" | "info";
  className?: string;
}) {
  const tones = {
    neutral: "border-line text-muted",
    signal: "border-signal/40 text-signal bg-signal/10",
    warn: "border-warn/40 text-warn bg-warn/10",
    danger: "border-danger/40 text-danger bg-danger/10",
    info: "border-info/40 text-info bg-info/10",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium",
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return (
    <div className={cn("relative overflow-hidden rounded-control bg-line/40", className)}>
      <div className="absolute inset-0 -translate-x-full animate-[shimmer_1.4s_infinite] bg-gradient-to-r from-transparent via-white/5 to-transparent" />
    </div>
  );
}

export function EmptyState({
  title,
  hint,
  icon,
  action,
}: {
  title: string;
  hint?: string;
  icon?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center rounded-card border border-dashed border-line bg-surface/50 px-6 py-16 text-center">
      {icon && <div className="mb-3 text-faint">{icon}</div>}
      <p className="font-display text-sm text-text">{title}</p>
      {hint && <p className="mt-1 max-w-sm text-xs text-muted">{hint}</p>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}

// Stat — a labeled instrument readout.
export function Stat({ label, value, sub }: { label: string; value: ReactNode; sub?: ReactNode }) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wide text-faint">{label}</div>
      <div className="tnum mt-1 text-2xl text-text">{value}</div>
      {sub && <div className="mt-0.5 text-xs text-muted">{sub}</div>}
    </div>
  );
}

export function SectionTitle({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div className="mb-3 flex items-center justify-between">
      <h2 className="font-display text-sm font-medium uppercase tracking-wider text-muted">{children}</h2>
      {right}
    </div>
  );
}

// Modal — a centered dialog over a scrim. Closes on backdrop click or Escape.
export function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-bg/70 p-4 backdrop-blur-sm"
      onClick={onClose}
      onKeyDown={(e) => e.key === "Escape" && onClose()}
      role="presentation"
    >
      <Card className="w-full max-w-md p-5" onClick={(e) => e.stopPropagation()}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="font-display text-sm font-medium text-text">{title}</h3>
          <button className="text-faint hover:text-text" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        {children}
      </Card>
    </div>
  );
}

// --- form primitives ---

const controlBase =
  "w-full rounded-control border bg-surface px-3 text-sm text-text placeholder:text-faint " +
  "transition-colors focus:outline-none focus:border-signal disabled:opacity-50";

function borderTone(invalid?: boolean): string {
  return invalid ? "border-danger/60" : "border-line";
}

// Field wraps a control with a label, optional hint, and an error line.
export function Field({
  label,
  error,
  hint,
  children,
}: {
  label: string;
  error?: string;
  hint?: string;
  children: ReactNode;
}) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs font-medium text-muted">{label}</span>
      {children}
      {error ? (
        <span className="mt-1 block text-xs text-danger">{error}</span>
      ) : hint ? (
        <span className="mt-1 block text-xs text-faint">{hint}</span>
      ) : null}
    </label>
  );
}

type InputProps = InputHTMLAttributes<HTMLInputElement> & { invalid?: boolean };
export function Input({ invalid, className, ...props }: InputProps) {
  return <input className={cn(controlBase, "h-9", borderTone(invalid), className)} {...props} />;
}

type SelectProps = SelectHTMLAttributes<HTMLSelectElement> & { invalid?: boolean };
export function Select({ invalid, className, ...props }: SelectProps) {
  return <select className={cn(controlBase, "h-9", borderTone(invalid), className)} {...props} />;
}

type TextareaProps = TextareaHTMLAttributes<HTMLTextAreaElement> & { invalid?: boolean };
export function Textarea({ invalid, className, ...props }: TextareaProps) {
  return <textarea className={cn(controlBase, "py-2", borderTone(invalid), className)} {...props} />;
}
