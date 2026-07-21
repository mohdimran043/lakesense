import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge, Button, Card, Field, Input, Select, cn } from "../components/ui";
import { useCreatePipeline } from "../lib/mutations";
import type { CreatePipelineRequest } from "../lib/api";

// A connector option: what it is, whether it ships today, and its settings.
interface Connector {
  type: string;
  label: string;
  badge?: string;
  ready: boolean;
  fields: { key: string; label: string; secret?: boolean; placeholder?: string }[];
}

const f = (key: string, label: string, placeholder?: string, secret?: boolean) => ({
  key,
  label,
  placeholder,
  secret,
});

const SOURCES: Connector[] = [
  {
    type: "postgres",
    label: "PostgreSQL",
    badge: "Certified",
    ready: true,
    fields: [f("host", "Host", "db.internal"), f("port", "Port", "5432"), f("database", "Database", "shop"), f("user", "User", "readonly"), f("password", "Password", "", true)],
  },
  { type: "sqlite", label: "SQLite", badge: "Beta", ready: true, fields: [f("path", "Database file", "./data.db")] },
  { type: "mysql", label: "MySQL", badge: "Coming soon", ready: false, fields: [] },
  { type: "mongodb", label: "MongoDB", badge: "Coming soon", ready: false, fields: [] },
  { type: "kafka", label: "Kafka", badge: "Coming soon", ready: false, fields: [] },
];

const DESTINATIONS: Connector[] = [
  { type: "parquet", label: "Parquet", badge: "Lakehouse", ready: true, fields: [f("path", "Output directory", "./out")] },
  { type: "ndjson", label: "NDJSON", ready: true, fields: [f("path", "Output directory", "./out")] },
];

const MODES = ["full_load", "incremental", "cdc"];
const STEPS = ["Source", "Destination", "Streams", "Review"];

interface Endpoint {
  type: string;
  settings: Record<string, string>;
}
interface Stream {
  name: string;
  mode: string;
  cursor_field: string;
}
interface State {
  name: string;
  environment: string;
  schedule: string;
  source: Endpoint;
  destination: Endpoint;
  streams: Stream[];
}

const initialState: State = {
  name: "",
  environment: "dev",
  schedule: "@daily",
  source: { type: "postgres", settings: {} },
  destination: { type: "parquet", settings: {} },
  streams: [{ name: "", mode: "full_load", cursor_field: "" }],
};

function connector(list: Connector[], type: string): Connector | undefined {
  return list.find((c) => c.type === type);
}

// validateStep mirrors the backend rules so errors surface before submit.
function validateStep(step: number, s: State): Record<string, string> {
  const e: Record<string, string> = {};
  if (step === 0) {
    if (!s.name.trim()) e.name = "A name is required.";
    const src = connector(SOURCES, s.source.type);
    if (!src?.ready) e.source = "Choose a source that ships today.";
    src?.fields.forEach((fld) => {
      if (!s.source.settings[fld.key]?.trim()) e["src_" + fld.key] = `${fld.label} is required.`;
    });
  }
  if (step === 1) {
    const dst = connector(DESTINATIONS, s.destination.type);
    if (!dst?.ready) e.destination = "Choose a destination that ships today.";
    dst?.fields.forEach((fld) => {
      if (!s.destination.settings[fld.key]?.trim()) e["dst_" + fld.key] = `${fld.label} is required.`;
    });
  }
  if (step === 2) {
    if (s.streams.length === 0) e.streams = "Add at least one stream.";
    s.streams.forEach((st, i) => {
      if (!st.name.trim()) e["stream_" + i] = "Stream name is required.";
      if (st.mode === "incremental" && !st.cursor_field.trim()) e["cursor_" + i] = "Incremental needs a cursor field.";
    });
  }
  return e;
}

function toRequest(s: State): CreatePipelineRequest {
  return {
    name: s.name.trim(),
    environment: s.environment,
    source: s.source,
    destination: s.destination,
    schedule: s.schedule,
    streams: s.streams.map((st) => ({
      name: st.name.trim(),
      mode: st.mode,
      ...(st.mode === "incremental" && st.cursor_field.trim() ? { cursor_field: st.cursor_field.trim() } : {}),
    })),
  };
}

export function CreatePipeline() {
  const nav = useNavigate();
  const create = useCreatePipeline();
  const [step, setStep] = useState(0);
  const [s, setS] = useState<State>(initialState);

  const errors = validateStep(step, s);
  const stepValid = Object.keys(errors).length === 0;

  const setSource = (type: string) => setS({ ...s, source: { type, settings: {} } });
  const setDest = (type: string) => setS({ ...s, destination: { type, settings: {} } });
  const setSetting = (which: "source" | "destination", key: string, val: string) =>
    setS({ ...s, [which]: { ...s[which], settings: { ...s[which].settings, [key]: val } } });

  const setStream = (i: number, patch: Partial<Stream>) =>
    setS({ ...s, streams: s.streams.map((st, j) => (j === i ? { ...st, ...patch } : st)) });
  const addStream = () => setS({ ...s, streams: [...s.streams, { name: "", mode: "full_load", cursor_field: "" }] });
  const removeStream = (i: number) => setS({ ...s, streams: s.streams.filter((_, j) => j !== i) });

  const submit = () => {
    create.mutate(toRequest(s), { onSuccess: (p) => nav(`/pipelines/${p.id}`) });
  };

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">New pipeline</h1>
        <p className="mt-1 text-sm text-muted">Connect a source, choose a lakehouse destination, pick streams.</p>
      </div>

      <ProgressRail step={step} />

      <Card className="p-6">
        {step === 0 && (
          <div className="space-y-4">
            <Field label="Pipeline name" error={errors.name}>
              <Input value={s.name} invalid={!!errors.name} placeholder="Orders to Lake" onChange={(e) => setS({ ...s, name: e.target.value })} />
            </Field>
            <div className="grid grid-cols-2 gap-4">
              <Field label="Environment">
                <Select value={s.environment} onChange={(e) => setS({ ...s, environment: e.target.value })}>
                  <option value="dev">dev</option>
                  <option value="staging">staging</option>
                  <option value="prod">prod</option>
                </Select>
              </Field>
              <Field label="Schedule" hint="How often it runs">
                <Select value={s.schedule} onChange={(e) => setS({ ...s, schedule: e.target.value })}>
                  <option value="">Manual</option>
                  <option value="@hourly">Hourly</option>
                  <option value="@daily">Daily</option>
                  <option value="@weekly">Weekly</option>
                </Select>
              </Field>
            </div>
            <ConnectorPicker list={SOURCES} selected={s.source.type} onSelect={setSource} error={errors.source} />
            <SettingsFields conn={connector(SOURCES, s.source.type)} settings={s.source.settings} errPrefix="src_" errors={errors} onChange={(k, v) => setSetting("source", k, v)} />
          </div>
        )}

        {step === 1 && (
          <div className="space-y-4">
            <ConnectorPicker list={DESTINATIONS} selected={s.destination.type} onSelect={setDest} error={errors.destination} />
            <SettingsFields conn={connector(DESTINATIONS, s.destination.type)} settings={s.destination.settings} errPrefix="dst_" errors={errors} onChange={(k, v) => setSetting("destination", k, v)} />
          </div>
        )}

        {step === 2 && (
          <div className="space-y-3">
            {errors.streams && <p className="text-xs text-danger">{errors.streams}</p>}
            {s.streams.map((st, i) => (
              <div key={i} className="rounded-control border border-line p-3">
                <div className="grid grid-cols-[1fr,auto] gap-3">
                  <Field label="Stream" error={errors["stream_" + i]}>
                    <Input value={st.name} invalid={!!errors["stream_" + i]} placeholder="public.orders" onChange={(e) => setStream(i, { name: e.target.value })} />
                  </Field>
                  <Field label="Mode">
                    <Select value={st.mode} onChange={(e) => setStream(i, { mode: e.target.value })}>
                      {MODES.map((m) => (
                        <option key={m} value={m}>
                          {m}
                        </option>
                      ))}
                    </Select>
                  </Field>
                </div>
                {st.mode === "incremental" && (
                  <Field label="Cursor field" error={errors["cursor_" + i]}>
                    <Input value={st.cursor_field} invalid={!!errors["cursor_" + i]} placeholder="updated_at" onChange={(e) => setStream(i, { cursor_field: e.target.value })} />
                  </Field>
                )}
                {s.streams.length > 1 && (
                  <button className="mt-2 text-xs text-faint hover:text-danger" onClick={() => removeStream(i)}>
                    Remove
                  </button>
                )}
              </div>
            ))}
            <Button variant="ghost" size="sm" onClick={addStream}>
              + Add stream
            </Button>
          </div>
        )}

        {step === 3 && <Review s={s} />}

        {create.isError && (
          <div className="mt-4 rounded-control border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
            {(create.error as Error).message}
          </div>
        )}

        <div className="mt-6 flex items-center justify-between">
          <Button variant="ghost" onClick={() => (step === 0 ? nav("/pipelines") : setStep(step - 1))}>
            {step === 0 ? "Cancel" : "Back"}
          </Button>
          {step < STEPS.length - 1 ? (
            <Button variant="primary" disabled={!stepValid} onClick={() => setStep(step + 1)}>
              Next
            </Button>
          ) : (
            <Button variant="primary" disabled={create.isPending} onClick={submit}>
              {create.isPending ? "Creating…" : "Create pipeline"}
            </Button>
          )}
        </div>
      </Card>
    </div>
  );
}

function ProgressRail({ step }: { step: number }) {
  return (
    <div className="flex items-center gap-2">
      {STEPS.map((label, i) => (
        <div key={label} className="flex flex-1 items-center gap-2">
          <div
            className={cn(
              "flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-[11px] tnum",
              i < step && "border-signal bg-signal/15 text-signal",
              i === step && "border-signal text-signal",
              i > step && "border-line text-faint",
            )}
          >
            {i + 1}
          </div>
          <span className={cn("text-xs", i === step ? "text-text" : "text-faint")}>{label}</span>
          {i < STEPS.length - 1 && <div className={cn("h-px flex-1", i < step ? "bg-signal/40" : "bg-line")} />}
        </div>
      ))}
    </div>
  );
}

function ConnectorPicker({
  list,
  selected,
  onSelect,
  error,
}: {
  list: Connector[];
  selected: string;
  onSelect: (type: string) => void;
  error?: string;
}) {
  return (
    <div>
      <span className="mb-1 block text-xs font-medium text-muted">Connector</span>
      <div className="grid grid-cols-3 gap-2">
        {list.map((c) => (
          <button
            key={c.type}
            disabled={!c.ready}
            onClick={() => onSelect(c.type)}
            className={cn(
              "flex flex-col items-start gap-1 rounded-control border px-3 py-2 text-left transition-colors",
              c.type === selected ? "border-signal bg-signal/10" : "border-line hover:border-signal/50",
              !c.ready && "cursor-not-allowed opacity-50 hover:border-line",
            )}
          >
            <span className="text-sm text-text">{c.label}</span>
            {c.badge && (
              <Badge tone={c.ready ? "signal" : "neutral"} className="!text-[10px]">
                {c.badge}
              </Badge>
            )}
          </button>
        ))}
      </div>
      {error && <p className="mt-1 text-xs text-danger">{error}</p>}
    </div>
  );
}

function SettingsFields({
  conn,
  settings,
  errPrefix,
  errors,
  onChange,
}: {
  conn: Connector | undefined;
  settings: Record<string, string>;
  errPrefix: string;
  errors: Record<string, string>;
  onChange: (key: string, val: string) => void;
}) {
  if (!conn || conn.fields.length === 0) return null;
  return (
    <div className="grid grid-cols-2 gap-3">
      {conn.fields.map((fld) => (
        <Field key={fld.key} label={fld.label} error={errors[errPrefix + fld.key]}>
          <Input
            type={fld.secret ? "password" : "text"}
            value={settings[fld.key] ?? ""}
            invalid={!!errors[errPrefix + fld.key]}
            placeholder={fld.placeholder}
            onChange={(e) => onChange(fld.key, e.target.value)}
          />
        </Field>
      ))}
    </div>
  );
}

function Review({ s }: { s: State }) {
  const row = (label: string, value: string) => (
    <div className="flex justify-between border-b border-line py-2 last:border-0">
      <span className="text-xs text-faint">{label}</span>
      <span className="tnum text-xs text-text">{value}</span>
    </div>
  );
  return (
    <div>
      {row("Name", s.name)}
      {row("Environment", s.environment)}
      {row("Schedule", s.schedule || "manual")}
      {row("Source", s.source.type)}
      {row("Destination", s.destination.type)}
      {row("Streams", s.streams.map((st) => `${st.name} (${st.mode})`).join(", "))}
    </div>
  );
}
