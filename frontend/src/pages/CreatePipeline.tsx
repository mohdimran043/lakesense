import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge, Button, Card, Field, Input, Select, cn } from "../components/ui";
import { useConnectors } from "../lib/hooks";
import { useCreatePipeline } from "../lib/mutations";
import type { ConnectorCatalog, ConnectorField, CreatePipelineRequest } from "../lib/api";

// Destinations are engine writers, not registry connectors (no Spec), so they
// stay a small hardcoded list — shaped like a catalog entry for uniform rendering.
const DESTINATIONS: ConnectorCatalog[] = [
  {
    type: "parquet",
    label: "Parquet",
    maturity: "lakehouse",
    capabilities: [],
    fields: [{ key: "path", label: "Output directory", type: "string", required: true, hint: "./out" }],
  },
  {
    type: "ndjson",
    label: "NDJSON",
    maturity: "",
    capabilities: [],
    fields: [{ key: "path", label: "Output directory", type: "string", required: true, hint: "./out" }],
  },
];

const MODES = ["full_load", "incremental", "cdc"];
const STEPS = ["Source", "Destination", "Streams", "Review"];

// A settings value mirrors a ConnectorField's type while editing: text/enum/
// integer inputs hold strings, checkboxes hold booleans, string-lists hold arrays.
type SettingVal = string | boolean | string[];

interface Endpoint {
  type: string;
  settings: Record<string, SettingVal>;
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
  source: { type: "", settings: {} },
  destination: { type: "parquet", settings: {} },
  streams: [{ name: "", mode: "full_load", cursor_field: "" }],
};

// seedDefaults pre-fills a connector's settings with its schema defaults so the
// form shows the same values the engine would use (esp. booleans/enums).
function seedDefaults(fields: ConnectorField[]): Record<string, SettingVal> {
  const out: Record<string, SettingVal> = {};
  for (const f of fields) {
    if (f.default === undefined || f.default === "") continue;
    if (f.type === "boolean") out[f.key] = f.default === "true";
    else if (f.type === "string-list") out[f.key] = [f.default];
    else out[f.key] = f.default;
  }
  return out;
}

function isEmptyVal(v: SettingVal | undefined): boolean {
  if (v === undefined) return true;
  if (Array.isArray(v)) return v.filter((x) => x.trim()).length === 0;
  if (typeof v === "boolean") return false;
  return !v.trim();
}

function validateStep(step: number, s: State, source: ConnectorCatalog | undefined): Record<string, string> {
  const e: Record<string, string> = {};
  if (step === 0) {
    if (!s.name.trim()) e.name = "A name is required.";
    if (!source) e.source = "Choose a source.";
    source?.fields.forEach((f) => {
      if (f.required && isEmptyVal(s.source.settings[f.key])) e["src_" + f.key] = `${f.label} is required.`;
    });
  }
  if (step === 1) {
    const dst = DESTINATIONS.find((d) => d.type === s.destination.type);
    dst?.fields.forEach((f) => {
      if (f.required && isEmptyVal(s.destination.settings[f.key])) e["dst_" + f.key] = `${f.label} is required.`;
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

// buildSettings converts the editing state into the typed JSON the write API
// stores: integers as numbers, booleans as booleans, string-lists as arrays,
// everything else as trimmed strings. Empty scalars are dropped.
function buildSettings(fields: ConnectorField[], raw: Record<string, SettingVal>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const f of fields) {
    const v = raw[f.key];
    if (v === undefined) continue;
    if (f.type === "string-list") {
      const arr = (Array.isArray(v) ? v : []).map((x) => x.trim()).filter(Boolean);
      if (arr.length) out[f.key] = arr;
    } else if (f.type === "boolean") {
      if (typeof v === "boolean") out[f.key] = v;
    } else if (f.type === "integer") {
      if (typeof v === "string" && v.trim()) out[f.key] = Number(v);
    } else if (typeof v === "string" && v.trim()) {
      out[f.key] = v.trim();
    }
  }
  return out;
}

function toRequest(s: State, source: ConnectorCatalog | undefined): CreatePipelineRequest {
  const dst = DESTINATIONS.find((d) => d.type === s.destination.type);
  return {
    name: s.name.trim(),
    environment: s.environment,
    source: { type: s.source.type, settings: buildSettings(source?.fields ?? [], s.source.settings) },
    destination: { type: s.destination.type, settings: buildSettings(dst?.fields ?? [], s.destination.settings) },
    schedule: s.schedule,
    streams: s.streams.map((st) => ({
      name: st.name.trim(),
      mode: st.mode,
      ...(st.mode === "incremental" && st.cursor_field.trim() ? { cursor_field: st.cursor_field.trim() } : {}),
    })),
  };
}

const MATURITY_LABEL: Record<string, string> = {
  certified: "Certified",
  stable: "Stable",
  beta: "Beta",
  lakehouse: "Lakehouse",
};

export function CreatePipeline() {
  const nav = useNavigate();
  const create = useCreatePipeline();
  const connectors = useConnectors();
  const [step, setStep] = useState(0);
  const [s, setS] = useState<State>(initialState);

  const sources = connectors.data ?? [];
  const source = sources.find((c) => c.type === s.source.type);

  // Once the catalog loads, default the source to postgres (or the first entry)
  // if the current selection isn't in it.
  useEffect(() => {
    if (sources.length === 0) return;
    setS((prev) => {
      if (sources.some((c) => c.type === prev.source.type)) return prev;
      const preferred = sources.find((c) => c.type === "postgres") ?? sources[0];
      return { ...prev, source: { type: preferred.type, settings: seedDefaults(preferred.fields) } };
    });
  }, [sources]);

  const errors = validateStep(step, s, source);
  const stepValid = Object.keys(errors).length === 0;

  const selectSource = (type: string) => {
    const c = sources.find((x) => x.type === type);
    setS({ ...s, source: { type, settings: c ? seedDefaults(c.fields) : {} } });
  };
  const selectDest = (type: string) => {
    const d = DESTINATIONS.find((x) => x.type === type);
    setS({ ...s, destination: { type, settings: d ? seedDefaults(d.fields) : {} } });
  };
  const setSetting = (which: "source" | "destination", key: string, val: SettingVal) =>
    setS({ ...s, [which]: { ...s[which], settings: { ...s[which].settings, [key]: val } } });

  const setStream = (i: number, patch: Partial<Stream>) =>
    setS({ ...s, streams: s.streams.map((st, j) => (j === i ? { ...st, ...patch } : st)) });
  const addStream = () => setS({ ...s, streams: [...s.streams, { name: "", mode: "full_load", cursor_field: "" }] });
  const removeStream = (i: number) => setS({ ...s, streams: s.streams.filter((_, j) => j !== i) });

  const submit = () => {
    create.mutate(toRequest(s, source), {
      onSuccess: (p) => nav(`/pipelines/${p.id}`),
    });
  };

  const dst = DESTINATIONS.find((d) => d.type === s.destination.type);

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

            {connectors.isLoading && <p className="text-sm text-muted">Loading connectors…</p>}
            {connectors.isError && (
              <div className="rounded-control border border-danger/40 bg-danger/10 px-3 py-2 text-xs text-danger">
                Couldn't load the connector catalog.{" "}
                <button className="underline" onClick={() => connectors.refetch()}>
                  Retry
                </button>
              </div>
            )}
            {!connectors.isLoading && !connectors.isError && (
              <>
                <ConnectorPicker
                  items={sources.map((c) => ({ type: c.type, label: c.label, badge: MATURITY_LABEL[c.maturity] ?? c.maturity }))}
                  selected={s.source.type}
                  onSelect={selectSource}
                  error={errors.source}
                />
                <SettingsFields fields={source?.fields ?? []} settings={s.source.settings} errPrefix="src_" errors={errors} onChange={(k, v) => setSetting("source", k, v)} />
              </>
            )}
          </div>
        )}

        {step === 1 && (
          <div className="space-y-4">
            <ConnectorPicker
              items={DESTINATIONS.map((d) => ({ type: d.type, label: d.label, badge: MATURITY_LABEL[d.maturity] ?? d.maturity }))}
              selected={s.destination.type}
              onSelect={selectDest}
              error={errors.destination}
            />
            <SettingsFields fields={dst?.fields ?? []} settings={s.destination.settings} errPrefix="dst_" errors={errors} onChange={(k, v) => setSetting("destination", k, v)} />
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

interface PickerItem {
  type: string;
  label: string;
  badge?: string;
}

function ConnectorPicker({
  items,
  selected,
  onSelect,
  error,
}: {
  items: PickerItem[];
  selected: string;
  onSelect: (type: string) => void;
  error?: string;
}) {
  return (
    <div>
      <span className="mb-1 block text-xs font-medium text-muted">Connector</span>
      <div className="grid grid-cols-3 gap-2">
        {items.map((c) => (
          <button
            key={c.type}
            onClick={() => onSelect(c.type)}
            className={cn(
              "flex flex-col items-start gap-1 rounded-control border px-3 py-2 text-left transition-colors",
              c.type === selected ? "border-signal bg-signal/10" : "border-line hover:border-signal/50",
            )}
          >
            <span className="text-sm text-text">{c.label}</span>
            {c.badge && (
              <Badge tone="signal" className="!text-[10px]">
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
  fields,
  settings,
  errPrefix,
  errors,
  onChange,
}: {
  fields: ConnectorField[];
  settings: Record<string, SettingVal>;
  errPrefix: string;
  errors: Record<string, string>;
  onChange: (key: string, val: SettingVal) => void;
}) {
  if (fields.length === 0) return null;
  return (
    <div className="grid grid-cols-2 gap-3">
      {fields.map((f) => (
        <div key={f.key} className={f.type === "string-list" ? "col-span-2" : ""}>
          <FieldControl f={f} value={settings[f.key]} err={errors[errPrefix + f.key]} onChange={(v) => onChange(f.key, v)} />
        </div>
      ))}
    </div>
  );
}

function FieldControl({
  f,
  value,
  err,
  onChange,
}: {
  f: ConnectorField;
  value: SettingVal | undefined;
  err?: string;
  onChange: (val: SettingVal) => void;
}) {
  const label = f.required ? f.label : `${f.label} (optional)`;

  if (f.type === "boolean") {
    const checked = value === true;
    return (
      <Field label={label} hint={f.hint}>
        <label className="flex items-center gap-2 text-sm text-text">
          <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
          <span className="text-muted">{f.hint || "Enabled"}</span>
        </label>
      </Field>
    );
  }

  if (f.type === "enum") {
    return (
      <Field label={label} error={err} hint={f.hint}>
        <Select value={typeof value === "string" ? value : f.default ?? ""} onChange={(e) => onChange(e.target.value)}>
          {(f.enum ?? []).map((opt) => (
            <option key={opt} value={opt}>
              {opt === "" ? "—" : opt}
            </option>
          ))}
        </Select>
      </Field>
    );
  }

  if (f.type === "string-list") {
    const items = Array.isArray(value) && value.length > 0 ? value : [""];
    const update = (i: number, v: string) => onChange(items.map((x, j) => (j === i ? v : x)));
    const add = () => onChange([...items, ""]);
    const remove = (i: number) => onChange(items.filter((_, j) => j !== i));
    return (
      <Field label={label} error={err} hint={f.hint}>
        <div className="space-y-2">
          {items.map((item, i) => (
            <div key={i} className="flex gap-2">
              <Input value={item} placeholder={f.hint} onChange={(e) => update(i, e.target.value)} />
              {items.length > 1 && (
                <button className="px-2 text-faint hover:text-danger" onClick={() => remove(i)} type="button" aria-label="Remove">
                  −
                </button>
              )}
            </div>
          ))}
          <button className="text-xs text-signal hover:underline" onClick={add} type="button">
            + Add
          </button>
        </div>
      </Field>
    );
  }

  // string / integer
  return (
    <Field label={label} error={err} hint={f.hint}>
      <Input
        type={f.secret ? "password" : f.type === "integer" ? "number" : "text"}
        value={typeof value === "string" ? value : ""}
        invalid={!!err}
        placeholder={f.default || f.hint}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
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
