import { useState } from "react";
import { Trash2 } from "lucide-react";
import { useChannels, useRules } from "../lib/hooks";
import { useCreateChannel, useCreateRule, useDeleteChannel, useDeleteRule } from "../lib/mutations";
import { Badge, Button, Card, EmptyState, Field, Input, Select, SectionTitle, Skeleton } from "../components/ui";
import type { Channel } from "../lib/api";

const CHANNEL_TYPES = ["slack", "telegram", "email", "webhook"];

// The event conditions a rule can watch, mapped to the engine's predicate shape.
const RULE_EVENTS: { label: string; condition: Record<string, unknown> }[] = [
  { label: "Sync failed", condition: { event: "sync_failed" } },
  { label: "Anomaly detected", condition: { event: "anomaly_detected" } },
  { label: "Checksum mismatch", condition: { event: "checksum_computed", field: "match", op: "is_false" } },
  { label: "Schema changed", condition: { event: "schema_changed" } },
];

export function Alerts() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Alerts &amp; Rules</h1>
        <p className="mt-1 text-sm text-muted">
          Route events to delivery channels. One incident is one thread — repeats deduplicate.
        </p>
      </div>
      <ChannelsSection />
      <RulesSection />
    </div>
  );
}

function ChannelsSection() {
  const { data, isLoading } = useChannels();
  const create = useCreateChannel();
  const del = useDeleteChannel();
  const [name, setName] = useState("");
  const [type, setType] = useState("slack");
  const [target, setTarget] = useState("");

  const targetKey = type === "email" ? "to" : type === "telegram" ? "chat_id" : "webhook_url";
  const valid = name.trim() !== "" && target.trim() !== "";
  const submit = () =>
    create.mutate(
      { name: name.trim(), type, config: { [targetKey]: target.trim() } },
      { onSuccess: () => { setName(""); setTarget(""); } },
    );

  return (
    <section>
      <SectionTitle>Channels</SectionTitle>
      <Card className="p-4">
        <div className="grid grid-cols-[1fr,10rem,1fr,auto] items-end gap-3">
          <Field label="Name">
            <Input value={name} placeholder="ops-slack" onChange={(e) => setName(e.target.value)} />
          </Field>
          <Field label="Type">
            <Select value={type} onChange={(e) => setType(e.target.value)}>
              {CHANNEL_TYPES.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </Select>
          </Field>
          <Field label={targetKey}>
            <Input value={target} placeholder="https://hooks.slack.com/…" onChange={(e) => setTarget(e.target.value)} />
          </Field>
          <Button variant="primary" disabled={!valid || create.isPending} onClick={submit}>
            Add
          </Button>
        </div>
        {create.isError && <p className="mt-2 text-xs text-danger">{(create.error as Error).message}</p>}

        <div className="mt-4">
          {isLoading ? (
            <Skeleton className="h-16" />
          ) : !data || data.length === 0 ? (
            <p className="text-xs text-faint">No channels yet — add one above.</p>
          ) : (
            <div className="divide-y divide-line">
              {data.map((c: Channel) => (
                <div key={c.id} className="flex items-center justify-between py-2">
                  <div className="flex items-center gap-2 text-sm">
                    <span className="text-text">{c.name}</span>
                    <Badge tone="info">{c.type}</Badge>
                    {!c.enabled && <Badge>disabled</Badge>}
                  </div>
                  <button className="text-faint hover:text-danger" disabled={del.isPending} onClick={() => del.mutate(c.id)}>
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>
    </section>
  );
}

function RulesSection() {
  const { data, isLoading } = useRules();
  const { data: channels } = useChannels();
  const create = useCreateRule();
  const del = useDeleteRule();
  const [name, setName] = useState("");
  const [eventIdx, setEventIdx] = useState(0);
  const [severity, setSeverity] = useState("warning");
  const [channelID, setChannelID] = useState<number | "">("");

  const valid = name.trim() !== "";
  const submit = () =>
    create.mutate(
      {
        name: name.trim(),
        condition: RULE_EVENTS[eventIdx].condition,
        severity,
        channel_ids: channelID === "" ? [] : [channelID],
      },
      { onSuccess: () => setName("") },
    );

  return (
    <section>
      <SectionTitle>Rules</SectionTitle>
      <Card className="p-4">
        <div className="grid grid-cols-[1fr,1fr,10rem,1fr,auto] items-end gap-3">
          <Field label="Rule name">
            <Input value={name} placeholder="Page on finance failures" onChange={(e) => setName(e.target.value)} />
          </Field>
          <Field label="When">
            <Select value={eventIdx} onChange={(e) => setEventIdx(Number(e.target.value))}>
              {RULE_EVENTS.map((ev, i) => (
                <option key={ev.label} value={i}>{ev.label}</option>
              ))}
            </Select>
          </Field>
          <Field label="Severity">
            <Select value={severity} onChange={(e) => setSeverity(e.target.value)}>
              <option value="info">info</option>
              <option value="warning">warning</option>
              <option value="critical">critical</option>
            </Select>
          </Field>
          <Field label="Notify channel">
            <Select value={channelID} onChange={(e) => setChannelID(e.target.value === "" ? "" : Number(e.target.value))}>
              <option value="">None (track only)</option>
              {(channels ?? []).map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </Select>
          </Field>
          <Button variant="primary" disabled={!valid || create.isPending} onClick={submit}>
            Add rule
          </Button>
        </div>
        {create.isError && <p className="mt-2 text-xs text-danger">{(create.error as Error).message}</p>}

        <div className="mt-4">
          {isLoading ? (
            <Skeleton className="h-16" />
          ) : !data || data.length === 0 ? (
            <EmptyState title="No rules yet" hint="Add a rule above to start alerting on events." />
          ) : (
            <div className="divide-y divide-line">
              {data.map((rule) => (
                <div key={rule.id} className="flex items-center justify-between py-2.5">
                  <div className="min-w-0">
                    <div className="text-sm text-text">{rule.name}</div>
                    <div className="tnum text-xs text-faint">
                      {rule.condition.event ?? "any"}
                      {rule.pipeline ? ` · ${rule.pipeline}` : " · all pipelines"}
                      {` · ${rule.channel_ids.length} channel${rule.channel_ids.length === 1 ? "" : "s"}`}
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge tone={rule.severity === "critical" ? "danger" : rule.severity === "warning" ? "warn" : "neutral"}>
                      {rule.severity}
                    </Badge>
                    <button className="text-faint hover:text-danger" disabled={del.isPending} onClick={() => del.mutate(rule.id)}>
                      <Trash2 size={14} />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>
    </section>
  );
}
