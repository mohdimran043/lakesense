import { useState } from "react";
import { useChannels, useEscalationPolicies, useOncallSchedules } from "../lib/hooks";
import { useCreateEscalationPolicy, useCreateOncallSchedule } from "../lib/mutations";
import { Badge, Button, Card, EmptyState, Field, Input, SectionTitle, Select, Skeleton } from "../components/ui";
import type { EscalationStep } from "../lib/api";

// Escalations & On-call: build policies (ordered steps that page targets after a
// delay) and rotations (who is on call). These are what a rule's incidents climb
// when left unacked — the PagerDuty-style behaviour, free.
export function Escalations() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Escalations &amp; On-call</h1>
        <p className="mt-1 text-sm text-muted">
          Unacked incidents climb a policy step by step until someone responds.
        </p>
      </div>
      <PoliciesSection />
      <SchedulesSection />
    </div>
  );
}

function PoliciesSection() {
  const { data, isLoading } = useEscalationPolicies();
  const { data: channels } = useChannels();
  const create = useCreateEscalationPolicy();
  const [name, setName] = useState("");
  const [steps, setSteps] = useState<EscalationStep[]>([{ after_seconds: 300, channel_ids: [] }]);

  const setStep = (i: number, patch: Partial<EscalationStep>) =>
    setSteps(steps.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  const valid = name.trim() !== "" && steps.every((s) => s.channel_ids.length > 0);

  const submit = () =>
    create.mutate(
      { name: name.trim(), steps },
      { onSuccess: () => { setName(""); setSteps([{ after_seconds: 300, channel_ids: [] }]); } },
    );

  return (
    <section>
      <SectionTitle>Policies</SectionTitle>
      <Card className="space-y-4 p-4">
        <Field label="Policy name">
          <Input value={name} placeholder="Critical data escalation" onChange={(e) => setName(e.target.value)} />
        </Field>
        <div className="space-y-2">
          {steps.map((s, i) => (
            <div key={i} className="grid grid-cols-[auto,1fr,1fr,auto] items-end gap-3 rounded-control border border-line p-3">
              <div className="pb-2 text-xs text-faint">Step {i + 1}</div>
              <Field label="After (minutes)">
                <Input
                  type="number"
                  value={String(s.after_seconds / 60)}
                  onChange={(e) => setStep(i, { after_seconds: Math.max(0, Number(e.target.value)) * 60 })}
                />
              </Field>
              <Field label="Notify channel">
                <Select
                  value={s.channel_ids[0] ?? ""}
                  onChange={(e) => setStep(i, { channel_ids: e.target.value === "" ? [] : [Number(e.target.value)] })}
                >
                  <option value="">Choose…</option>
                  {(channels ?? []).map((c) => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </Select>
              </Field>
              {steps.length > 1 && (
                <button className="pb-2 text-xs text-faint hover:text-danger" onClick={() => setSteps(steps.filter((_, j) => j !== i))}>
                  Remove
                </button>
              )}
            </div>
          ))}
          <Button variant="ghost" size="sm" onClick={() => setSteps([...steps, { after_seconds: 900, channel_ids: [] }])}>
            + Add step
          </Button>
        </div>
        {create.isError && <p className="text-xs text-danger">{(create.error as Error).message}</p>}
        <div className="flex justify-end">
          <Button variant="primary" disabled={!valid || create.isPending} onClick={submit}>
            Create policy
          </Button>
        </div>

        <div className="border-t border-line pt-3">
          {isLoading ? (
            <Skeleton className="h-12" />
          ) : !data || data.length === 0 ? (
            <p className="text-xs text-faint">No policies yet.</p>
          ) : (
            <div className="divide-y divide-line">
              {data.map((p) => (
                <div key={p.id} className="flex items-center justify-between py-2 text-sm">
                  <span className="text-text">{p.name}</span>
                  <Badge>{p.steps.length} step{p.steps.length === 1 ? "" : "s"}</Badge>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>
    </section>
  );
}

function SchedulesSection() {
  const { data, isLoading } = useOncallSchedules();
  const { data: channels } = useChannels();
  const create = useCreateOncallSchedule();
  const [name, setName] = useState("");
  const [responder, setResponder] = useState("");
  const [channelID, setChannelID] = useState<number | "">("");

  const valid = name.trim() !== "" && responder.trim() !== "" && channelID !== "";
  const submit = () =>
    create.mutate(
      { name: name.trim(), rotation: [{ name: responder.trim(), channel_ids: channelID === "" ? [] : [channelID] }] },
      { onSuccess: () => { setName(""); setResponder(""); } },
    );

  return (
    <section>
      <SectionTitle>On-call rotations</SectionTitle>
      <Card className="p-4">
        <div className="grid grid-cols-[1fr,1fr,1fr,auto] items-end gap-3">
          <Field label="Rotation name">
            <Input value={name} placeholder="Primary" onChange={(e) => setName(e.target.value)} />
          </Field>
          <Field label="Responder">
            <Input value={responder} placeholder="Alice" onChange={(e) => setResponder(e.target.value)} />
          </Field>
          <Field label="Reach via">
            <Select value={channelID} onChange={(e) => setChannelID(e.target.value === "" ? "" : Number(e.target.value))}>
              <option value="">Choose…</option>
              {(channels ?? []).map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </Select>
          </Field>
          <Button variant="primary" disabled={!valid || create.isPending} onClick={submit}>
            Add
          </Button>
        </div>
        {create.isError && <p className="mt-2 text-xs text-danger">{(create.error as Error).message}</p>}
        <div className="mt-4">
          {isLoading ? (
            <Skeleton className="h-12" />
          ) : !data || data.length === 0 ? (
            <EmptyState title="No rotations yet" hint="Add a responder above; the rotation advances by ISO week." />
          ) : (
            <div className="divide-y divide-line">
              {data.map((sc) => (
                <div key={sc.id} className="flex items-center justify-between py-2 text-sm">
                  <span className="text-text">{sc.name}</span>
                  <span className="tnum text-xs text-faint">{sc.rotation.map((rr) => rr.name).join(" → ") || "—"}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </Card>
    </section>
  );
}
