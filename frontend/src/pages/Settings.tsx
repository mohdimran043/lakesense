import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button, Card, Field, Input, SectionTitle } from "../components/ui";
import { getCostModel, setCostModel } from "../lib/settings";

// Settings: the transparent cost model (anti-Fivetran-opacity). Saved locally and
// applied to the Analytics estimate. No auth/SSO yet — that is the future Pro tier.
export function Settings() {
  const qc = useQueryClient();
  const initial = getCostModel();
  const [perGB, setPerGB] = useState(String(initial.costPerGB));
  const [perHour, setPerHour] = useState(String(initial.costPerHour));
  const [saved, setSaved] = useState(false);

  const save = () => {
    setCostModel({ costPerGB: Number(perGB) || 0, costPerHour: Number(perHour) || 0 });
    void qc.invalidateQueries({ queryKey: ["analytics"] });
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div className="mx-auto max-w-xl space-y-6">
      <div>
        <h1 className="font-display text-2xl font-semibold tracking-tight text-text">Settings</h1>
        <p className="mt-1 text-sm text-muted">Tune the transparent cost model applied across Analytics.</p>
      </div>

      <section>
        <SectionTitle>Cost model</SectionTitle>
        <Card className="space-y-4 p-4">
          <div className="grid grid-cols-2 gap-4">
            <Field label="$ / GB stored" hint="e.g. S3 standard ≈ 0.023">
              <Input type="number" step="0.001" value={perGB} onChange={(e) => setPerGB(e.target.value)} />
            </Field>
            <Field label="$ / compute-hour" hint="engine runtime cost">
              <Input type="number" step="0.01" value={perHour} onChange={(e) => setPerHour(e.target.value)} />
            </Field>
          </div>
          <div className="flex items-center justify-end gap-3">
            {saved && <span className="text-xs text-signal">Saved — Analytics updated</span>}
            <Button variant="primary" onClick={save}>
              Save cost model
            </Button>
          </div>
        </Card>
      </section>

      <section>
        <SectionTitle>About</SectionTitle>
        <Card className="p-4 text-sm text-muted">
          Authentication, SSO &amp; RBAC are deliberately reserved for a future Pro tier — everything else is free and
          self-hosted. Actions you take here are recorded in the audit log.
        </Card>
      </section>
    </div>
  );
}
