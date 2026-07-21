import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Pause, Play, RotateCcw, Trash2 } from "lucide-react";
import { Button, Field, Input, Modal, Select } from "./ui";
import { useArchivePipeline, useBackfill, useRunPipeline, useSetPipelineStatus } from "../lib/mutations";
import type { BackfillRequest } from "../lib/api";

// PipelineActions is the command bar on a pipeline's detail page: run now, pause/
// resume, backfill a window, and archive (typed-confirmation). Each button wires
// a mutation hook; the queries refresh so the page reflects the change.
export function PipelineActions({ id, name, status }: { id: number; name: string; status: string }) {
  const run = useRunPipeline(id);
  const setStatus = useSetPipelineStatus(id);
  const [modal, setModal] = useState<"backfill" | "delete" | null>(null);
  const [ranFlash, setRanFlash] = useState(false);

  const triggerRun = () =>
    run.mutate(undefined, {
      onSuccess: () => {
        setRanFlash(true);
        setTimeout(() => setRanFlash(false), 2500);
      },
    });

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button variant="primary" size="sm" disabled={run.isPending} onClick={triggerRun}>
        <Play size={13} /> {ranFlash ? "Run started" : run.isPending ? "Starting…" : "Run now"}
      </Button>

      {status === "active" ? (
        <Button variant="outline" size="sm" disabled={setStatus.isPending} onClick={() => setStatus.mutate("paused")}>
          <Pause size={13} /> Pause
        </Button>
      ) : status === "paused" ? (
        <Button variant="outline" size="sm" disabled={setStatus.isPending} onClick={() => setStatus.mutate("active")}>
          <Play size={13} /> Resume
        </Button>
      ) : null}

      <Button variant="outline" size="sm" onClick={() => setModal("backfill")}>
        <RotateCcw size={13} /> Backfill
      </Button>
      <Button variant="ghost" size="sm" className="text-danger hover:text-danger" onClick={() => setModal("delete")}>
        <Trash2 size={13} /> Archive
      </Button>

      {run.isError && <span className="text-xs text-danger">{(run.error as Error).message}</span>}

      {modal === "backfill" && <BackfillModal id={id} onClose={() => setModal(null)} />}
      {modal === "delete" && <ArchiveModal id={id} name={name} onClose={() => setModal(null)} />}
    </div>
  );
}

function BackfillModal({ id, onClose }: { id: number; onClose: () => void }) {
  const backfill = useBackfill(id);
  const [stream, setStream] = useState("");
  const [mode, setMode] = useState<"pk_range" | "changed_since">("pk_range");
  const [pkMin, setPkMin] = useState("");
  const [pkMax, setPkMax] = useState("");
  const [sinceField, setSinceField] = useState("");
  const [sinceValue, setSinceValue] = useState("");

  const valid =
    stream.trim() !== "" &&
    (mode === "pk_range" ? pkMin.trim() !== "" || pkMax.trim() !== "" : sinceField.trim() !== "");

  const submit = () => {
    const body: BackfillRequest =
      mode === "pk_range"
        ? { stream: stream.trim(), pk_min: pkMin.trim(), pk_max: pkMax.trim() }
        : { stream: stream.trim(), since_field: sinceField.trim(), since_value: sinceValue.trim() };
    backfill.mutate(body, { onSuccess: onClose });
  };

  return (
    <Modal title="Backfill a window" onClose={onClose}>
      <div className="space-y-3">
        <Field label="Stream" hint="namespace.name">
          <Input value={stream} placeholder="public.orders" onChange={(e) => setStream(e.target.value)} />
        </Field>
        <Field label="Window">
          <Select value={mode} onChange={(e) => setMode(e.target.value as "pk_range" | "changed_since")}>
            <option value="pk_range">Primary-key range</option>
            <option value="changed_since">Changed since</option>
          </Select>
        </Field>
        {mode === "pk_range" ? (
          <div className="grid grid-cols-2 gap-3">
            <Field label="PK min">
              <Input value={pkMin} placeholder="1" onChange={(e) => setPkMin(e.target.value)} />
            </Field>
            <Field label="PK max">
              <Input value={pkMax} placeholder="1000" onChange={(e) => setPkMax(e.target.value)} />
            </Field>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-3">
            <Field label="Cursor field">
              <Input value={sinceField} placeholder="updated_at" onChange={(e) => setSinceField(e.target.value)} />
            </Field>
            <Field label="Since value">
              <Input value={sinceValue} placeholder="2026-01-01" onChange={(e) => setSinceValue(e.target.value)} />
            </Field>
          </div>
        )}
        {backfill.isError && <p className="text-xs text-danger">{(backfill.error as Error).message}</p>}
        <div className="flex justify-end gap-2 pt-1">
          <Button variant="ghost" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" size="sm" disabled={!valid || backfill.isPending} onClick={submit}>
            {backfill.isPending ? "Launching…" : "Launch backfill"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function ArchiveModal({ id, name, onClose }: { id: number; name: string; onClose: () => void }) {
  const archive = useArchivePipeline(id);
  const nav = useNavigate();
  const [confirm, setConfirm] = useState("");

  return (
    <Modal title="Archive pipeline" onClose={onClose}>
      <p className="text-sm text-muted">
        This pauses the pipeline and hides it. Sync history and diff records are preserved. Type the pipeline name to
        confirm.
      </p>
      <div className="mt-3 space-y-3">
        <Field label={`Type “${name}” to confirm`}>
          <Input value={confirm} placeholder={name} onChange={(e) => setConfirm(e.target.value)} />
        </Field>
        {archive.isError && <p className="text-xs text-danger">{(archive.error as Error).message}</p>}
        <div className="flex justify-end gap-2">
          <Button variant="ghost" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            size="sm"
            className="!bg-danger !text-bg"
            disabled={confirm !== name || archive.isPending}
            onClick={() => archive.mutate(undefined, { onSuccess: () => nav("/pipelines") })}
          >
            {archive.isPending ? "Archiving…" : "Archive"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}
