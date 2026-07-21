# Track C1 — Create-Pipeline Wizard + Mutation Layer (Design)

**Date:** 2026-07-21
**Spec refs:** `lakesense-final-prompt.md` §4.16b (first-run onboarding), §4.16g (Create-pipeline wizard, Source picker)
**Status:** approved design, ready to implement (on `main`)
**Parent:** Track C (write-path UI). C1 is the first slice; it builds the mutation
plumbing C2–C5 reuse.

## Goal

Let a user create a pipeline from the dashboard — a stepped wizard (Source →
Destination → Streams → Review) that `POST`s to the B1 write API and lands on the
new pipeline's detail page. This is the #1 missing UI and the spec's "empty
install → first sync" flow. It also introduces the frontend's write layer
(mutation helpers, form primitives) that every later write screen depends on.

## Non-goals (later Track C slices)

- Run / pause / backfill / delete actions (C2), incident actions (C3), rules &
  channels management (C4), environments/settings (C5).
- Live connection testing ("Test connection") — the backend has no check endpoint
  exposed to the UI yet; the wizard validates shape and relies on the first run
  surfacing connectivity errors as events.
- A component test runner — the repo gates the frontend with strict `tsc` + `vite
  build` + `oxlint`; C1 keeps to that (no vitest scope creep). Wizard logic is
  kept in small typed helpers.

## Architecture

All additive to the existing frontend (React 19, TanStack Query, react-router 7,
the "abyssal depth-sounder" tokens + `components/ui.tsx`).

```
lib/api.ts        + post/patch/del helpers (X-Actor header) + createPipeline()
lib/mutations.ts  (new) useCreatePipeline() → invalidates ["pipelines"]
components/ui.tsx  + Field, Input, Select, Textarea (token-styled form primitives)
pages/CreatePipeline.tsx (new)  the 4-step wizard
App.tsx            + route /pipelines/new
pages/Pipelines.tsx + "New pipeline" CTA (header + empty state)
```

### Write helpers (`lib/api.ts`)

```ts
async function send<T>(method: string, path: string, body?: unknown): Promise<T>
// sets { "content-type": "application/json", "x-actor": "web-ui", accept }
// throws Error(body.error ?? `Request failed (status)`) on !res.ok
export const post  = <T>(p: string, b?: unknown) => send<T>("POST", p, b);
export const patch = <T>(p: string, b?: unknown) => send<T>("PATCH", p, b);
export const del   = (p: string) => send<void>("DELETE", p);
```

Types mirror the B1 request/response:
```ts
export interface EndpointInput { type: string; settings: Record<string,string> }
export interface StreamInput { name: string; mode: string; cursor_field?: string }
export interface CreatePipelineRequest {
  name: string; environment: string;
  source: EndpointInput; destination: EndpointInput;
  schedule: string; streams: StreamInput[];
}
export interface CreatedPipeline { id: number; name: string; slug: string; /* … */ }
api.createPipeline = (req) => post<CreatedPipeline>("/pipelines", req);
```

### Mutation hook (`lib/mutations.ts`)

```ts
export function useCreatePipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreatePipelineRequest) => api.createPipeline(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipelines"] }),
  });
}
```

### Form primitives (`components/ui.tsx`)

`Field({label, error, hint, children})` renders a label, the control, an optional
hint, and an error line (danger token). `Input`, `Select`, `Textarea` are thin
token-styled wrappers (`border-line bg-surface focus:border-signal rounded-control
h-9`), forwarding native props, with an `invalid` styling hook.

### Connector catalog (in `pages/CreatePipeline.tsx`)

A small static catalog drives the pickers and per-type fields — honest about
what ships:

```ts
const SOURCES = [
  { type: "postgres", label: "PostgreSQL", badge: "Certified", ready: true,
    fields: ["host","port","database","user","password"] },
  { type: "sqlite", label: "SQLite", badge: "Beta", ready: true, fields: ["path"] },
  { type: "mysql", label: "MySQL", badge: "Coming soon", ready: false, fields: [] },
  // …others: ready:false, disabled in the picker
];
const DESTINATIONS = [
  { type: "parquet", label: "Parquet", ready: true, fields: ["path"] },
  { type: "ndjson", label: "NDJSON", ready: true, fields: ["path"] },
];
```

Only `ready` types are selectable; the rest render disabled with a "Coming soon"
badge (the honest-matrix principle, in the UI).

### The wizard (`pages/CreatePipeline.tsx`)

Local state: `step` (0..3), `name`, `environment` (default "dev"), `schedule`
(default "@daily"), `source {type, settings}`, `destination {type, settings}`,
`streams: StreamInput[]`.

- **Step 0 — Source:** name + environment + schedule; a source-type picker (cards
  with maturity badges); the chosen type's fields as `Input`s writing into
  `source.settings`. `password` uses `type=password`.
- **Step 1 — Destination:** destination-type picker + its fields (path).
- **Step 2 — Streams:** a list editor — each row `name` (`namespace.name`
  placeholder), `mode` (`Select`: full_load / incremental / cdc), and a
  `cursor_field` `Input` revealed only for incremental. Add/remove rows.
- **Step 3 — Review:** a read-only summary of everything → **Create** button.

A progress rail (4 labeled dots/segments) shows position. **Back**/**Next**
gate on per-step validity; **Create** calls `useCreatePipeline` and on success
`navigate('/pipelines/' + created.id)`. Mutation error renders a form-level
danger banner; the raw server message (e.g. "stream X: incremental mode requires
a cursor_field") is shown verbatim since the backend already writes human copy.

### Validation (`validateStep`, pure)

Mirrors the backend so the user gets inline feedback before submit:
- Step 0: `name` non-empty; a `ready` source type chosen; its required fields
  non-empty.
- Step 1: a `ready` destination type; its fields non-empty.
- Step 2: ≥1 stream; every stream has a name and a mode; incremental ⇒ cursor.
- Step 3: always valid (submit).

`validateStep(step, state) → Record<field,string>` returns per-field errors;
`Next`/`Create` are disabled when the current step has any.

## Testing / gates

Frontend gate (in `make check`): `oxlint` clean, strict `tsc` (no `any`), `vite
build` succeeds. Wizard reducer/validation kept as pure functions so they are
trivially correct and could take unit tests later. Manual pass: the wizard
renders, steps gate correctly, and a Postgres/SQLite pipeline submits.

## Rollout

Commits on `main` (per founder request): (1) write helpers + form primitives +
mutation hook; (2) the wizard page + route + CTA. `make check` green after each.
`PROGRESS.md` (local) advances 4.16b/4.16g: create-pipeline wizard shipped,
consuming the B1 API.
