import { useEffect, useRef, useState, type ReactNode } from "react";
import { useReveal } from "./lib/useReveal";

// CountUp animates a number from 0 → value once, when scrolled into view.
// Respects reduced-motion (shows the final value immediately). It stops after
// one run — no perpetual animation.
function CountUp({ value, decimals = 0, prefix = "", suffix = "" }: { value: number; decimals?: number; prefix?: string; suffix?: string }) {
  const [n, setN] = useState(0);
  const ref = useRef<HTMLSpanElement>(null);
  const done = useRef(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) {
      setN(value);
      return;
    }
    const obs = new IntersectionObserver((entries) => {
      for (const e of entries) {
        if (e.isIntersecting && !done.current) {
          done.current = true;
          const start = performance.now();
          const dur = 1200;
          const tick = (t: number) => {
            const p = Math.min((t - start) / dur, 1);
            const eased = 1 - Math.pow(1 - p, 3);
            setN(value * eased);
            if (p < 1) requestAnimationFrame(tick);
          };
          requestAnimationFrame(tick);
          obs.unobserve(e.target);
        }
      }
    }, { threshold: 0.4 });
    obs.observe(el);
    return () => obs.disconnect();
  }, [value]);
  return (
    <span ref={ref} className="tnum">
      {prefix}
      {n.toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}
      {suffix}
    </span>
  );
}

// ── Benchmarks (real, measured numbers) ─────────────────────────────────────
export function Benchmarks() {
  const ref = useReveal<HTMLElement>();
  return (
    <section id="benchmarks" ref={ref} className="reveal mx-auto max-w-6xl px-6 py-24">
      <div className="mb-3 font-mono text-xs uppercase tracking-[0.2em] text-aqua">Measured, not borrowed</div>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">
        <span className="text-aqua">~5.9 million</span> rows a minute.
      </h2>
      <p className="mt-3 max-w-2xl text-muted">
        A full-load migration of a 1,000,000-row table, timed by the engine's own
        checksummed accounting. Your hardware, your numbers — reproduce every one.
      </p>
      <div className="mt-10 grid gap-4 sm:grid-cols-3">
        <div className="glass rounded-2xl p-6">
          <div className="text-3xl font-semibold text-aqua md:text-4xl">
            <CountUp value={98278} suffix="" />
          </div>
          <div className="mt-1 text-sm text-muted">rows / second (SQLite full load)</div>
        </div>
        <div className="glass rounded-2xl p-6">
          <div className="text-3xl font-semibold text-aqua md:text-4xl">
            <CountUp value={37.7} decimals={1} suffix=" MB/s" />
          </div>
          <div className="mt-1 text-sm text-muted">sustained to disk, verified</div>
        </div>
        <div className="glass rounded-2xl p-6">
          <div className="text-3xl font-semibold text-aqua md:text-4xl">
            <CountUp value={10.2} decimals={1} suffix="s" />
          </div>
          <div className="mt-1 text-sm text-muted">to migrate 1M rows end-to-end</div>
        </div>
      </div>
      <p className="mt-6 font-mono text-xs text-faint">
        Measured on a 20-core box, NDJSON writer, keyset-chunked · reproduce with
        <span className="text-muted"> scripts/benchmark.sh</span> · Parquet output ships today;
        parallel readers (v0.2) go faster. We never cite anyone else's benchmarks as ours.
      </p>
    </section>
  );
}

function Section({ id, children, className = "" }: { id?: string; children: ReactNode; className?: string }) {
  const ref = useReveal<HTMLElement>();
  return (
    <section id={id} ref={ref} className={`reveal mx-auto max-w-6xl px-6 py-24 ${className}`}>
      {children}
    </section>
  );
}

function Eyebrow({ children }: { children: ReactNode }) {
  return <div className="mb-3 font-mono text-xs uppercase tracking-[0.2em] text-aqua">{children}</div>;
}

// ── Problem ───────────────────────────────────────────────────────────────
export function Problem() {
  return (
    <Section>
      <Eyebrow>The problem</Eyebrow>
      <h2 className="max-w-3xl font-display text-3xl font-semibold leading-tight md:text-4xl">
        You found out from a stakeholder. Again.
      </h2>
      <p className="mt-4 max-w-2xl text-lg text-muted">
        A pipeline half-loaded overnight. A column silently went null. A source drifted. Your
        warehouse looked fine — until someone downstream noticed the numbers were wrong. The tools
        that catch this cost more than the pipeline itself, and live in three other vendors' dashboards.
      </p>
    </Section>
  );
}

// ── The Paywall Buster ──────────────────────────────────────────────────────
const paywall = [
  ["Data-diff validation", "Datafold charges for it", "checksums on every sync"],
  ["Escalation & on-call", "PagerDuty charges for it", "policies + rotations, free"],
  ["Data-quality monitors", "Monte Carlo's core product", "freshness · volume · drift"],
  ["Column-level lineage", "Atlan / Monte Carlo charge", "source→dest, per column"],
  ["Cost & volume analytics", "Fivetran, and opaque", "transparent model, free"],
  ["Audit logs", "enterprise tier, everywhere", "append-only, free"],
];

export function PaywallBuster() {
  return (
    <Section id="wedge">
      <Eyebrow>The wedge</Eyebrow>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">
        Free what the industry <span className="text-aqua">paywalls</span>.
      </h2>
      <p className="mt-3 max-w-2xl text-muted">
        The features that build trust — validation, observability, on-call — shipped in one open tool
        instead of sold across four.
      </p>
      <div className="mt-10 grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {paywall.map(([feature, who, ls]) => (
          <div key={feature} className="glass rounded-2xl p-5">
            <div className="font-display text-lg font-medium">{feature}</div>
            <div className="mt-3 flex items-center gap-2 text-sm text-faint line-through decoration-faint/60">
              {who}
            </div>
            <div className="mt-1 flex items-center gap-2 text-sm text-aqua">
              <span className="tnum">✓</span> {ls}
            </div>
          </div>
        ))}
      </div>
    </Section>
  );
}

// ── Sources band ────────────────────────────────────────────────────────────
// Shipping today vs on the honest roadmap — the site says exactly what the
// product does, never more (the connector-honesty principle, on the marketing).
const shippingSources: { name: string; maturity: "Certified" | "Stable" | "Beta" }[] = [
  { name: "PostgreSQL", maturity: "Certified" },
  { name: "MySQL", maturity: "Certified" },
  { name: "SQLite", maturity: "Beta" },
];
const roadmapSources = [
  "MariaDB", "Aurora", "CockroachDB", "TimescaleDB", "YugabyteDB", "AlloyDB", "Percona", "TiDB",
  "MongoDB", "SQL Server", "Kafka", "Oracle", "DB2", "ClickHouse", "Cassandra", "ScyllaDB",
  "DynamoDB", "Elasticsearch", "OpenSearch", "Redis", "S3", "GCS", "Azure Blob", "MinIO",
];

export function Sources() {
  return (
    <Section id="sources">
      <Eyebrow>Shipping today, honestly badged</Eyebrow>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">
        Three sources ship now. <span className="text-aqua">25+</span> on the roadmap.
      </h2>
      <p className="mt-3 max-w-2xl text-muted">
        The connector SDK, one event schema, and inherited checksums make the family big — but we badge
        every source by the test battery it actually passes. A transparent matrix beats a matrix of lies.
      </p>

      <div className="mt-9 text-xs font-mono uppercase tracking-widest text-aqua">Shipping</div>
      <div className="mt-3 flex flex-wrap gap-2.5">
        {shippingSources.map((s) => (
          <span key={s.name} className="inline-flex items-center gap-2 rounded-full border border-aqua/50 bg-aqua/10 px-3.5 py-1.5 font-mono text-sm text-aqua">
            <span aria-hidden>✓</span> {s.name}
            <span className="rounded-full bg-aqua/15 px-1.5 text-[10px] uppercase tracking-wide">{s.maturity}</span>
          </span>
        ))}
      </div>

      <div className="mt-7 text-xs font-mono uppercase tracking-widest text-faint">On the roadmap</div>
      <div className="mt-3 flex flex-wrap gap-2.5">
        {roadmapSources.map((s) => (
          <span key={s} className="rounded-full border border-white/10 bg-white/[0.02] px-3.5 py-1.5 font-mono text-sm text-faint">
            {s} <span className="ml-1 text-[10px] text-faint/70">soon</span>
          </span>
        ))}
      </div>
    </Section>
  );
}

// ── Showcase (real product screenshots in tilted frames) ───────────────────
export function Showcase() {
  return (
    <Section id="product">
      <Eyebrow>The product</Eyebrow>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">
        Every sync ships with proof it's correct.
      </h2>
      <div className="mt-12 grid items-center gap-8 md:grid-cols-2">
        <Frame src={`${import.meta.env.BASE_URL}shots/dashboard.jpg`} alt="LakeSense dashboard — fleet health and verified badges" tilt="-4deg" />
        <div>
          <h3 className="font-display text-xl font-medium">A fleet that senses itself</h3>
          <p className="mt-2 text-muted">
            Composite health scores, live incident feed, and the aqua “✓ verified” badge on every
            pipeline — the correctness signal is the hero, not a footnote.
          </p>
        </div>
        <div className="md:order-2">
          <h3 className="font-display text-xl font-medium">Proof, not vibes</h3>
          <p className="mt-2 text-muted">
            Order-independent checksums on both the source read and the destination write. A drop or
            corruption anywhere shows up as a mismatch — the Datafold feature, free, on every run.
          </p>
        </div>
        <Frame src={`${import.meta.env.BASE_URL}shots/diff.jpg`} alt="LakeSense data-diff correctness board" tilt="4deg" className="md:order-1" />
      </div>
    </Section>
  );
}

function Frame({ src, alt, tilt, className = "" }: { src: string; alt: string; tilt: string; className?: string }) {
  return (
    <div
      className={`glass overflow-hidden rounded-2xl p-1.5 shadow-2xl shadow-aqua/10 ${className}`}
      style={{ transform: `perspective(1200px) rotateY(${tilt})` }}
    >
      <img src={src} alt={alt} loading="lazy" className="w-full rounded-xl border border-white/5" />
    </div>
  );
}

// ── Architecture ────────────────────────────────────────────────────────────
export function Architecture() {
  const steps = [
    ["Sources", "Postgres · SQLite +"],
    ["lsengine", "chunk · CDC · checksum"],
    ["Collector", "JSONL → event store"],
    ["Intelligence", "rules · escalation · anomaly · quality"],
    ["You", "alerted, with proof"],
  ];
  return (
    <Section id="architecture">
      <Eyebrow>Architecture</Eyebrow>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">A Go engine, an intelligence layer.</h2>
      <div className="mt-12 flex flex-col items-stretch gap-3 md:flex-row md:items-center">
        {steps.map(([t, s], i) => (
          <div key={t} className="flex flex-1 items-center gap-3">
            <div className="glass flex-1 rounded-xl px-4 py-5 text-center">
              <div className="font-display font-medium">{t}</div>
              <div className="mt-1 font-mono text-xs text-faint">{s}</div>
            </div>
            {i < steps.length - 1 && <div className="hidden text-aqua md:block">→</div>}
          </div>
        ))}
      </div>
    </Section>
  );
}

// ── Pricing ─────────────────────────────────────────────────────────────────
const tiers = [
  {
    name: "Free",
    price: "$0",
    sub: "open-core, self-hosted",
    highlight: true,
    features: ["The entire product", "Engine + CDC + data-diff", "Escalation, anomaly, quality", "Lineage, cost, audit, backfills"],
  },
  {
    name: "Pro",
    price: "per seat",
    sub: "the open-core line",
    features: ["SSO & RBAC", "Advanced SLA reports", "Priority support"],
  },
  {
    name: "Enterprise",
    price: "annual",
    sub: "for scale",
    features: ["Compliance packs", "Dedicated support", "Managed SaaS control plane"],
  },
];

export function Pricing() {
  return (
    <Section id="pricing">
      <Eyebrow>Pricing</Eyebrow>
      <h2 className="font-display text-3xl font-semibold md:text-4xl">We give away the enterprise tier.</h2>
      <div className="mt-10 grid gap-5 md:grid-cols-3">
        {tiers.map((t) => (
          <div
            key={t.name}
            className={`glass rounded-2xl p-6 ${t.highlight ? "ring-1 ring-aqua/50" : ""}`}
          >
            {t.highlight && <div className="mb-2 inline-block rounded-full bg-aqua/15 px-2 py-0.5 font-mono text-[11px] text-aqua">EVERYTHING, FREE</div>}
            <div className="font-display text-xl font-semibold">{t.name}</div>
            <div className="mt-1 tnum text-3xl">{t.price}</div>
            <div className="text-sm text-faint">{t.sub}</div>
            <ul className="mt-5 space-y-2 text-sm text-muted">
              {t.features.map((f) => (
                <li key={f} className="flex gap-2">
                  <span className="text-aqua">✓</span> {f}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </Section>
  );
}

// ── FAQ ─────────────────────────────────────────────────────────────────────
const faqs: [string, string][] = [
  ["Is it really free?", "Yes. The entire current product is Apache-2.0 open-core. SSO/RBAC and compliance packs are reserved for a future Pro tier — the only things behind the line."],
  ["How is the engine different?", "It's a clean Go reimplementation — no JVM. Parallel-chunked loads, logical-replication CDC, and order-independent checksums on both sides of every sync, so each copy proves it's correct."],
  ["What about lock-in?", "Data lands in open lakehouse formats (Parquet, Iceberg). The control plane is one Go binary plus Postgres you run yourself."],
  ["Which sources work today?", "PostgreSQL (full + CDC) and SQLite ship now. The rest carry honest Beta / Coming-soon badges until they pass their test battery — a transparent matrix beats a matrix of lies."],
];

export function FAQ() {
  return (
    <Section id="faq">
      <Eyebrow>FAQ</Eyebrow>
      <div className="grid gap-6 md:grid-cols-2">
        {faqs.map(([q, a]) => (
          <div key={q}>
            <h3 className="font-display text-lg font-medium">{q}</h3>
            <p className="mt-2 text-muted">{a}</p>
          </div>
        ))}
      </div>
    </Section>
  );
}

// ── Footer ──────────────────────────────────────────────────────────────────
export function Footer() {
  return (
    <footer className="border-t border-white/10 px-6 py-12 text-center">
      <div className="mx-auto max-w-6xl">
        <div className="font-display text-lg font-semibold">
          Lake<span className="text-aqua">Sense</span>
        </div>
        <p className="mx-auto mt-3 max-w-xl text-xs leading-relaxed text-faint">
          LakeSense's engine architecture was informed by studying open-source projects including
          OLake (Apache 2.0). LakeSense is a clean reimplementation — no upstream code is copied.
          Apache-2.0 · open-core.
        </p>
      </div>
    </footer>
  );
}
