# LakeSense — Business Narrative

## The one-liner

LakeSense replicates 25+ sources into open lakehouse formats with its own
Go-native engine, proves the data is correct, tells the right person when it
isn't, and gives away free what Fivetran, Monte Carlo, and PagerDuty charge for.

## Target user

The **data engineer or small data team** who has outgrown cron-and-scripts but
can't justify the combined bill of Fivetran (ingestion) + Monte Carlo
(observability) + Datafold (validation) + PagerDuty (on-call) — often
$100k+/year across four vendors. They live in dark dashboards, value correctness
and density over polish, and want to own their stack (open formats, no lock-in).

## The wedge — "we give away the enterprise tier"

The industry paywalls the features that build trust: validation, lineage,
quality monitors, on-call, cost transparency, audit. LakeSense ships all of them
free, in one tool, on open lakehouse formats. The differentiation isn't "cheaper
Fivetran" — it's **the pipeline that proves itself correct and tells you when it
isn't**, with the observability suite included rather than sold separately.

Every sync carries an order-independent checksum on both the source read and the
destination write. That single capability — free here, a paid product at
Datafold — is the credibility anchor the rest of the story hangs on.

## Pricing

| Tier | Price | What's included |
|---|---|---|
| **Free** (open-core, Apache-2.0) | $0, self-hosted | The entire current product: engine, CDC, data-diff, escalation & on-call, anomaly detection, quality monitors, lineage, cost analytics, audit log, config versioning, environments, backfills. |
| **Pro** | per-seat / per-workspace | **SSO & RBAC**, advanced SLA reporting, priority support. The open-core line — features enterprises require but individuals don't. |
| **Enterprise** | annual | Compliance packs (SOC2/HIPAA evidence), dedicated support, managed SaaS control plane. |

The Free tier is deliberately generous — it's the top of the funnel and the
marketing. Monetization comes from the org-level features (SSO/RBAC/compliance)
that only matter once a team has adopted and scaled on the free product.

## Competitor landscape

| | Ingestion | Validation | Observability | On-call | Open formats | Self-host |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **LakeSense** | ✅ | ✅ free | ✅ free | ✅ free | ✅ | ✅ |
| OLake | ✅ | — | — | — | ✅ | ✅ |
| Airbyte | ✅ | — | partial | — | ✅ | ✅ |
| Fivetran | ✅ | — | paid | — | partial | ❌ |
| Monte Carlo | — | — | ✅ paid | — | — | ❌ |
| Datafold | — | ✅ paid | partial | — | — | ❌ |
| PagerDuty | — | — | — | ✅ paid | — | ❌ |

LakeSense's bet: the buyer who currently stitches 3–4 of these together will
prefer one open tool that does the connected job — and correctness/observability
being *free and built-in* is what makes switching worth it.

## 6-month roadmap {#roadmap}

- **v0.1 (now):** PostgreSQL (full + CDC) & SQLite connectors, the full
  intelligence layer, dockerized deploy, dashboard, verification suite + CI.
- **v0.2:** MySQL Tier-A CDC; MongoDB/MSSQL/Kafka (Tier B); Parquet + append-mode
  Iceberg writers; the write-path UI (create-pipeline wizard, rules builder,
  backfill launcher); reverse-direction sources (Snowflake/BigQuery/Redshift).
- **v0.3:** Tier-C connector factory (ClickHouse, DynamoDB, Elasticsearch,
  Object Storage, …); CDC upgrades (Oracle LogMiner, DynamoDB Streams,
  CockroachDB changefeeds); Helm chart.
- **Pro (parallel):** SSO/RBAC, SLA reports, SaaS control plane.

Community demand steers connector order — the docs site carries a public roadmap
with a "Request a connector" issue template, the strongest signal a solo founder
has for what to build next.

## Why now

Open lakehouse formats (Iceberg) have won the destination war; the ingestion +
observability layer above them is still fragmented and expensive. A single,
open, correctness-first platform is a credible wedge into a market that's paying
four vendors for one job.
