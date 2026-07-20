-- LakeSense control-plane schema, v1.
-- One comprehensive init migration. All ids are bigint identity; JSONB holds
-- connector-specific and evolving payloads. Timestamps are timestamptz (UTC).
-- The `events` table is the landing zone for the engine's JSONL event stream
-- (engine/internal/events envelope) — everything else is derived from it.

-- ---------------------------------------------------------------------------
-- Environments & pipelines
-- ---------------------------------------------------------------------------
CREATE TABLE environments (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT        NOT NULL,
    slug        TEXT        NOT NULL UNIQUE,
    kind        TEXT        NOT NULL DEFAULT 'dev' CHECK (kind IN ('dev', 'staging', 'prod')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipelines (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    environment_id       BIGINT      NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    name                 TEXT        NOT NULL,
    slug                 TEXT        NOT NULL,
    source_type          TEXT        NOT NULL,
    source_config        JSONB       NOT NULL DEFAULT '{}',
    destination_type     TEXT        NOT NULL DEFAULT 'ndjson',
    destination_config   JSONB       NOT NULL DEFAULT '{}',
    catalog              JSONB       NOT NULL DEFAULT '{}',
    schedule             TEXT        NOT NULL DEFAULT '',   -- cron or '' for manual
    status               TEXT        NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active', 'paused', 'archived')),
    current_version      INT         NOT NULL DEFAULT 0,
    last_sync_at         TIMESTAMPTZ,
    health_score         INT,                               -- 0..100, computed
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, slug)
);
CREATE INDEX idx_pipelines_env ON pipelines(environment_id);

CREATE TABLE pipeline_config_versions (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id  BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version      INT         NOT NULL,
    yaml         TEXT        NOT NULL,
    config       JSONB       NOT NULL,
    note         TEXT        NOT NULL DEFAULT '',
    created_by   TEXT        NOT NULL DEFAULT 'system',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version)
);

-- ---------------------------------------------------------------------------
-- Engine event stream & derived metrics
-- ---------------------------------------------------------------------------
CREATE TABLE events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id  BIGINT      REFERENCES pipelines(id) ON DELETE CASCADE,
    sync_id      TEXT        NOT NULL DEFAULT '',
    ts           TIMESTAMPTZ NOT NULL,
    kind         TEXT        NOT NULL,
    stream       TEXT        NOT NULL DEFAULT '',
    payload      JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_events_pipeline_ts ON events(pipeline_id, ts DESC);
CREATE INDEX idx_events_kind        ON events(kind);
CREATE INDEX idx_events_sync        ON events(sync_id);

CREATE TABLE metrics (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id      BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    stream           TEXT        NOT NULL DEFAULT '',
    sync_id          TEXT        NOT NULL DEFAULT '',
    ts               TIMESTAMPTZ NOT NULL,
    rows_read        BIGINT      NOT NULL DEFAULT 0,
    rows_written     BIGINT      NOT NULL DEFAULT 0,
    bytes_written    BIGINT      NOT NULL DEFAULT 0,
    duration_seconds DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE INDEX idx_metrics_pipeline_ts ON metrics(pipeline_id, ts DESC);

CREATE TABLE column_stats (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id       BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    stream            TEXT        NOT NULL,
    column_name       TEXT        NOT NULL,
    sync_id           TEXT        NOT NULL DEFAULT '',
    ts                TIMESTAMPTZ NOT NULL,
    row_count         BIGINT      NOT NULL DEFAULT 0,
    null_count        BIGINT      NOT NULL DEFAULT 0,
    distinct_estimate BIGINT      NOT NULL DEFAULT 0,
    min_value         TEXT,
    max_value         TEXT,
    numeric_sketch    JSONB       NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_column_stats_lookup ON column_stats(pipeline_id, stream, column_name, ts DESC);

CREATE TABLE baselines (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id  BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    metric_key   TEXT        NOT NULL,   -- e.g. rows_per_second, duration, volume, lag
    bucket       TEXT        NOT NULL DEFAULT 'all',  -- e.g. weekday-hour bucket
    mean         DOUBLE PRECISION NOT NULL DEFAULT 0,
    stddev       DOUBLE PRECISION NOT NULL DEFAULT 0,
    ewma         DOUBLE PRECISION NOT NULL DEFAULT 0,
    median       DOUBLE PRECISION NOT NULL DEFAULT 0,
    mad          DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_count BIGINT      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, metric_key, bucket)
);

-- ---------------------------------------------------------------------------
-- Notifications: rules, channels, incidents, alerts, escalation, on-call
-- ---------------------------------------------------------------------------
CREATE TABLE channels (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT        NOT NULL,
    type        TEXT        NOT NULL CHECK (type IN ('slack', 'telegram', 'email', 'webhook')),
    config      JSONB       NOT NULL DEFAULT '{}',
    enabled     BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE escalation_policies (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT        NOT NULL,
    steps       JSONB       NOT NULL DEFAULT '[]',  -- ordered [{after_seconds, channel_ids[], oncall_schedule_id}]
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oncall_schedules (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT        NOT NULL,
    rotation    JSONB       NOT NULL DEFAULT '[]',  -- weekly rotation of responders
    overrides   JSONB       NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rules (
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id           BIGINT      REFERENCES pipelines(id) ON DELETE CASCADE, -- NULL = global
    stream                TEXT        NOT NULL DEFAULT '',                          -- '' = all streams
    name                  TEXT        NOT NULL,
    condition             JSONB       NOT NULL,   -- {event, field, op, value} predicate tree
    severity              TEXT        NOT NULL DEFAULT 'warning'
                           CHECK (severity IN ('info', 'warning', 'critical')),
    channel_ids           BIGINT[]    NOT NULL DEFAULT '{}',
    escalation_policy_id  BIGINT      REFERENCES escalation_policies(id) ON DELETE SET NULL,
    enabled               BOOLEAN     NOT NULL DEFAULT true,
    dedup_window_seconds  INT         NOT NULL DEFAULT 300,
    quiet_hours           JSONB       NOT NULL DEFAULT '{}',  -- {tz, ranges:[{start,end}]}
    maintenance_until     TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_rules_pipeline ON rules(pipeline_id);

CREATE TABLE incidents (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id      BIGINT      REFERENCES pipelines(id) ON DELETE CASCADE,
    title            TEXT        NOT NULL,
    severity         TEXT        NOT NULL DEFAULT 'warning'
                      CHECK (severity IN ('info', 'warning', 'critical')),
    status           TEXT        NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'acked', 'snoozed', 'resolved')),
    fingerprint      TEXT        NOT NULL,   -- dedup key: one incident = one alert thread
    correlation_key  TEXT        NOT NULL DEFAULT '', -- storm-suppression grouping
    event_count      INT         NOT NULL DEFAULT 1,
    summary          TEXT        NOT NULL DEFAULT '',
    root_cause       TEXT        NOT NULL DEFAULT '',
    suggested_fix    TEXT        NOT NULL DEFAULT '',
    postmortem       TEXT        NOT NULL DEFAULT '',
    escalation_step  INT         NOT NULL DEFAULT 0,
    escalation_policy_id BIGINT   REFERENCES escalation_policies(id) ON DELETE SET NULL,
    next_escalation_at TIMESTAMPTZ,
    snoozed_until    TIMESTAMPTZ,
    opened_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acked_at         TIMESTAMPTZ,
    acked_by         TEXT,
    resolved_at      TIMESTAMPTZ
);
CREATE UNIQUE INDEX idx_incidents_open_fingerprint
    ON incidents(fingerprint) WHERE status IN ('open', 'acked', 'snoozed');
CREATE INDEX idx_incidents_status ON incidents(status);
CREATE INDEX idx_incidents_pipeline ON incidents(pipeline_id);

CREATE TABLE alerts (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id  BIGINT      NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    rule_id      BIGINT      REFERENCES rules(id) ON DELETE SET NULL,
    channel_id   BIGINT      REFERENCES channels(id) ON DELETE SET NULL,
    status       TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'sent', 'failed')),
    payload      JSONB       NOT NULL DEFAULT '{}',
    error        TEXT        NOT NULL DEFAULT '',
    sent_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_alerts_incident ON alerts(incident_id);

CREATE TABLE acks (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id  BIGINT      NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    actor        TEXT        NOT NULL,
    action       TEXT        NOT NULL CHECK (action IN ('ack', 'snooze', 'resolve', 'reopen')),
    note         TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Data-diff / verification
-- ---------------------------------------------------------------------------
CREATE TABLE diff_runs (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id      BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    stream           TEXT        NOT NULL,
    sync_id          TEXT        NOT NULL DEFAULT '',
    kind             TEXT        NOT NULL DEFAULT 'sync' CHECK (kind IN ('sync', 'verify')),
    source_rows      BIGINT      NOT NULL DEFAULT 0,
    dest_rows        BIGINT      NOT NULL DEFAULT 0,
    source_checksum  TEXT        NOT NULL DEFAULT '',
    dest_checksum    TEXT        NOT NULL DEFAULT '',
    match            BOOLEAN     NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_diff_runs_pipeline ON diff_runs(pipeline_id, created_at DESC);

CREATE TABLE diff_findings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    diff_run_id  BIGINT      NOT NULL REFERENCES diff_runs(id) ON DELETE CASCADE,
    range_min    TEXT        NOT NULL DEFAULT '',
    range_max    TEXT        NOT NULL DEFAULT '',
    sample_pks   JSONB       NOT NULL DEFAULT '[]',
    detail       TEXT        NOT NULL DEFAULT ''
);

-- ---------------------------------------------------------------------------
-- Lineage
-- ---------------------------------------------------------------------------
CREATE TABLE lineage_edges (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id   BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    source_stream TEXT        NOT NULL,
    source_column TEXT        NOT NULL,
    source_type   TEXT        NOT NULL DEFAULT '',
    dest_table    TEXT        NOT NULL,
    dest_column   TEXT        NOT NULL,
    dest_type     TEXT        NOT NULL DEFAULT '',
    sync_id       TEXT        NOT NULL DEFAULT '',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, source_stream, source_column, dest_column)
);

-- ---------------------------------------------------------------------------
-- Data-quality monitors
-- ---------------------------------------------------------------------------
CREATE TABLE quality_monitors (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id  BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    stream       TEXT        NOT NULL,
    column_name  TEXT        NOT NULL DEFAULT '',   -- '' for table-level monitors
    kind         TEXT        NOT NULL CHECK (kind IN ('freshness', 'volume', 'null_rate', 'distribution')),
    config       JSONB       NOT NULL DEFAULT '{}',
    baseline     JSONB       NOT NULL DEFAULT '{}',
    enabled      BOOLEAN     NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_quality_monitors_pipeline ON quality_monitors(pipeline_id);

CREATE TABLE quality_results (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    monitor_id   BIGINT      NOT NULL REFERENCES quality_monitors(id) ON DELETE CASCADE,
    sync_id      TEXT        NOT NULL DEFAULT '',
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    breached     BOOLEAN     NOT NULL DEFAULT false,
    detail       TEXT        NOT NULL DEFAULT ''
);
CREATE INDEX idx_quality_results_monitor ON quality_results(monitor_id, ts DESC);

-- ---------------------------------------------------------------------------
-- Audit log & backfills
-- ---------------------------------------------------------------------------
CREATE TABLE audit_log (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor        TEXT        NOT NULL DEFAULT 'system',
    action       TEXT        NOT NULL,
    entity_type  TEXT        NOT NULL,
    entity_id    TEXT        NOT NULL DEFAULT '',
    before       JSONB       NOT NULL DEFAULT '{}',
    after        JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_log_entity ON audit_log(entity_type, entity_id);
CREATE INDEX idx_audit_log_created ON audit_log(created_at DESC);

CREATE TABLE backfill_jobs (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id   BIGINT      NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    stream        TEXT        NOT NULL,
    mode          TEXT        NOT NULL CHECK (mode IN ('pk_range', 'time_window', 'changed_since')),
    params        JSONB       NOT NULL DEFAULT '{}',
    status        TEXT        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    rows          BIGINT      NOT NULL DEFAULT 0,
    diff_run_id   BIGINT      REFERENCES diff_runs(id) ON DELETE SET NULL,
    requested_by  TEXT        NOT NULL DEFAULT 'system',
    error         TEXT        NOT NULL DEFAULT '',
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_backfill_jobs_pipeline ON backfill_jobs(pipeline_id, created_at DESC);
