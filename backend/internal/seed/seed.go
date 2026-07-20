// Package seed generates a realistic multi-day engine event stream and feeds it
// through the real collector ingestion path, so every LakeSense screen is
// demoable without a live database. It is the `lakesense seed` command.
//
// The generated history deliberately includes the interesting cases: healthy
// syncs with verified checksums, a gradual slowdown, a volume-drop anomaly, a
// hard failure, a schema change, and a checksum mismatch — the shapes the
// intelligence layer exists to catch.
package seed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/backend/internal/collector"
)

// pipelineSpec describes a demo pipeline to synthesize.
type pipelineSpec struct {
	name        string
	slug        string
	sourceType  string
	destType    string
	streams     []string
	columns     map[string][]column // per stream
	baseRows    int64
	baseBytes   int64
	baseSeconds float64
}

type column struct{ name, typ string }

var demoPipelines = []pipelineSpec{
	{
		name: "Orders (PostgreSQL)", slug: "orders-postgres",
		sourceType: "postgres", destType: "iceberg",
		streams:  []string{"public.orders", "public.customers"},
		baseRows: 1_204_331, baseBytes: 512 << 20, baseSeconds: 42,
		columns: map[string][]column{
			"public.orders":    {{"id", "int64"}, {"customer_id", "int64"}, {"total", "decimal"}, {"status", "string"}, {"created_at", "timestamp"}},
			"public.customers": {{"id", "int64"}, {"email", "string"}, {"country", "string"}, {"created_at", "timestamp"}},
		},
	},
	{
		name: "Events (SQLite)", slug: "events-sqlite",
		sourceType: "sqlite", destType: "parquet",
		streams:  []string{"main.events"},
		baseRows: 88_500, baseBytes: 64 << 20, baseSeconds: 8,
		columns: map[string][]column{
			"main.events": {{"id", "int64"}, {"kind", "string"}, {"payload", "json"}, {"ts", "timestamp"}},
		},
	},
	{
		name: "Inventory (MySQL)", slug: "inventory-mysql",
		sourceType: "mysql", destType: "iceberg",
		streams:  []string{"shop.inventory"},
		baseRows: 340_900, baseBytes: 128 << 20, baseSeconds: 19,
		columns: map[string][]column{
			"shop.inventory": {{"sku", "string"}, {"warehouse", "string"}, {"qty", "int64"}, {"updated_at", "timestamp"}},
		},
	},
}

// Run seeds environments, pipelines, and `days` of synthetic history.
func Run(ctx context.Context, pool *pgxpool.Pool, days int) error {
	if days <= 0 {
		days = 14
	}
	envID, err := ensureEnvironment(ctx, pool, "Production", "prod", "prod")
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic demo data, not security
	sink := collector.NewPgSink(pool)
	ing := collector.NewIngester(sink)

	for _, spec := range demoPipelines {
		pipelineID, err := ensurePipeline(ctx, pool, envID, spec)
		if err != nil {
			return err
		}
		events := generateHistory(spec, days, rng)
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		for _, e := range events {
			if err := enc.Encode(e); err != nil {
				return fmt.Errorf("encode seed event: %w", err)
			}
		}
		if _, err := ing.Ingest(ctx, pipelineID, buf); err != nil {
			return fmt.Errorf("ingest seed for %s: %w", spec.slug, err)
		}
	}
	return nil
}

// generateHistory produces the JSONL event objects for one pipeline's history.
func generateHistory(spec pipelineSpec, days int, rng *rand.Rand) []collector.Event {
	var events []collector.Event
	start := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)

	for d := 0; d < days; d++ {
		day := start.Add(time.Duration(d) * 24 * time.Hour)
		syncID := fmt.Sprintf("%s-%03d", spec.slug, d)

		// Pick a daily profile: mostly healthy, with planted incidents.
		profile := "healthy"
		switch {
		case d == days-3:
			profile = "failure"
		case d == days-5:
			profile = "volume_drop"
		case d == days-7:
			profile = "mismatch"
		case d == days-9:
			profile = "schema_change"
		case d >= days-2:
			profile = "slowdown"
		}

		events = append(events, ev(day, "sync_started", syncID, "", map[string]any{
			"connector": spec.sourceType, "destination": spec.destType, "streams": spec.streams,
		}))

		var totalRead, totalBytes int64
		var totalSeconds float64
		failed := false

		for _, stream := range spec.streams {
			jitter := 0.9 + rng.Float64()*0.2
			rows := int64(float64(spec.baseRows) * jitter / float64(len(spec.streams)))
			bytesW := int64(float64(spec.baseBytes) * jitter / float64(len(spec.streams)))
			secs := spec.baseSeconds * jitter / float64(len(spec.streams))

			switch profile {
			case "volume_drop":
				rows /= 3 // anomaly: two-thirds of the rows vanished
			case "slowdown":
				secs *= 2.5 + rng.Float64()
			case "schema_change":
				events = append(events, ev(day, "schema_changed", syncID, stream, map[string]any{
					"added": []map[string]any{{"name": "promo_code", "type": "string", "nullable": true}},
				}))
			}

			// Column mappings feed lineage.
			for _, c := range spec.columns[stream] {
				events = append(events, ev(day, "column_mapping", syncID, stream, map[string]any{
					"source_column": c.name, "source_type": c.typ, "dest_column": c.name, "dest_type": c.typ,
				}))
			}

			if profile == "failure" {
				events = append(events, ev(day, "sync_failed", syncID, stream, map[string]any{
					"code": "connection_refused", "retryable": true,
					"message": "dial tcp " + spec.sourceType + ": connection refused after 3 retries",
				}))
				failed = true
				break
			}

			src := fmt.Sprintf("%016x", rng.Uint64())
			dst := src
			destRows := rows
			if profile == "mismatch" {
				dst = fmt.Sprintf("%016x", rng.Uint64()) // checksum diverges
				destRows = rows - 1 - int64(rng.Intn(5)) // a few rows lost
			}
			cols := colNames(spec.columns[stream])
			events = append(events,
				ev(day, "checksum_computed", syncID, stream, map[string]any{"side": "source", "rows": rows, "checksum": src, "columns": cols}),
				ev(day, "checksum_computed", syncID, stream, map[string]any{"side": "destination", "rows": destRows, "checksum": dst, "columns": cols}),
				ev(day, "stream_finished", syncID, stream, map[string]any{"mode": "cdc", "rows_read": rows, "rows_written": destRows, "bytes_written": bytesW}),
			)
			totalRead += rows
			totalBytes += bytesW
			totalSeconds += secs
		}

		if !failed {
			events = append(events, ev(day, "sync_finished", syncID, "", map[string]any{
				"rows_read": totalRead, "rows_written": totalRead,
				"bytes_written": totalBytes, "duration_seconds": round2(totalSeconds),
			}))
		}
	}
	return events
}

func ev(ts time.Time, kind, syncID, stream string, payload map[string]any) collector.Event {
	raw, _ := json.Marshal(payload)
	return collector.Event{V: 1, TS: ts, Kind: kind, SyncID: syncID, Stream: stream, Payload: raw}
}

func colNames(cols []column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.name
	}
	return out
}

func round2(f float64) float64 { return float64(int64(f*100)) / 100 }
