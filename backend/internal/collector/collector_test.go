package collector

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSink records calls so ingestion logic can be asserted without a database.
type fakeSink struct {
	events   int
	metrics  []SyncFinished
	diffs    []DiffRun
	lineage  int
	synced   int
	failNext bool
}

func (f *fakeSink) InsertEvent(_ context.Context, _ int64, _ Event) error {
	if f.failNext {
		return assertErr
	}
	f.events++
	return nil
}
func (f *fakeSink) RecordMetric(_ context.Context, _ int64, _ string, m SyncFinished, _ time.Time) error {
	f.metrics = append(f.metrics, m)
	return nil
}
func (f *fakeSink) UpsertDiffRun(_ context.Context, _ int64, _, _ string, d DiffRun) error {
	f.diffs = append(f.diffs, d)
	return nil
}
func (f *fakeSink) RecordLineage(_ context.Context, _ int64, _ string, _ ColumnMapping, _ string) error {
	f.lineage++
	return nil
}
func (f *fakeSink) MarkSynced(_ context.Context, _ int64, _ Event) error { f.synced++; return nil }

var assertErr = errTest("boom")

type errTest string

func (e errTest) Error() string { return string(e) }

const matchingStream = `
{"v":1,"ts":"2026-01-01T00:00:00Z","event":"sync_started","sync_id":"s1","payload":{}}
{"v":1,"ts":"2026-01-01T00:00:01Z","event":"checksum_computed","sync_id":"s1","stream":"public.a","payload":{"side":"source","rows":100,"checksum":"abc"}}
{"v":1,"ts":"2026-01-01T00:00:02Z","event":"checksum_computed","sync_id":"s1","stream":"public.a","payload":{"side":"destination","rows":100,"checksum":"abc"}}
{"v":1,"ts":"2026-01-01T00:00:03Z","event":"column_mapping","sync_id":"s1","stream":"public.a","payload":{"source_column":"id","source_type":"int64","dest_column":"id","dest_type":"int64"}}
{"v":1,"ts":"2026-01-01T00:00:04Z","event":"sync_finished","sync_id":"s1","payload":{"rows_read":100,"rows_written":100,"bytes_written":2048,"duration_seconds":1.5}}
`

func TestIngestDerivesMatchingDiffAndMetric(t *testing.T) {
	f := &fakeSink{}
	n, err := NewIngester(f).Ingest(context.Background(), 7, strings.NewReader(matchingStream))
	require.NoError(t, err)
	assert.Equal(t, 5, n, "all five events stored")
	assert.Equal(t, 5, f.events)

	require.Len(t, f.diffs, 1)
	assert.True(t, f.diffs[0].Match, "equal rows+checksum ⇒ verified")
	assert.Equal(t, int64(100), f.diffs[0].SourceRows)

	require.Len(t, f.metrics, 1)
	assert.Equal(t, int64(100), f.metrics[0].RowsWritten)
	assert.InDelta(t, 1.5, f.metrics[0].DurationSeconds, 1e-9)

	assert.Equal(t, 1, f.lineage)
	assert.Equal(t, 1, f.synced)
}

func TestIngestFlagsMismatchedChecksum(t *testing.T) {
	const mismatched = `
{"v":1,"ts":"2026-01-01T00:00:01Z","event":"checksum_computed","sync_id":"s2","stream":"public.b","payload":{"side":"source","rows":100,"checksum":"abc"}}
{"v":1,"ts":"2026-01-01T00:00:02Z","event":"checksum_computed","sync_id":"s2","stream":"public.b","payload":{"side":"destination","rows":99,"checksum":"xyz"}}
`
	f := &fakeSink{}
	_, err := NewIngester(f).Ingest(context.Background(), 1, strings.NewReader(mismatched))
	require.NoError(t, err)
	require.Len(t, f.diffs, 1)
	assert.False(t, f.diffs[0].Match, "unequal rows/checksum ⇒ NOT verified")
}

func TestIngestToleratesMalformedLine(t *testing.T) {
	const withGarbage = `
{"v":1,"ts":"2026-01-01T00:00:01Z","event":"sync_started","sync_id":"s3","payload":{}}
this is not json
{"v":1,"ts":"2026-01-01T00:00:04Z","event":"sync_finished","sync_id":"s3","payload":{"rows_read":1,"rows_written":1}}
`
	f := &fakeSink{}
	n, err := NewIngester(f).Ingest(context.Background(), 1, strings.NewReader(withGarbage))
	require.NoError(t, err)
	assert.Equal(t, 2, n, "the two valid events are stored; the garbage line is skipped")
	assert.Len(t, f.metrics, 1)
}

func TestIngestPropagatesSinkError(t *testing.T) {
	f := &fakeSink{failNext: true}
	_, err := NewIngester(f).Ingest(context.Background(), 1,
		strings.NewReader(`{"v":1,"ts":"2026-01-01T00:00:01Z","event":"sync_started","sync_id":"s","payload":{}}`+"\n"))
	require.Error(t, err)
}
