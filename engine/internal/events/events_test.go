package events

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitterWritesEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		kind    Kind
		stream  string
		payload any
		check   func(t *testing.T, line map[string]any)
	}{
		{
			name:   "sync_started with payload",
			kind:   KindSyncStarted,
			stream: "",
			payload: SyncStarted{
				Connector:   "postgres",
				Destination: "parquet",
				Streams:     []string{"public.users"},
			},
			check: func(t *testing.T, line map[string]any) {
				payload, ok := line["payload"].(map[string]any)
				require.True(t, ok, "payload must be an object")
				assert.Equal(t, "postgres", payload["connector"])
				assert.Equal(t, []any{"public.users"}, payload["streams"])
			},
		},
		{
			name:   "stream-scoped event carries stream",
			kind:   KindChunkCompleted,
			stream: "public.orders",
			payload: ChunkCompleted{
				ChunkMin: "1", ChunkMax: "1000", Rows: 1000, Remaining: 3,
			},
			check: func(t *testing.T, line map[string]any) {
				assert.Equal(t, "public.orders", line["stream"])
			},
		},
		{
			name:    "no payload omits field",
			kind:    KindSyncFinished,
			stream:  "",
			payload: nil,
			check: func(t *testing.T, line map[string]any) {
				_, present := line["payload"]
				assert.False(t, present, "nil payload must be omitted")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			em := NewEmitter(&buf, "sync-123", "pipe-1")
			em.now = func() time.Time { return time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC) }

			require.NoError(t, em.Emit(tt.kind, tt.stream, tt.payload))

			var line map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
			assert.Equal(t, float64(SchemaVersion), line["v"])
			assert.Equal(t, string(tt.kind), line["event"])
			assert.Equal(t, "sync-123", line["sync_id"])
			assert.Equal(t, "pipe-1", line["pipeline_id"])
			assert.Equal(t, "2026-07-19T12:00:00Z", line["ts"])
			tt.check(t, line)
		})
	}
}

func TestEmitterConcurrentProducesValidJSONL(t *testing.T) {
	var buf bytes.Buffer
	em := NewEmitter(&buf, NewSyncID(), "")

	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, em.Emit(KindStreamProgress, "s.t", StreamProgress{RowsRead: 1}))
		}()
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, n)
	for _, l := range lines {
		var e Event
		require.NoError(t, json.Unmarshal([]byte(l), &e), "every line must be standalone JSON")
	}
}

func TestNewSyncIDSortableAndUnique(t *testing.T) {
	a := NewSyncID()
	time.Sleep(2 * time.Millisecond)
	b := NewSyncID()
	assert.Less(t, a, b, "IDs must sort by creation time")
	assert.NotEqual(t, a, b)
}
