package enrich

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func failure() Failure {
	return Failure{
		Pipeline: "orders-postgres", Stream: "public.orders", Connector: "postgres",
		ErrorCode: "connection_refused", ErrorMessage: "dial tcp: connection refused", Retryable: true,
	}
}

func TestFallbackWhenNoKey(t *testing.T) {
	c := New("", "claude-opus-4-8", "", nil)
	assert.False(t, c.Enabled())

	e, err := c.Enrich(context.Background(), failure())
	require.NoError(t, err)
	assert.Equal(t, "fallback", e.Source)
	assert.Contains(t, e.RootCause, "could not reach") // known-code mapping
	assert.Equal(t, []string{"public.orders"}, e.AffectedTables)
	assert.Equal(t, "warning", e.Severity) // retryable ⇒ warning
	assert.NotEmpty(t, e.SuggestedFix)
}

func TestFallbackSeverityForHardFailure(t *testing.T) {
	f := failure()
	f.Retryable = false
	f.ErrorCode = "schema_mismatch"
	e := fallback(f)
	assert.Equal(t, "critical", e.Severity)
}

func TestLLMPathParsesStrictJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-key", r.Header.Get("x-api-key"))
		assert.Equal(t, apiVersion, r.Header.Get("anthropic-version"))
		// Model returns JSON wrapped in a markdown fence to exercise stripping.
		payload := map[string]any{
			"content": []map[string]any{{
				"type": "text",
				"text": "```json\n{\"root_cause\":\"Source DB unreachable\",\"affected_tables\":[\"public.orders\"],\"suggested_fix\":\"Restart Postgres\",\"severity\":\"critical\"}\n```",
			}},
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	c := New("test-key", "claude-opus-4-8", srv.URL, srv.Client())
	e, err := c.Enrich(context.Background(), failure())
	require.NoError(t, err)
	assert.Equal(t, "llm", e.Source)
	assert.Equal(t, "Source DB unreachable", e.RootCause)
	assert.Equal(t, "critical", e.Severity)
	assert.Equal(t, []string{"public.orders"}, e.AffectedTables)
}

func TestLLMErrorDegradesToFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"nope"}`)
	}))
	defer srv.Close()

	c := New("test-key", "claude-opus-4-8", srv.URL, srv.Client())
	e, err := c.Enrich(context.Background(), failure())
	require.Error(t, err, "the API error is surfaced for logging")
	assert.Equal(t, "fallback", e.Source, "but a usable enrichment is still returned")
	assert.NotEmpty(t, e.RootCause)
}

func TestRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"content": []map[string]any{{"type": "text",
				"text": `{"root_cause":"x","affected_tables":[],"suggested_fix":"y","severity":"warning"}`}},
		})
	}))
	defer srv.Close()

	c := New("k", "claude-opus-4-8", srv.URL, srv.Client())
	e, err := c.Enrich(context.Background(), failure())
	require.NoError(t, err)
	assert.Equal(t, "llm", e.Source)
	assert.Equal(t, 2, calls, "retried once after the 503")
	// affected_tables was empty in the reply, so Enrich backfills from the stream.
	assert.Equal(t, []string{"public.orders"}, e.AffectedTables)
}

func TestPostmortemFallback(t *testing.T) {
	c := New("", "claude-opus-4-8", "", nil)
	pm, err := c.DraftPostmortem(context.Background(), failure(), "restarted source and re-ran")
	require.NoError(t, err)
	assert.Contains(t, pm, "postmortem")
	assert.Contains(t, pm, "orders-postgres")
}
