package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// health and version handlers do not touch the pool, so they are testable
// without a database — the fast path that runs in every CI push.
func TestHealthAndVersion(t *testing.T) {
	h := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	t.Run("healthz", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "ok", body["status"])
	})

	t.Run("computeHealth", func(t *testing.T) {
		yes, no := true, false
		assert.Equal(t, 100, computeHealth(&no, true, 0), "clean pipeline")
		assert.Equal(t, 100, computeHealth(nil, true, 0), "no recent runs, verified")
		assert.Equal(t, 60, computeHealth(&yes, true, 0), "recent failure -40")
		assert.Equal(t, 75, computeHealth(&no, false, 0), "unverified diff -25")
		assert.Equal(t, 70, computeHealth(&no, true, 3), "open incidents capped at -30")
		assert.Equal(t, 25, computeHealth(&yes, false, 1), "-40 -25 -10")
	})

	t.Run("version", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/version", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "lakesense", body["name"])
	})
}
