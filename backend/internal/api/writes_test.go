package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
	"github.com/lakesense/lakesense/backend/internal/pipelines"
	"github.com/lakesense/lakesense/backend/internal/runner"
)

// memRepo is a minimal in-memory pipelines.Repo for handler tests.
type memRepo struct {
	rows map[int64]pipelines.PipelineRow
	hist map[int64][]configver.Version
	next int64
}

func newMemRepo() *memRepo {
	return &memRepo{rows: map[int64]pipelines.PipelineRow{}, hist: map[int64][]configver.Version{}, next: 1}
}
func (m *memRepo) EnsureEnvironment(context.Context, string) (int64, error) { return 1, nil }
func (m *memRepo) CreatePipeline(_ context.Context, _ int64, p pipelines.PipelineRow, v configver.Version, _ []byte) (int64, error) {
	id := m.next
	m.next++
	m.rows[id] = p
	m.hist[id] = []configver.Version{v}
	return id, nil
}
func (m *memRepo) GetPipeline(_ context.Context, id int64) (pipelines.PipelineRow, bool, error) {
	p, ok := m.rows[id]
	return p, ok, nil
}
func (m *memRepo) ConfigHistory(_ context.Context, id int64) ([]configver.Version, error) {
	return m.hist[id], nil
}
func (m *memRepo) UpdatePipeline(_ context.Context, id int64, p pipelines.PipelineRow, v configver.Version, _ []byte, newV bool) error {
	m.rows[id] = p
	if newV {
		m.hist[id] = append(m.hist[id], v)
	}
	return nil
}
func (m *memRepo) SetStatus(_ context.Context, id int64, s string) error {
	p := m.rows[id]
	p.Status = s
	m.rows[id] = p
	return nil
}

type nopRecorder struct{}

func (nopRecorder) Record(context.Context, audit.Entry) error { return nil }

// testServer builds a router with a service over memRepo (no DB).
func testServer() http.Handler {
	svc := pipelines.NewService(newMemRepo(), nopRecorder{}, func() time.Time { return time.Unix(0, 0).UTC() })
	return chiRouter(&Server{logger: slog.Default(), pipelines: svc})
}

func TestCreatePipelineEndpoint(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"name": "Orders", "environment": "dev",
		"source":      map[string]any{"type": "postgres"},
		"destination": map[string]any{"type": "parquet"},
		"schedule":    "@daily",
		"streams":     []map[string]any{{"name": "public.orders", "mode": "full_load"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	req.Header.Set("X-Actor", "alice")
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var p pipelines.Pipeline
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	require.Equal(t, "orders", p.Slug)
	require.Equal(t, 1, p.CurrentVersion)
}

func TestCreatePipelineValidation400(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"name": ""}) // invalid
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// fakeRunner records Run calls for the handler test.
type fakeRunner struct {
	mu       sync.Mutex
	ran      []int64
	notFound bool
}

func (f *fakeRunner) Run(_ context.Context, id int64) (runner.RunResult, error) {
	f.mu.Lock()
	f.ran = append(f.ran, id)
	f.mu.Unlock()
	if f.notFound {
		return runner.RunResult{}, &runner.NotFoundError{ID: id}
	}
	return runner.RunResult{Events: 3}, nil
}

func (f *fakeRunner) Backfill(_ context.Context, id int64, _ runner.BackfillOpts) (runner.RunResult, error) {
	f.mu.Lock()
	f.ran = append(f.ran, id)
	f.mu.Unlock()
	return runner.RunResult{Events: 1}, nil
}

func (f *fakeRunner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ran)
}

func TestRunEndpointAccepts202(t *testing.T) {
	fr := &fakeRunner{}
	svc := pipelines.NewService(newMemRepo(), nopRecorder{}, func() time.Time { return time.Unix(0, 0).UTC() })
	s := &Server{logger: slog.Default(), pipelines: svc, runner: fr}
	h := chiRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/1/run", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	require.Eventually(t, func() bool { return fr.count() == 1 }, time.Second, 5*time.Millisecond)
}

func TestUpdateNotFound404(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"name":        "X",
		"source":      map[string]any{"type": "postgres"},
		"destination": map[string]any{"type": "parquet"},
		"streams":     []map[string]any{{"name": "s", "mode": "full_load"}},
	})
	req := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/999", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}
