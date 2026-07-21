package pipelines

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
)

// fakeRepo is an in-memory Repo for service unit tests.
type fakeRepo struct {
	envs      map[string]int64
	pipelines map[int64]PipelineRow
	history   map[int64][]configver.Version
	nextID    int64
	failNext  error // when set, the next CreatePipeline call returns it
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{envs: map[string]int64{}, pipelines: map[int64]PipelineRow{}, history: map[int64][]configver.Version{}, nextID: 1}
}

func (f *fakeRepo) EnsureEnvironment(_ context.Context, slug string) (int64, error) {
	if id, ok := f.envs[slug]; ok {
		return id, nil
	}
	id := int64(100 + len(f.envs))
	f.envs[slug] = id
	return id, nil
}

func (f *fakeRepo) CreatePipeline(_ context.Context, _ int64, p PipelineRow, v configver.Version, _ []byte) (int64, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
	}
	id := f.nextID
	f.nextID++
	f.pipelines[id] = p
	f.history[id] = []configver.Version{v}
	return id, nil
}

func (f *fakeRepo) GetPipeline(_ context.Context, id int64) (PipelineRow, bool, error) {
	p, ok := f.pipelines[id]
	return p, ok, nil
}

func (f *fakeRepo) ConfigHistory(_ context.Context, id int64) ([]configver.Version, error) {
	return f.history[id], nil
}

func (f *fakeRepo) UpdatePipeline(_ context.Context, id int64, p PipelineRow, v configver.Version, _ []byte, newVersion bool) error {
	f.pipelines[id] = p
	if newVersion {
		f.history[id] = append(f.history[id], v)
	}
	return nil
}

func (f *fakeRepo) SetStatus(_ context.Context, id int64, status string) error {
	p := f.pipelines[id]
	p.Status = status
	f.pipelines[id] = p
	return nil
}

// fakeRecorder captures audit entries.
type fakeRecorder struct{ entries []audit.Entry }

func (r *fakeRecorder) Record(_ context.Context, e audit.Entry) error {
	r.entries = append(r.entries, e)
	return nil
}

func fixedNow() time.Time { return time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) }

func sampleReq() CreatePipelineRequest {
	return CreatePipelineRequest{
		Name:        "Orders to Lake",
		Environment: "dev",
		Source:      Endpoint{Type: "postgres", Settings: map[string]string{"host": "db"}},
		Destination: Endpoint{Type: "parquet", Settings: map[string]string{"path": "./out"}},
		Schedule:    "@daily",
		Streams:     []Stream{{Name: "public.orders", Mode: "full_load"}},
	}
}

func TestCreatePersistsV1AndAudits(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)

	p, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)
	require.Equal(t, "postgres", p.SourceType)
	require.Equal(t, "parquet", p.DestinationType)
	require.Equal(t, "orders-to-lake", p.Slug)
	require.Equal(t, 1, p.CurrentVersion)

	require.Len(t, repo.history[p.ID], 1)
	require.Equal(t, 1, repo.history[p.ID][0].Number)

	require.Len(t, rec.entries, 1)
	require.Equal(t, "pipeline.create", rec.entries[0].Action)
	require.Equal(t, "alice", rec.entries[0].Actor)
}

func TestCreateValidationRejectsAndDoesNotPersist(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)

	bad := sampleReq()
	bad.Name = ""
	_, err := svc.Create(context.Background(), "alice", bad)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	require.Empty(t, repo.pipelines, "no pipeline persisted on validation failure")
	require.Empty(t, rec.entries, "no audit entry on validation failure")
}

func TestCreateIncrementalRequiresCursor(t *testing.T) {
	svc := NewService(newFakeRepo(), &fakeRecorder{}, fixedNow)
	bad := sampleReq()
	bad.Streams = []Stream{{Name: "public.orders", Mode: "incremental"}} // no cursor
	_, err := svc.Create(context.Background(), "alice", bad)
	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
}

func TestUpdateChangedConfigCreatesV2(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	changed := sampleReq()
	changed.Schedule = "@hourly"
	p2, err := svc.Update(context.Background(), "bob", p.ID, changed)
	require.NoError(t, err)
	require.Equal(t, 2, p2.CurrentVersion)
	require.Len(t, repo.history[p.ID], 2)
}

func TestUpdateIdenticalConfigCreatesNoNewVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	p2, err := svc.Update(context.Background(), "bob", p.ID, sampleReq()) // identical
	require.NoError(t, err)
	require.Equal(t, 1, p2.CurrentVersion, "no-op change keeps version 1")
	require.Len(t, repo.history[p.ID], 1)
}

func TestRollbackAppendsRestoringVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())
	changed := sampleReq()
	changed.Schedule = "@hourly"
	_, _ = svc.Update(context.Background(), "bob", p.ID, changed) // v2

	p3, err := svc.Rollback(context.Background(), "carol", p.ID, 1)
	require.NoError(t, err)
	require.Equal(t, 3, p3.CurrentVersion)
	require.Equal(t, repo.history[p.ID][0].YAML, repo.history[p.ID][2].YAML, "v3 restores v1 content")
}

func TestUpdateNotFound(t *testing.T) {
	svc := NewService(newFakeRepo(), &fakeRecorder{}, fixedNow)
	_, err := svc.Update(context.Background(), "bob", 999, sampleReq())
	var nf *NotFoundError
	require.ErrorAs(t, err, &nf)
}

func TestArchiveSetsStatusAndAudits(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	require.NoError(t, svc.SetStatus(context.Background(), "bob", p.ID, "archived"))
	require.Equal(t, "archived", repo.pipelines[p.ID].Status)
	require.Equal(t, "pipeline.archive", rec.entries[len(rec.entries)-1].Action)
}
