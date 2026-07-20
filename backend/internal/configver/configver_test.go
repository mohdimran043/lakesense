package configver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func baseConfig() Config {
	return Config{
		Name:        "orders",
		Source:      Endpoint{Type: "postgres", Settings: map[string]string{"host": "db", "database": "shop"}},
		Destination: Endpoint{Type: "iceberg"},
		Schedule:    "@daily",
		Streams:     []Stream{{Name: "public.orders", Mode: "cdc"}},
	}
}

func TestCanonicalYAMLIsDeterministic(t *testing.T) {
	// Same logical config with map keys inserted in a different order must
	// serialize byte-identically (yaml.v3 sorts map keys).
	a := baseConfig()
	b := baseConfig()
	b.Source.Settings = map[string]string{"database": "shop", "host": "db"} // reordered

	ya, err := CanonicalYAML(a)
	require.NoError(t, err)
	yb, err := CanonicalYAML(b)
	require.NoError(t, err)
	assert.Equal(t, ya, yb, "canonical YAML must be order-independent")
	assert.Contains(t, ya, "name: orders")
}

func TestRoundTrip(t *testing.T) {
	y, err := CanonicalYAML(baseConfig())
	require.NoError(t, err)
	got, err := Parse(y)
	require.NoError(t, err)
	assert.Equal(t, baseConfig(), got)
}

func TestNewVersionSkipsNoOpChange(t *testing.T) {
	v1, created, err := NewVersion(nil, baseConfig(), "initial", "alice", t0)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, 1, v1.Number)

	// Identical config → no new version.
	same, created2, err := NewVersion([]Version{v1}, baseConfig(), "again", "alice", t0)
	require.NoError(t, err)
	assert.False(t, created2, "identical config must not create a version")
	assert.Equal(t, v1.Number, same.Number)

	// A real change → v2.
	changed := baseConfig()
	changed.Schedule = "@hourly"
	v2, created3, err := NewVersion([]Version{v1}, changed, "faster", "bob", t0)
	require.NoError(t, err)
	assert.True(t, created3)
	assert.Equal(t, 2, v2.Number)
}

func TestDiffMarksOnlyChangedLines(t *testing.T) {
	v1, _, _ := NewVersion(nil, baseConfig(), "initial", "alice", t0)
	changed := baseConfig()
	changed.Schedule = "@hourly"
	changed.Streams = append(changed.Streams, Stream{Name: "public.customers", Mode: "incremental", CursorField: "updated_at"})
	v2, _, _ := NewVersion([]Version{v1}, changed, "add stream", "bob", t0)

	d := Diff(v1.YAML, v2.YAML)
	assert.True(t, Changed(d))

	var added, removed []string
	for _, l := range d {
		if l.Op == "+" {
			added = append(added, l.Text)
		}
		if l.Op == "-" {
			removed = append(removed, l.Text)
		}
	}
	// The schedule line changed and the new stream lines were added.
	assert.Contains(t, removed, "schedule: '@daily'")
	assert.Contains(t, added, "schedule: '@hourly'")
	assert.Contains(t, joinLines(added), "public.customers")
	// Unchanged lines (name: orders) are context, not add/remove.
	assert.NotContains(t, joinLines(added), "name: orders")
	assert.NotContains(t, joinLines(removed), "name: orders")
}

func TestDiffOfIdenticalIsAllContext(t *testing.T) {
	y, _ := CanonicalYAML(baseConfig())
	d := Diff(y, y)
	assert.False(t, Changed(d))
}

func TestRollbackAppendsRestoringVersion(t *testing.T) {
	v1, _, _ := NewVersion(nil, baseConfig(), "initial", "alice", t0)
	c2 := baseConfig()
	c2.Schedule = "@hourly"
	v2, _, _ := NewVersion([]Version{v1}, c2, "faster", "bob", t0)

	// Roll back to v1 → creates v3 with v1's content.
	v3, err := Rollback([]Version{v1, v2}, 1, "carol", t0)
	require.NoError(t, err)
	assert.Equal(t, 3, v3.Number)
	assert.Equal(t, v1.YAML, v3.YAML, "rollback restores the target's exact config")
	assert.Equal(t, "rollback to v1", v3.Note)

	_, err = Rollback([]Version{v1, v2}, 9, "carol", t0)
	assert.Error(t, err, "rolling back to a missing version fails")
}

// strings joins a slice for substring assertions.
func joinLines(xs []string) string {
	out := ""
	for _, x := range xs {
		out += x + "\n"
	}
	return out
}
