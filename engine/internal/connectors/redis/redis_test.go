package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestConfigValidateDefaults(t *testing.T) {
	c := &Config{Address: "localhost:6379"}
	require.NoError(t, c.validate())
	require.Equal(t, "*", c.Pattern)
	require.Equal(t, int64(500), c.ScanCount)

	require.Error(t, (&Config{}).validate())                               // no address
	require.Error(t, (&Config{Address: "x", DB: -1}).validate())           // bad db
	require.Error(t, (&Config{Type: "postgres", Address: "x"}).validate()) // wrong type
}

func TestTTLSeconds(t *testing.T) {
	require.Equal(t, int64(-1), ttlSeconds(-1))                   // no expiry (-1ns)
	require.Equal(t, int64(-1), ttlSeconds(-2))                   // key gone (-2ns)
	require.Equal(t, int64(60), ttlSeconds(60*time.Second))       // positive TTL
	require.Equal(t, int64(1), ttlSeconds(1500*time.Millisecond)) // floored to seconds
}

func TestMarshal(t *testing.T) {
	// A plain string is emitted as valid JSON (quoted), so the JSON value column
	// carries every Redis shape losslessly.
	s, err := marshal("hello")
	require.NoError(t, err)
	require.Equal(t, `"hello"`, s)

	s, err = marshal([]string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, `["a","b"]`, s)
}

func TestDiscoverFixedSchema(t *testing.T) {
	c := &Connector{cfg: Config{Address: "x", DB: 3, Pattern: "*"}}
	// Discover reads no server state; a non-nil client is the only precondition.
	c.Setup(context.Background(), []byte(`{"address":"localhost:6379","db":3}`))
	streams, err := c.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, streams, 1)

	s := streams[0]
	require.Equal(t, "keys", s.Name)
	require.Equal(t, "db3", s.Namespace)
	require.Equal(t, []model.SyncMode{model.ModeFullLoad}, s.SupportedSyncModes)

	key, ok := s.Schema.Column(colKey)
	require.True(t, ok)
	require.True(t, key.PrimaryKey)
	val, ok := s.Schema.Column(colValue)
	require.True(t, ok)
	require.Equal(t, model.TypeJSON, val.Type)
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "redis", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
	require.Equal(t, []sdk.Capability{sdk.CapFullLoad}, spec.Capabilities)
}
