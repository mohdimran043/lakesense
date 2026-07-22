package objectstore

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func collect(t *testing.T, format, data string) []sdk.Row {
	t.Helper()
	var rows []sdk.Row
	err := parseRecords(context.Background(), format, strings.NewReader(data), func(_ context.Context, r sdk.Row) error {
		rows = append(rows, r)
		return nil
	})
	require.NoError(t, err)
	return rows
}

func TestParseNDJSON(t *testing.T) {
	rows := collect(t, "ndjson", `{"id":1,"name":"a","tags":["x","y"]}
{"id":2,"name":"b"}
`)
	require.Len(t, rows, 2)
	require.EqualValues(t, 1, rows[0]["id"])
	require.Equal(t, "a", rows[0]["name"])
	require.Equal(t, `["x","y"]`, rows[0]["tags"], "nested array → JSON")
}

func TestParseCSV(t *testing.T) {
	rows := collect(t, "csv", "id,name\n1,alice\n2,bob\n")
	require.Len(t, rows, 2)
	require.Equal(t, "1", rows[0]["id"])
	require.Equal(t, "alice", rows[0]["name"])
}

func TestColumnsOf(t *testing.T) {
	nd, err := columnsOf("ndjson", strings.NewReader(`{"b":2,"a":1}`))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, nd, "keys sorted for stable order")

	cs, err := columnsOf("csv", strings.NewReader("x,y,z\n1,2,3\n"))
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y", "z"}, cs)
}

func TestConfigValidateProviders(t *testing.T) {
	// S3 (default provider) requires an endpoint.
	s3 := &Config{Endpoint: "s3.amazonaws.com", Bucket: "b"}
	require.NoError(t, s3.validate())
	require.Equal(t, providerS3, s3.Provider)
	require.Equal(t, "ndjson", s3.Format)
	require.Equal(t, "b", s3.Stream) // stream defaults to bucket
	require.Error(t, (&Config{Bucket: "b"}).validate(), "s3 needs an endpoint")

	// Azure requires a connection string or account+key.
	az := &Config{Provider: providerAzure, Bucket: "c", ConnectionString: "x"}
	require.NoError(t, az.validate())
	require.NoError(t, (&Config{Provider: providerAzure, Bucket: "c", Account: "a", AccountKey: "k"}).validate())
	require.Error(t, (&Config{Provider: providerAzure, Bucket: "c"}).validate(), "azure needs credentials")
	require.Error(t, (&Config{Provider: providerAzure, ConnectionString: "x"}).validate(), "needs bucket/container")

	require.Error(t, (&Config{Provider: "gcs-native", Bucket: "b"}).validate(), "unknown provider")
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "object_storage", spec.Type)
	// Azure is now an advertised preset alongside the S3 family.
	var names []string
	for _, p := range spec.Presets {
		names = append(names, p.Name)
	}
	require.Contains(t, names, "azure")
}
