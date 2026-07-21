package runner

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenEndpoint(t *testing.T) {
	out, err := flattenEndpoint([]byte(`{"type":"postgres","settings":{"host":"db","user":"ro"}}`))
	require.NoError(t, err)
	require.Equal(t, "postgres", out["type"])
	require.Equal(t, "db", out["host"])
	require.Equal(t, "ro", out["user"])
}

func TestParseSelectionsSplitsNamespace(t *testing.T) {
	sels, err := parseSelections([]byte(`{"streams":[{"name":"public.orders","mode":"full_load"},{"name":"main.items","mode":"incremental","cursor_field":"updated_at"}]}`))
	require.NoError(t, err)
	require.Len(t, sels, 2)
	require.Equal(t, "public", sels[0].Namespace)
	require.Equal(t, "orders", sels[0].Name)
	require.Equal(t, "updated_at", sels[1].CursorField)
}

func TestBuildCatalogAttachesSelectedStreams(t *testing.T) {
	discovered := []byte(`{"streams":[{"namespace":"public","name":"orders","schema":{"columns":[{"name":"id","type":"int64"}]}}]}`)
	sels := []StreamSelection{{Namespace: "public", Name: "orders", Mode: "full_load"}}
	out, err := buildCatalog(discovered, sels)
	require.NoError(t, err)

	var cat map[string]any
	require.NoError(t, json.Unmarshal(out, &cat))
	require.Contains(t, cat, "streams")
	selected, ok := cat["selected_streams"].([]any)
	require.True(t, ok)
	require.Len(t, selected, 1)
	first := selected[0].(map[string]any)
	require.Equal(t, "public", first["namespace"])
	require.Equal(t, "orders", first["name"])
	require.Equal(t, "full_load", first["mode"])
}
