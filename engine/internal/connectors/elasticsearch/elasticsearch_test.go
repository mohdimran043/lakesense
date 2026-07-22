package elasticsearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"byte": model.TypeInt32, "short": model.TypeInt32, "integer": model.TypeInt32,
		"long": model.TypeInt64, "unsigned_long": model.TypeInt64,
		"float": model.TypeFloat32, "half_float": model.TypeFloat32,
		"double": model.TypeFloat64, "scaled_float": model.TypeFloat64,
		"boolean": model.TypeBool,
		"date":    model.TypeTimestamp, "date_nanos": model.TypeTimestamp,
		"object": model.TypeJSON, "nested": model.TypeJSON, "flattened": model.TypeJSON,
		"keyword": model.TypeString, "text": model.TypeString, "ip": model.TypeString, "geo_point": model.TypeString,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(mappingProp{Type: in}), "mapType(%q)", in)
	}
}

func TestMapTypeObjectWithProperties(t *testing.T) {
	// A field with nested properties collapses to JSON regardless of declared type.
	p := mappingProp{Type: "text", Properties: map[string]mappingProp{"a": {Type: "long"}}}
	require.Equal(t, model.TypeJSON, mapType(p))
}

func TestSchemaFromMapping(t *testing.T) {
	props := map[string]mappingProp{
		"name":       {Type: "keyword"},
		"created_at": {Type: "date"},
		"nested":     {Properties: map[string]mappingProp{"x": {Type: "long"}}},
	}
	schema := schemaFromMapping(props)

	// _id is first, is the primary key, and is non-nullable.
	require.Equal(t, idField, schema.Columns[0].Name)
	require.True(t, schema.Columns[0].PrimaryKey)
	require.False(t, schema.Columns[0].Nullable)

	// Remaining columns are sorted by name and carry the source type.
	got := map[string]model.Column{}
	for _, c := range schema.Columns {
		got[c.Name] = c
	}
	require.Equal(t, model.TypeTimestamp, got["created_at"].Type)
	require.Equal(t, "date", got["created_at"].SourceType)
	require.Equal(t, model.TypeJSON, got["nested"].Type)
	require.True(t, got["name"].Nullable)
}

func TestSuggestCursor(t *testing.T) {
	withTS := model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeString},
		{Name: "@timestamp", Type: model.TypeTimestamp},
	}}
	require.Equal(t, "@timestamp", suggestCursor(withTS))

	// updated_at is preferred over @timestamp when both exist.
	both := model.Schema{Columns: []model.Column{
		{Name: "@timestamp", Type: model.TypeTimestamp},
		{Name: "updated_at", Type: model.TypeTimestamp},
	}}
	require.Equal(t, "updated_at", suggestCursor(both))

	// A same-named non-timestamp column is not chosen.
	strTS := model.Schema{Columns: []model.Column{{Name: "timestamp", Type: model.TypeString}}}
	require.Equal(t, "", suggestCursor(strTS))
}

func TestHitToRow(t *testing.T) {
	src := json.RawMessage(`{"name":"a","count":42,"tags":["x","y"],"meta":{"k":1}}`)
	row, err := hitToRow("doc-1", src)
	require.NoError(t, err)

	require.Equal(t, "doc-1", row[idField])
	require.Equal(t, "a", row["name"])
	// Integers preserve exact text (no float scientific notation) via json.Number.
	require.Equal(t, json.Number("42"), row["count"])
	// Arrays and nested objects serialize to JSON strings.
	require.Equal(t, `["x","y"]`, row["tags"])
	require.Equal(t, `{"k":1}`, row["meta"])
}

func TestHitToRowEmptySource(t *testing.T) {
	row, err := hitToRow("doc-1", nil)
	require.NoError(t, err)
	require.Equal(t, "doc-1", row[idField])
	require.Len(t, row, 1)
}

func TestCursorString(t *testing.T) {
	v := 1.6e12
	resp := searchResponse{}
	resp.Aggregations.MaxCursor.Value = &v
	resp.Aggregations.MaxCursor.ValueAsString = "2020-09-13T12:26:40.000Z"

	// Temporal cursor uses the formatted date string.
	tsStream := model.Stream{Schema: model.Schema{Columns: []model.Column{
		{Name: "ts", Type: model.TypeTimestamp},
	}}}
	require.Equal(t, "2020-09-13T12:26:40.000Z", cursorString(tsStream, "ts", resp))

	// Numeric cursor uses plain decimal text, never scientific notation.
	numStream := model.Stream{Schema: model.Schema{Columns: []model.Column{
		{Name: "seq", Type: model.TypeInt64},
	}}}
	require.Equal(t, "1600000000000", cursorString(numStream, "seq", resp))

	// No documents → empty watermark.
	empty := searchResponse{}
	require.Equal(t, "", cursorString(numStream, "seq", empty))
}

func TestConfigValidate(t *testing.T) {
	c := &Config{Addresses: []string{"http://localhost:9200"}, Index: "orders"}
	require.NoError(t, c.validate())
	require.Equal(t, 1000, c.PageSize) // default applied

	require.Error(t, (&Config{Index: "orders"}).validate())                                               // no address
	require.Error(t, (&Config{Addresses: []string{"http://x"}}).validate())                               // no index
	require.Error(t, (&Config{Type: "postgres", Addresses: []string{"http://x"}, Index: "o"}).validate()) // wrong type
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "elasticsearch", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
	require.Contains(t, spec.Capabilities, sdk.CapFullLoad)
	require.Contains(t, spec.Capabilities, sdk.CapIncremental)
}
