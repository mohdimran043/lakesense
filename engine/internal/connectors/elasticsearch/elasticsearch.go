// Package elasticsearch implements the LakeSense Elasticsearch/OpenSearch
// connector: full load and incremental reads over indices via the search API
// (PIT + search_after for stable, resumable pagination). Schema is derived from
// the index mapping; each document's _id becomes the primary key. CDC is
// honestly absent (Elasticsearch has no native change feed), so the connector
// is Beta. OpenSearch speaks the same search/mapping API and is served by the
// same connector.
package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	es "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "elasticsearch"

// idField is the synthetic column carrying a document's _id (its primary key).
const idField = "_id"

// Config is the source configuration.
type Config struct {
	Type      string   `json:"type,omitempty"`
	Addresses []string `json:"addresses"` // e.g. ["http://localhost:9200"]
	Username  string   `json:"username,omitempty"`
	Password  string   `json:"password,omitempty"`
	APIKey    string   `json:"api_key,omitempty"`
	// Index is a single index name or a comma-separated / wildcard pattern
	// (e.g. "orders" or "logs-*"). Each concrete index becomes a stream.
	Index string `json:"index"`
	// PageSize is the search batch size (default 1000).
	PageSize int `json:"page_size,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if len(c.Addresses) == 0 {
		return fmt.Errorf("at least one address is required")
	}
	if c.Index == "" {
		return fmt.Errorf("index is required")
	}
	if c.PageSize <= 0 {
		c.PageSize = 1000
	}
	return nil
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg    Config
	client *es.Client
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Elasticsearch",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "opensearch", DisplayName: "OpenSearch", Notes: "same search/mapping API; same connector"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse elasticsearch config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid elasticsearch config: %w", err)
	}
	client, err := es.NewClient(es.Config{
		Addresses: c.cfg.Addresses,
		Username:  c.cfg.Username,
		Password:  c.cfg.Password,
		APIKey:    c.cfg.APIKey,
	})
	if err != nil {
		return fmt.Errorf("create elasticsearch client: %w", err)
	}
	c.client = client
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	res, err := c.client.Ping(c.client.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("cannot reach elasticsearch: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return fmt.Errorf("elasticsearch ping failed: %s", res.Status())
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error { return nil }

// Discover implements sdk.Connector: resolves the index pattern to concrete
// indices and derives each one's schema from its mapping.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	res, err := c.client.Indices.GetMapping(
		c.client.Indices.GetMapping.WithContext(ctx),
		c.client.Indices.GetMapping.WithIndex(c.cfg.Index),
	)
	if err != nil {
		return nil, fmt.Errorf("get mapping for %q: %w", c.cfg.Index, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return nil, fmt.Errorf("get mapping for %q: %s", c.cfg.Index, res.Status())
	}
	// { "<index>": { "mappings": { "properties": { "<field>": {"type": ...} } } } }
	var body map[string]struct {
		Mappings struct {
			Properties map[string]mappingProp `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode mapping: %w", err)
	}
	indices := make([]string, 0, len(body))
	for name := range body {
		indices = append(indices, name)
	}
	sort.Strings(indices)

	streams := make([]model.Stream, 0, len(indices))
	for _, name := range indices {
		schema := schemaFromMapping(body[name].Mappings.Properties)
		streams = append(streams, model.Stream{
			Namespace:          "",
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

// mappingProp is one field's mapping entry (its type, and nested properties).
type mappingProp struct {
	Type       string                 `json:"type"`
	Properties map[string]mappingProp `json:"properties"`
}

// schemaFromMapping builds a schema from index mapping properties. The _id field
// is prepended as the primary key. Object/nested fields collapse to JSON.
func schemaFromMapping(props map[string]mappingProp) model.Schema {
	schema := model.Schema{Columns: []model.Column{
		{Name: idField, Type: model.TypeString, Nullable: false, PrimaryKey: true},
	}}
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p := props[name]
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(p),
			SourceType: p.Type,
			Nullable:   true,
		})
	}
	return schema
}

// mapType maps an Elasticsearch field mapping to a lake type.
func mapType(p mappingProp) model.DataType {
	if len(p.Properties) > 0 { // object / nested
		return model.TypeJSON
	}
	switch p.Type {
	case "byte", "short", "integer":
		return model.TypeInt32
	case "long", "unsigned_long":
		return model.TypeInt64
	case "float", "half_float":
		return model.TypeFloat32
	case "double", "scaled_float":
		return model.TypeFloat64
	case "boolean":
		return model.TypeBool
	case "date", "date_nanos":
		return model.TypeTimestamp
	case "object", "nested", "flattened":
		return model.TypeJSON
	default:
		// keyword, text, ip, geo_point, etc.
		return model.TypeString
	}
}

// suggestCursor picks a date field to default the incremental cursor to.
func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updated_at", "modified_at", "@timestamp", "created_at", "timestamp"} {
		if col, ok := s.Column(name); ok && col.Type == model.TypeTimestamp {
			return name
		}
	}
	return ""
}

// do executes a JSON search request and decodes the response into out. It is the
// single choke point for search API calls. An empty index means the target is
// carried by a point-in-time in the body (PIT searches reject a URL index).
func (c *Connector) do(ctx context.Context, index string, body map[string]any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	opts := []func(*esapi.SearchRequest){
		c.client.Search.WithContext(ctx),
		c.client.Search.WithBody(bytes.NewReader(buf)),
	}
	if index != "" {
		opts = append(opts, c.client.Search.WithIndex(index))
	}
	res, err := c.client.Search(opts...)
	if err != nil {
		return fmt.Errorf("search %s: %w", indexLabel(index), err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		msg, _ := io.ReadAll(res.Body)
		return fmt.Errorf("search %s: %s: %s", indexLabel(index), res.Status(), strings.TrimSpace(string(msg)))
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return fmt.Errorf("decode search response: %w", err)
	}
	return nil
}

// indexLabel renders an index for error messages, naming the PIT case.
func indexLabel(index string) string {
	if index == "" {
		return "(point-in-time)"
	}
	return index
}

// searchResponse is the subset of a search reply we consume.
type searchResponse struct {
	// PitID is the (possibly refreshed) point-in-time id carried across pages.
	PitID string `json:"pit_id"`
	Hits  struct {
		Hits []struct {
			ID     string          `json:"_id"`
			Source json.RawMessage `json:"_source"`
			Sort   []any           `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		MaxCursor struct {
			Value         *float64 `json:"value"`
			ValueAsString string   `json:"value_as_string"`
		} `json:"max_cursor"`
	} `json:"aggregations"`
}

// hitToRow flattens a document into an engine row: _source fields (nested values
// serialized to JSON) plus the _id column.
func hitToRow(id string, source json.RawMessage) (sdk.Row, error) {
	var m map[string]any
	if len(source) > 0 {
		// UseNumber keeps integer cursors exact (avoids float scientific
		// notation leaking into stored watermarks).
		dec := json.NewDecoder(bytes.NewReader(source))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return nil, fmt.Errorf("decode _source of %s: %w", id, err)
		}
	}
	row := make(sdk.Row, len(m)+1)
	for k, v := range m {
		row[k] = normalizeValue(v)
	}
	row[idField] = id
	return row, nil
}

// normalizeValue keeps scalar JSON values; nested objects/arrays become JSON.
func normalizeValue(v any) any {
	switch v.(type) {
	case map[string]any, []any:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	default:
		return v
	}
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Elasticsearch source",
  "type": "object",
  "required": ["addresses", "index"],
  "properties": {
    "addresses": {"type": "array", "items": {"type": "string"}, "title": "Node addresses", "description": "e.g. http://localhost:9200"},
    "username": {"type": "string", "title": "Username"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "api_key": {"type": "string", "title": "API key", "format": "password"},
    "index": {"type": "string", "title": "Index or pattern", "description": "e.g. orders or logs-*"},
    "page_size": {"type": "integer", "title": "Page size", "default": 1000}
  }
}`
