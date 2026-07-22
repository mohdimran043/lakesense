// Package kafka implements the LakeSense Kafka connector: a bounded-offset reader
// over a topic. Each message becomes a row in a fixed envelope — partition,
// offset (together the primary key), key, value (JSON per message), timestamp,
// headers. Full load reads every retained message up to the current end offsets;
// incremental resumes from stored per-partition offsets to the end offsets
// captured at call time (bounded, so a read always terminates). The cursor is the
// per-partition offset map, JSON-encoded. v1 decodes message values as JSON (raw
// bytes fall back to a JSON string); Avro / schema-registry is not yet supported,
// so the connector is Beta. Design follows docs/analysis/other-sources.md.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "kafka"

// cursorField is the synthetic cursor key: its value is a JSON per-partition
// offset map, opaque to the engine.
const cursorField = "_offset"

// Envelope column names.
const (
	colPartition = "partition"
	colOffset    = "offset"
	colKey       = "key"
	colValue     = "value"
	colTimestamp = "timestamp"
	colHeaders   = "headers"
)

// Config is the source configuration.
type Config struct {
	Type    string   `json:"type,omitempty"`
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	// MaxBytes bounds one fetch response (default 1 MiB).
	MaxBytes int `json:"max_bytes,omitempty"`
	// DialTimeout bounds connect/read operations in seconds (default 15).
	DialTimeout int `json:"dial_timeout,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 1 << 20
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 15
	}
	return nil
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg Config
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Kafka",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "redpanda", DisplayName: "Redpanda", Notes: "Kafka-protocol compatible; same connector"},
		},
	}
}

// Setup implements sdk.Connector. Kafka connections are short-lived (opened per
// partition read), so Setup only parses and validates config.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse kafka config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid kafka config: %w", err)
	}
	return nil
}

func (c *Connector) timeout() time.Duration { return time.Duration(c.cfg.DialTimeout) * time.Second }

// dial opens a connection to the first reachable broker.
func (c *Connector) dial(ctx context.Context) (*kafka.Conn, error) {
	d := &kafka.Dialer{Timeout: c.timeout()}
	var lastErr error
	for _, b := range c.cfg.Brokers {
		conn, err := d.DialContext(ctx, "tcp", b)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("dial kafka: %w", lastErr)
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return fmt.Errorf("cannot reach kafka: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ReadPartitions(); err != nil {
		return fmt.Errorf("cannot list kafka metadata: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error { return nil }

// Discover implements sdk.Connector: one stream for the configured topic with a
// fixed envelope schema. Partitions are read to confirm the topic exists.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	parts, err := c.partitions(ctx)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("topic %q has no partitions (does it exist?)", c.cfg.Topic)
	}
	schema := model.Schema{Columns: []model.Column{
		{Name: colPartition, Type: model.TypeInt32, Nullable: false, PrimaryKey: true},
		{Name: colOffset, Type: model.TypeInt64, Nullable: false, PrimaryKey: true},
		{Name: colKey, Type: model.TypeString, Nullable: true},
		{Name: colValue, Type: model.TypeJSON, Nullable: true},
		{Name: colTimestamp, Type: model.TypeTimestamp, Nullable: false},
		{Name: colHeaders, Type: model.TypeJSON, Nullable: true},
	}}
	return []model.Stream{{
		Namespace:          "",
		Name:               c.cfg.Topic,
		Schema:             schema,
		SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
		DefaultCursorField: cursorField,
	}}, nil
}

// partitions lists the partition ids of the configured topic.
func (c *Connector) partitions(ctx context.Context) ([]int, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	parts, err := conn.ReadPartitions(c.cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions for %q: %w", c.cfg.Topic, err)
	}
	ids := make([]int, 0, len(parts))
	for _, p := range parts {
		ids = append(ids, p.ID)
	}
	sort.Ints(ids)
	return ids, nil
}

// encodeOffsets serializes a partition→offset map as a deterministic JSON cursor.
func encodeOffsets(m map[int]int64) string {
	// map[int] marshals with string keys sorted numerically by encoding/json? No —
	// json sorts string keys lexically. Build explicitly for stable, exact output.
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	out := make(map[string]int64, len(m))
	for _, k := range keys {
		out[fmt.Sprintf("%d", k)] = m[k]
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// decodeOffsets parses a JSON cursor into a partition→offset map. An empty string
// yields an empty map (start from each partition's earliest offset).
func decodeOffsets(s string) (map[int]int64, error) {
	out := map[int]int64{}
	if s == "" {
		return out, nil
	}
	raw := map[string]int64{}
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, fmt.Errorf("decode offset cursor %q: %w", s, err)
	}
	for k, v := range raw {
		var p int
		if _, err := fmt.Sscan(k, &p); err != nil {
			return nil, fmt.Errorf("bad partition key %q in cursor: %w", k, err)
		}
		out[p] = v
	}
	return out, nil
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Kafka source",
  "type": "object",
  "required": ["brokers", "topic"],
  "properties": {
    "brokers": {"type": "array", "items": {"type": "string"}, "title": "Bootstrap brokers", "description": "host:port"},
    "topic": {"type": "string", "title": "Topic"},
    "max_bytes": {"type": "integer", "title": "Max fetch bytes", "default": 1048576},
    "dial_timeout": {"type": "integer", "title": "Dial timeout (s)", "default": 15}
  }
}`
