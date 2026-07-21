// Package objectstore implements the LakeSense object-storage source: a single
// S3-compatible connector serving Amazon S3, Google Cloud Storage (S3 interop),
// and MinIO. It reads NDJSON and CSV files under a prefix as a stream, inferring
// the schema from the first object; incremental picks up objects modified since
// the last run. Azure Blob (a different API) stays on the roadmap. No CDC.
package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "object_storage"

// Config is the source configuration.
type Config struct {
	Type      string `json:"type,omitempty"`
	Endpoint  string `json:"endpoint"` // e.g. "s3.amazonaws.com", "127.0.0.1:9000"
	Region    string `json:"region,omitempty"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	// Format of the objects: "ndjson" (default) or "csv".
	Format string `json:"format,omitempty"`
	// UseSSL toggles https (default true; set false for local MinIO). Pointer so
	// the zero value is distinguishable from an explicit false.
	UseSSL *bool `json:"use_ssl,omitempty"`
	// Stream overrides the logical stream name (defaults to the bucket).
	Stream string `json:"stream,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	switch c.Format {
	case "", "ndjson":
		c.Format = "ndjson"
	case "csv":
	default:
		return fmt.Errorf("format must be \"ndjson\" or \"csv\", got %q", c.Format)
	}
	if c.Stream == "" {
		c.Stream = c.Bucket
	}
	return nil
}

func (c *Config) useSSL() bool { return c.UseSSL == nil || *c.UseSSL }

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg    Config
	client *minio.Client
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Object Storage (S3-compatible)",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "s3", DisplayName: "Amazon S3"},
			{Name: "gcs", DisplayName: "Google Cloud Storage", Notes: "via the S3 interoperability endpoint"},
			{Name: "minio", DisplayName: "MinIO"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse object_storage config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid object_storage config: %w", err)
	}
	client, err := minio.New(c.cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.cfg.AccessKey, c.cfg.SecretKey, ""),
		Secure: c.cfg.useSSL(),
		Region: c.cfg.Region,
	})
	if err != nil {
		return fmt.Errorf("create object-storage client: %w", err)
	}
	c.client = client
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	ok, err := c.client.BucketExists(ctx, c.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("cannot reach bucket %s: %w", c.cfg.Bucket, err)
	}
	if !ok {
		return fmt.Errorf("bucket %s not found or not accessible", c.cfg.Bucket)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error { return nil }

// Discover implements sdk.Connector: one stream over the bucket/prefix, its
// schema inferred from the first object. Columns are string-typed (files are
// untyped); NDJSON keys or the CSV header become the columns.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	cols, err := c.inferColumns(ctx)
	if err != nil {
		return nil, err
	}
	schema := model.Schema{Columns: make([]model.Column, len(cols))}
	for i, name := range cols {
		schema.Columns[i] = model.Column{Name: name, Type: model.TypeString, Nullable: true}
	}
	return []model.Stream{{
		Namespace:          c.cfg.Bucket,
		Name:               c.cfg.Stream,
		Schema:             schema,
		SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
		DefaultCursorField: "", // incremental is by object modified-time, not a column
	}}, nil
}

// inferColumns reads the first object under the prefix and returns its columns.
func (c *Connector) inferColumns(ctx context.Context) ([]string, error) {
	key, ok, err := c.firstObject(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []string{}, nil // empty prefix — no columns yet
	}
	obj, err := c.client.GetObject(ctx, c.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	return columnsOf(c.cfg.Format, obj)
}

// firstObject returns the first non-directory object key under the prefix.
func (c *Connector) firstObject(ctx context.Context) (string, bool, error) {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range c.client.ListObjects(listCtx, c.cfg.Bucket, minio.ListObjectsOptions{Prefix: c.cfg.Prefix, Recursive: true}) {
		if o.Err != nil {
			return "", false, fmt.Errorf("list objects: %w", o.Err)
		}
		if strings.HasSuffix(o.Key, "/") {
			continue
		}
		return o.Key, true, nil
	}
	return "", false, nil
}

// base returns an object's file name for diagnostics.
func base(key string) string { return path.Base(key) }

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Object Storage source (S3-compatible)",
  "type": "object",
  "required": ["endpoint", "bucket", "access_key", "secret_key"],
  "properties": {
    "endpoint": {"type": "string", "title": "Endpoint", "description": "e.g. s3.amazonaws.com, storage.googleapis.com, host:9000"},
    "region": {"type": "string", "title": "Region"},
    "bucket": {"type": "string", "title": "Bucket"},
    "prefix": {"type": "string", "title": "Key prefix"},
    "access_key": {"type": "string", "title": "Access key"},
    "secret_key": {"type": "string", "title": "Secret key", "format": "password"},
    "format": {"type": "string", "title": "Object format", "default": "ndjson", "enum": ["ndjson", "csv"]},
    "use_ssl": {"type": "boolean", "title": "Use TLS", "default": true},
    "stream": {"type": "string", "title": "Stream name"}
  }
}`
