// Package objectstore implements the LakeSense object-storage source: one
// connector serving Amazon S3, Google Cloud Storage (S3 interop), MinIO, and
// Azure Blob behind a common backend interface. It reads NDJSON and CSV files
// under a prefix as a stream, inferring the schema from the first object;
// incremental picks up objects modified since the last run. No CDC.
package objectstore

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "object_storage"

// Providers.
const (
	providerS3    = "s3"
	providerAzure = "azure"
)

// Config is the source configuration. Provider selects the backend; S3 fields
// (endpoint/access_key/secret_key) and Azure fields (connection_string, or
// account+account_key) apply to their respective providers. Bucket names the
// bucket (S3) or container (Azure).
type Config struct {
	Type     string `json:"type,omitempty"`
	Provider string `json:"provider,omitempty"` // "s3" (default) or "azure"
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix,omitempty"`
	// S3-compatible fields.
	Endpoint  string `json:"endpoint,omitempty"` // e.g. "s3.amazonaws.com", "127.0.0.1:9000"
	Region    string `json:"region,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	// UseSSL toggles https (default true; set false for local MinIO). Pointer so
	// the zero value is distinguishable from an explicit false.
	UseSSL *bool `json:"use_ssl,omitempty"`
	// Azure Blob fields.
	ConnectionString string `json:"connection_string,omitempty"`
	Account          string `json:"account,omitempty"`
	AccountKey       string `json:"account_key,omitempty"`
	// Format of the objects: "ndjson" (default) or "csv".
	Format string `json:"format,omitempty"`
	// Stream overrides the logical stream name (defaults to the bucket).
	Stream string `json:"stream,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	switch c.Provider {
	case "", providerS3:
		c.Provider = providerS3
		if c.Endpoint == "" {
			return fmt.Errorf("endpoint is required for s3")
		}
	case providerAzure:
		if c.ConnectionString == "" && (c.Account == "" || c.AccountKey == "") {
			return fmt.Errorf("azure requires connection_string, or account and account_key")
		}
	default:
		return fmt.Errorf("provider must be %q or %q, got %q", providerS3, providerAzure, c.Provider)
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
	cfg     Config
	backend backend
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Object Storage (S3 + Azure)",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "s3", DisplayName: "Amazon S3"},
			{Name: "gcs", DisplayName: "Google Cloud Storage", Notes: "via the S3 interoperability endpoint"},
			{Name: "minio", DisplayName: "MinIO"},
			{Name: "azure", DisplayName: "Azure Blob Storage", Notes: "set provider=azure"},
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
	b, err := c.newBackend()
	if err != nil {
		return err
	}
	c.backend = b
	return nil
}

// newBackend builds the storage backend for the configured provider.
func (c *Connector) newBackend() (backend, error) {
	switch c.cfg.Provider {
	case providerAzure:
		client, err := c.newAzureClient()
		if err != nil {
			return nil, fmt.Errorf("create azure client: %w", err)
		}
		return &azureBackend{client: client, container: c.cfg.Bucket, prefix: c.cfg.Prefix}, nil
	default: // providerS3
		client, err := minio.New(c.cfg.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(c.cfg.AccessKey, c.cfg.SecretKey, ""),
			Secure: c.cfg.useSSL(),
			Region: c.cfg.Region,
		})
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		return &s3Backend{client: client, bucket: c.cfg.Bucket, prefix: c.cfg.Prefix}, nil
	}
}

// newAzureClient builds an azblob client from a connection string, or from an
// account name + key (optionally against a custom endpoint, e.g. Azurite).
func (c *Connector) newAzureClient() (*azblob.Client, error) {
	if c.cfg.ConnectionString != "" {
		return azblob.NewClientFromConnectionString(c.cfg.ConnectionString, nil)
	}
	cred, err := azblob.NewSharedKeyCredential(c.cfg.Account, c.cfg.AccountKey)
	if err != nil {
		return nil, err
	}
	serviceURL := c.cfg.Endpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", c.cfg.Account)
	}
	return azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.backend == nil {
		return fmt.Errorf("connector not set up")
	}
	ok, err := c.backend.exists(ctx)
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
	if c.backend == nil {
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
	obj, err := c.backend.open(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	return columnsOf(c.cfg.Format, obj)
}

// firstObject returns the first non-directory object key under the prefix.
func (c *Connector) firstObject(ctx context.Context) (string, bool, error) {
	var key string
	var found bool
	err := c.backend.list(ctx, func(o objectInfo) error {
		key, found = o.Key, true
		return errStopList
	})
	if err != nil {
		return "", false, err
	}
	return key, found, nil
}

// base returns an object's file name for diagnostics.
func base(key string) string { return path.Base(key) }

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Object Storage source (S3-compatible or Azure Blob)",
  "type": "object",
  "required": ["bucket"],
  "properties": {
    "provider": {"type": "string", "title": "Provider", "default": "s3", "enum": ["s3", "azure"]},
    "bucket": {"type": "string", "title": "Bucket or container"},
    "prefix": {"type": "string", "title": "Key prefix"},
    "endpoint": {"type": "string", "title": "Endpoint", "description": "s3: s3.amazonaws.com, host:9000. azure: optional custom blob endpoint"},
    "region": {"type": "string", "title": "Region (s3)"},
    "access_key": {"type": "string", "title": "Access key (s3)"},
    "secret_key": {"type": "string", "title": "Secret key (s3)", "format": "password"},
    "use_ssl": {"type": "boolean", "title": "Use TLS (s3)", "default": true},
    "connection_string": {"type": "string", "title": "Connection string (azure)", "format": "password"},
    "account": {"type": "string", "title": "Account (azure)"},
    "account_key": {"type": "string", "title": "Account key (azure)", "format": "password"},
    "format": {"type": "string", "title": "Object format", "default": "ndjson", "enum": ["ndjson", "csv"]},
    "stream": {"type": "string", "title": "Stream name"}
  }
}`
