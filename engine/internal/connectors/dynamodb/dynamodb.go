// Package dynamodb implements the LakeSense DynamoDB connector: a full-load Scan
// snapshot of one or more tables. DynamoDB items are schemaless apart from their
// key attributes, so each table's schema is the typed key attributes (primary
// key) plus columns inferred by sampling items — nested maps/lists/sets collapse
// to JSON, mixed-typed attributes fall back to string (the document-model
// approach shared with MongoDB). There is no modified-time cursor and change data
// needs DynamoDB Streams, so v1 is full-load only and Beta. Works against Amazon
// DynamoDB and local emulators (DynamoDB Local, LocalStack) via an endpoint
// override. Design follows docs/analysis/other-sources.md.
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "dynamodb"

// Config is the source configuration.
type Config struct {
	Type      string `json:"type,omitempty"`
	Region    string `json:"region,omitempty"`   // default "us-east-1"
	Endpoint  string `json:"endpoint,omitempty"` // override for DynamoDB Local / LocalStack
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	// Table restricts discovery to a single table; empty discovers every table.
	Table string `json:"table,omitempty"`
	// SampleSize is how many items are scanned to infer non-key columns (default 100).
	SampleSize int32 `json:"sample_size,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.SampleSize <= 0 {
		c.SampleSize = 100
	}
	return nil
}

// Connector implements sdk.Connector and sdk.FullLoader.
type Connector struct {
	cfg    Config
	client *dynamodb.Client
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "DynamoDB",
		Capabilities: []sdk.Capability{sdk.CapFullLoad},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(ctx context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse dynamodb config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid dynamodb config: %w", err)
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(c.cfg.Region)}
	if c.cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.cfg.AccessKey, c.cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	c.client = dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if c.cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.cfg.Endpoint)
		}
	})
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	if _, err := c.client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(1)}); err != nil {
		return fmt.Errorf("cannot reach dynamodb: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error { return nil }

// Discover implements sdk.Connector: one stream per table, schema = typed key
// attributes (primary key) plus columns inferred from a sample of items.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	tables, err := c.listTables(ctx)
	if err != nil {
		return nil, err
	}
	streams := make([]model.Stream, 0, len(tables))
	for _, name := range tables {
		schema, err := c.tableSchema(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("schema for %s: %w", name, err)
		}
		streams = append(streams, model.Stream{
			Namespace:          "",
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad},
		})
	}
	return streams, nil
}

// listTables returns the configured table, or every table (paginated).
func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	if c.cfg.Table != "" {
		return []string{c.cfg.Table}, nil
	}
	var out []string
	var start *string
	for {
		res, err := c.client.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: start})
		if err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		out = append(out, res.TableNames...)
		if res.LastEvaluatedTableName == nil {
			break
		}
		start = res.LastEvaluatedTableName
	}
	sort.Strings(out)
	return out, nil
}

// tableSchema builds a table's schema: typed key attributes (primary key) first,
// then attributes discovered by sampling items.
func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	desc, err := c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		return model.Schema{}, fmt.Errorf("describe table: %w", err)
	}
	keyTypes := map[string]model.DataType{}
	for _, ad := range desc.Table.AttributeDefinitions {
		keyTypes[*ad.AttributeName] = scalarType(ad.AttributeType)
	}
	isKey := map[string]bool{}
	var schema model.Schema
	// Key attributes: HASH before RANGE, both primary-key and non-null.
	for _, want := range []types.KeyType{types.KeyTypeHash, types.KeyTypeRange} {
		for _, ks := range desc.Table.KeySchema {
			if ks.KeyType != want {
				continue
			}
			name := *ks.AttributeName
			isKey[name] = true
			schema.Columns = append(schema.Columns, model.Column{
				Name: name, Type: keyTypes[name], PrimaryKey: true, Nullable: false,
			})
		}
	}
	// Non-key attributes from a sample of items.
	inferred, err := c.sampleColumns(ctx, table, isKey)
	if err != nil {
		return model.Schema{}, err
	}
	for _, col := range inferred {
		schema.Columns = append(schema.Columns, col)
	}
	return schema, nil
}

// sampleColumns scans up to SampleSize items and infers the non-key columns,
// merging types across items (conflicts fall back to string).
func (c *Connector) sampleColumns(ctx context.Context, table string, isKey map[string]bool) ([]model.Column, error) {
	res, err := c.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(table),
		Limit:     aws.Int32(c.cfg.SampleSize),
	})
	if err != nil {
		return nil, fmt.Errorf("sample scan: %w", err)
	}
	colTypes := map[string]model.DataType{}
	for _, item := range res.Items {
		for name, av := range item {
			if isKey[name] {
				continue
			}
			colTypes[name] = mergeType(colTypes[name], attrType(av))
		}
	}
	names := make([]string, 0, len(colTypes))
	for n := range colTypes {
		names = append(names, n)
	}
	sort.Strings(names)
	cols := make([]model.Column, len(names))
	for i, n := range names {
		cols[i] = model.Column{Name: n, Type: colTypes[n], Nullable: true}
	}
	return cols, nil
}

// scalarType maps a DynamoDB key attribute type to a lake type.
func scalarType(t types.ScalarAttributeType) model.DataType {
	switch t {
	case types.ScalarAttributeTypeN:
		return model.TypeDecimal
	case types.ScalarAttributeTypeB:
		return model.TypeBinary
	default: // S
		return model.TypeString
	}
}

// mergeType combines an attribute's observed types across items: equal types
// stay; any conflict falls back to string (matching MongoDB's mixed-type rule).
func mergeType(existing, next model.DataType) model.DataType {
	if existing == "" || existing == next {
		return next
	}
	return model.TypeString
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "DynamoDB source",
  "type": "object",
  "properties": {
    "region": {"type": "string", "title": "Region", "default": "us-east-1"},
    "endpoint": {"type": "string", "title": "Endpoint", "description": "override for DynamoDB Local / LocalStack"},
    "access_key": {"type": "string", "title": "Access key"},
    "secret_key": {"type": "string", "title": "Secret key", "format": "password"},
    "table": {"type": "string", "title": "Table", "description": "empty = all tables"},
    "sample_size": {"type": "integer", "title": "Schema sample size", "default": 100}
  }
}`
