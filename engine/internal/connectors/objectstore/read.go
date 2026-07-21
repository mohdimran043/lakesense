package objectstore

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader: a single chunk (objects are streamed
// within ReadChunk).
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: reads every object under the prefix.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	_, err := c.readObjects(ctx, time.Time{}, emit)
	return err
}

// MaxCursor implements sdk.IncrementalReader: incremental is by object
// modified-time, so the watermark is the newest object's timestamp.
func (c *Connector) MaxCursor(ctx context.Context, _ model.Stream, _ string) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	var newest time.Time
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range c.client.ListObjects(listCtx, c.cfg.Bucket, minio.ListObjectsOptions{Prefix: c.cfg.Prefix, Recursive: true}) {
		if o.Err != nil {
			return "", fmt.Errorf("list objects: %w", o.Err)
		}
		if o.LastModified.After(newest) {
			newest = o.LastModified
		}
	}
	if newest.IsZero() {
		return "", nil
	}
	return newest.UTC().Format(time.RFC3339Nano), nil
}

// ReadIncrement implements sdk.IncrementalReader: rows from objects modified
// after the since watermark; returns the newest object time seen.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, _, since string, emit sdk.RowFunc) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	var sinceT time.Time
	if since != "" {
		if t, err := time.Parse(time.RFC3339Nano, since); err == nil {
			sinceT = t
		}
	}
	newest, err := c.readObjects(ctx, sinceT, emit)
	if err != nil {
		return "", err
	}
	if newest.IsZero() {
		return since, nil
	}
	return newest.UTC().Format(time.RFC3339Nano), nil
}

// readObjects lists and parses every object under the prefix modified after
// `after` (zero = all), emitting each record. It returns the newest object time.
func (c *Connector) readObjects(ctx context.Context, after time.Time, emit sdk.RowFunc) (time.Time, error) {
	var newest time.Time
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range c.client.ListObjects(listCtx, c.cfg.Bucket, minio.ListObjectsOptions{Prefix: c.cfg.Prefix, Recursive: true}) {
		if o.Err != nil {
			return newest, fmt.Errorf("list objects: %w", o.Err)
		}
		if strings.HasSuffix(o.Key, "/") || o.Size == 0 {
			continue
		}
		if !after.IsZero() && !o.LastModified.After(after) {
			continue
		}
		if err := c.readObject(ctx, o.Key, emit); err != nil {
			return newest, err
		}
		if o.LastModified.After(newest) {
			newest = o.LastModified
		}
	}
	return newest, nil
}

// readObject streams one object's records to emit.
func (c *Connector) readObject(ctx context.Context, key string, emit sdk.RowFunc) error {
	obj, err := c.client.GetObject(ctx, c.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("open %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	if err := parseRecords(ctx, c.cfg.Format, obj, emit); err != nil {
		return fmt.Errorf("parse %s: %w", base(key), err)
	}
	return nil
}

// columnsOf infers the column names of an object by format.
func columnsOf(format string, r io.Reader) ([]string, error) {
	switch format {
	case "csv":
		header, err := csv.NewReader(r).Read()
		if err != nil {
			if err == io.EOF {
				return []string{}, nil
			}
			return nil, err
		}
		return header, nil
	default: // ndjson
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		if sc.Scan() {
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				return nil, err
			}
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return keys, nil
		}
		return []string{}, sc.Err()
	}
}

// parseRecords parses an object's records (ndjson or csv) into engine rows.
func parseRecords(ctx context.Context, format string, r io.Reader, emit sdk.RowFunc) error {
	if format == "csv" {
		cr := csv.NewReader(r)
		header, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		for {
			rec, err := cr.Read()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			row := make(sdk.Row, len(header))
			for i, h := range header {
				if i < len(rec) {
					row[h] = rec[i]
				}
			}
			if err := emit(ctx, row); err != nil {
				return err
			}
		}
	}
	// ndjson
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 32<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			return err
		}
		row := make(sdk.Row, len(m))
		for k, v := range m {
			row[k] = normalizeValue(v)
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return sc.Err()
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
