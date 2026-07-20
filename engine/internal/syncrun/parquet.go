package syncrun

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

// parquetWriter writes each stream as a directory of immutable part-files. One
// part-file is finalized per Flush boundary (footer + fsync), so a completed
// chunk maps to a durable, readable file — the write-ahead discipline the
// orchestrator already enforces, satisfied without changes to the runner.
type parquetWriter struct {
	dir string
}

func newParquetWriter(dir string) (*parquetWriter, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet destination requires a path")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create destination dir %s: %w", dir, err)
	}
	return &parquetWriter{dir: dir}, nil
}

func (w *parquetWriter) Open(_ context.Context, stream model.Stream, destTable string, truncate bool) (StreamWriter, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	streamDir := filepath.Join(w.dir, name)
	if err := os.MkdirAll(streamDir, 0o755); err != nil {
		return nil, fmt.Errorf("create stream dir %s: %w", streamDir, err)
	}
	if truncate {
		existing, _ := filepath.Glob(filepath.Join(streamDir, "*.parquet"))
		for _, p := range existing {
			if err := os.Remove(p); err != nil {
				return nil, fmt.Errorf("truncate %s: %w", p, err)
			}
		}
	}
	schema, names, colType := buildParquetSchema(stream)
	colIndex := make(map[string]int, len(names))
	for i, n := range names {
		colIndex[n] = i
	}
	return &parquetStreamWriter{
		dir:      streamDir,
		syncID:   events.NewSyncID(),
		schema:   schema,
		names:    names,
		colIndex: colIndex,
		colType:  colType,
		columns:  dataColumns(stream),
	}, nil
}

func (w *parquetWriter) Close(context.Context) error { return nil }

type parquetStreamWriter struct {
	dir      string
	syncID   string
	schema   *parquet.Schema
	names    []string
	colIndex map[string]int
	colType  map[string]model.DataType
	columns  []string

	pending []parquet.Row
	seq     int
	digest  digest
	bytes   int64
	closed  bool
}

func (s *parquetStreamWriter) WriteRow(_ context.Context, row sdk.Row) error {
	h, err := hashRow(row, s.columns)
	if err != nil {
		return err
	}
	s.digest.add(h)
	s.pending = append(s.pending, rowToParquet(row, s.colIndex, s.colType, len(s.names)))
	return nil
}

// Flush finalizes the currently-buffered rows as one immutable part-file. It is
// a no-op when nothing is pending, so repeated Flush calls never emit empties.
func (s *parquetStreamWriter) Flush(context.Context) error {
	if s.closed {
		return fmt.Errorf("parquet stream writer for %s already closed", s.dir)
	}
	if len(s.pending) == 0 {
		return nil
	}
	path := filepath.Join(s.dir, fmt.Sprintf("%s-%04d.parquet", s.syncID, s.seq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open part %s: %w", path, err)
	}
	pw := parquet.NewWriter(f, s.schema)
	if _, err := pw.WriteRows(s.pending); err != nil {
		_ = f.Close()
		return fmt.Errorf("write parquet rows to %s: %w", path, err)
	}
	if err := pw.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("finalize parquet %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if st, err := os.Stat(path); err == nil {
		s.bytes += st.Size()
	}
	s.pending = s.pending[:0]
	s.seq++
	return nil
}

func (s *parquetStreamWriter) Close(ctx context.Context) (WriteResult, error) {
	if s.closed {
		return WriteResult{}, fmt.Errorf("parquet stream writer for %s already closed", s.dir)
	}
	if err := s.Flush(ctx); err != nil {
		return WriteResult{}, err
	}
	s.closed = true
	return WriteResult{Rows: s.digest.Rows(), Bytes: s.bytes, Checksum: s.digest.Hex()}, nil
}
