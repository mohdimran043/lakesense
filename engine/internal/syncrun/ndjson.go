package syncrun

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// ndjsonWriter is the interim v0.1 destination: one newline-delimited JSON
// file per stream under a directory. It is rock-solid and dependency-free —
// exactly what the Phase 1 writer brainstorm called for before Parquet — and
// it exercises the full orchestrator/Writer contract so Parquet (2.5) drops in
// without touching the engine core.
type ndjsonWriter struct {
	dir string
}

func newNDJSONWriter(dir string) (*ndjsonWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create destination dir %s: %w", dir, err)
	}
	return &ndjsonWriter{dir: dir}, nil
}

func (w *ndjsonWriter) Open(_ context.Context, stream model.Stream, destTable string, truncate bool) (StreamWriter, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	path := filepath.Join(w.dir, name+".ndjson")
	flags := os.O_CREATE | os.O_WRONLY
	if truncate {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_APPEND
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return &ndjsonStreamWriter{
		path:    path,
		file:    f,
		buf:     bufio.NewWriter(f),
		columns: dataColumns(stream),
	}, nil
}

func (w *ndjsonWriter) Close(context.Context) error { return nil }

type ndjsonStreamWriter struct {
	path    string
	file    *os.File
	buf     *bufio.Writer
	columns []string
	digest  digest
	bytes   int64
	closed  bool
}

func (s *ndjsonStreamWriter) WriteRow(_ context.Context, row sdk.Row) error {
	line, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("encode row for %s: %w", s.path, err)
	}
	n, err := s.buf.Write(line)
	if err != nil {
		return fmt.Errorf("write row to %s: %w", s.path, err)
	}
	if err := s.buf.WriteByte('\n'); err != nil {
		return fmt.Errorf("write newline to %s: %w", s.path, err)
	}
	s.bytes += int64(n) + 1

	h, err := hashRow(row, s.columns)
	if err != nil {
		return err
	}
	s.digest.add(h)
	return nil
}

// Flush makes buffered rows durable (bufio flush + fsync), so a completed-chunk
// marker in state is never ahead of the bytes on disk.
func (s *ndjsonStreamWriter) Flush(context.Context) error {
	if s.closed {
		return fmt.Errorf("stream writer for %s already closed", s.path)
	}
	if err := s.buf.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", s.path, err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", s.path, err)
	}
	return nil
}

func (s *ndjsonStreamWriter) Close(context.Context) (WriteResult, error) {
	if s.closed {
		return WriteResult{}, fmt.Errorf("stream writer for %s already closed", s.path)
	}
	s.closed = true
	if err := s.buf.Flush(); err != nil {
		_ = s.file.Close()
		return WriteResult{}, fmt.Errorf("flush %s: %w", s.path, err)
	}
	if err := s.file.Sync(); err != nil {
		_ = s.file.Close()
		return WriteResult{}, fmt.Errorf("sync %s: %w", s.path, err)
	}
	if err := s.file.Close(); err != nil {
		return WriteResult{}, fmt.Errorf("close %s: %w", s.path, err)
	}
	return WriteResult{Rows: s.digest.Rows(), Bytes: s.bytes, Checksum: s.digest.Hex()}, nil
}
