package syncrun

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

type parquetReader struct{ dir string }

func newParquetReader(dir string) (*parquetReader, error) {
	if dir == "" {
		return nil, fmt.Errorf("parquet reader requires a path")
	}
	return &parquetReader{dir: dir}, nil
}

func (r *parquetReader) OpenRead(_ context.Context, stream model.Stream, destTable string) (StreamReader, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	return &parquetStreamReader{dir: filepath.Join(r.dir, name)}, nil
}

func (r *parquetReader) Close(context.Context) error { return nil }

type parquetStreamReader struct{ dir string }

func (s *parquetStreamReader) Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error {
	parts, err := filepath.Glob(filepath.Join(s.dir, "*.parquet"))
	if err != nil {
		return fmt.Errorf("glob %s: %w", s.dir, err)
	}
	sort.Strings(parts) // ascending: later part-files carry newer writes
	var all []sdk.Row
	for _, p := range parts {
		rows, err := readParquetPart(p)
		if err != nil {
			return err
		}
		all = append(all, rows...)
	}
	for _, row := range resolveCurrentState(all) {
		if !inRange(row, bounds) {
			continue
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *parquetStreamReader) Close(context.Context) error { return nil }

// readParquetPart reads one part-file, recovering column names from its footer
// schema so decoding never depends on the writer's in-memory ordering.
func readParquetPart(path string) ([]sdk.Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return nil, fmt.Errorf("parse parquet %s: %w", path, err)
	}
	cols := pf.Schema().Columns()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c[len(c)-1]
	}
	reader := parquet.NewGenericReader[any](pf)
	defer func() { _ = reader.Close() }()

	var out []sdk.Row
	buf := make([]parquet.Row, 256)
	for {
		n, err := reader.ReadRows(buf)
		for i := 0; i < n; i++ {
			out = append(out, parquetToRow(buf[i], names))
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read rows %s: %w", path, err)
		}
	}
	return out, nil
}
