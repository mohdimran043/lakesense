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

type ndjsonReader struct{ dir string }

func newNDJSONReader(dir string) (*ndjsonReader, error) {
	if dir == "" {
		return nil, fmt.Errorf("ndjson reader requires a path")
	}
	return &ndjsonReader{dir: dir}, nil
}

func (r *ndjsonReader) OpenRead(_ context.Context, stream model.Stream, destTable string) (StreamReader, error) {
	name := destTable
	if name == "" {
		name = stream.ID()
	}
	return &ndjsonStreamReader{path: filepath.Join(r.dir, name+".ndjson")}, nil
}

func (r *ndjsonReader) Close(context.Context) error { return nil }

type ndjsonStreamReader struct{ path string }

func (s *ndjsonStreamReader) Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // empty destination
		}
		return fmt.Errorf("open %s: %w", s.path, err)
	}
	defer func() { _ = f.Close() }()

	var all []sdk.Row
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var row sdk.Row
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			continue // tolerate a torn trailing line
		}
		all = append(all, row)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", s.path, err)
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

func (s *ndjsonStreamReader) Close(context.Context) error { return nil }
