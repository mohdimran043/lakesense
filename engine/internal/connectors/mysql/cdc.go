package mysql

import (
	"context"
	"fmt"
	"strconv"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// PrepareCDC implements sdk.ChangeStreamer: it captures the current binlog
// coordinate as the replication anchor BEFORE any backfill, so nothing committed
// after this point is missed. Row-based binlog with FULL row images is required
// (the connector documents this in Check-style errors if a change can't be
// decoded).
func (c *Connector) PrepareCDC(ctx context.Context, _ []model.Stream) (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	file, pos, err := c.masterPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("prepare CDC: %w", err)
	}
	return map[string]string{"file": file, "pos": strconv.FormatUint(uint64(pos), 10)}, nil
}

// StreamChanges implements sdk.ChangeStreamer: it replays binlog row events from
// position up to the current coordinate (captured at call time), emitting one
// Change per affected row, and returns the final coordinate. Reading past the
// bounded target is avoided by stopping once the stream reaches it; an idle
// timeout guards against a stall when the primary is quiet.
func (c *Connector) StreamChanges(ctx context.Context, streams []model.Stream, position map[string]string, emit sdk.ChangeFunc) (map[string]string, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	startFile := position["file"]
	startPos, _ := strconv.ParseUint(position["pos"], 10, 32)
	if startFile == "" {
		f, p, err := c.masterPosition(ctx)
		if err != nil {
			return nil, err
		}
		startFile, startPos = f, uint64(p)
	}

	// Bounded target: everything committed up to now.
	targetFile, targetPos, err := c.masterPosition(ctx)
	if err != nil {
		return nil, err
	}
	if reached(startFile, uint32(startPos), targetFile, targetPos) {
		return position, nil // already caught up — nothing to replay
	}

	selected := selectedByID(streams)

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: c.cfg.ServerID,
		Flavor:   "mysql",
		Host:     c.cfg.Host,
		Port:     uint16(c.cfg.Port),
		User:     c.cfg.User,
		Password: c.cfg.Password,
	})
	defer syncer.Close()

	streamer, err := syncer.StartSync(gomysql.Position{Name: startFile, Pos: uint32(startPos)})
	if err != nil {
		return nil, fmt.Errorf("start binlog sync at %s:%d: %w", startFile, startPos, err)
	}

	idle := time.Duration(30) * time.Second
	curFile, curPos := startFile, uint32(startPos)

	for {
		evCtx, cancel := context.WithTimeout(ctx, idle)
		ev, err := streamer.GetEvent(evCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Idle timeout before reaching the target: stop where we are.
			break
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			curFile = string(e.NextLogName)
			curPos = uint32(e.Position)
			continue
		case *replication.RowsEvent:
			if err := emitRows(ctx, ev.Header.EventType, e, selected, emit); err != nil {
				return nil, err
			}
		}
		curPos = ev.Header.LogPos

		if reached(curFile, curPos, targetFile, targetPos) {
			break
		}
	}

	return map[string]string{"file": curFile, "pos": strconv.FormatUint(uint64(curPos), 10)}, nil
}

// emitRows turns one binlog RowsEvent into engine Changes for the selected
// streams. INSERT emits one Change per row; UPDATE emits the after-image of each
// (before, after) pair; DELETE emits the (identity) row.
func emitRows(ctx context.Context, evType replication.EventType, e *replication.RowsEvent, selected map[string]model.Stream, emit sdk.ChangeFunc) error {
	id := model.StreamID(string(e.Table.Schema), string(e.Table.Table))
	stream, ok := selected[id]
	if !ok {
		return nil // change for a stream not in this run
	}
	cols := stream.Schema.Columns

	switch evType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for _, r := range e.Rows {
			if err := emit(ctx, change(id, sdk.ChangeInsert, rowFromValues(cols, r))); err != nil {
				return err
			}
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			after := e.Rows[i+1]
			if err := emit(ctx, change(id, sdk.ChangeUpdate, rowFromValues(cols, after))); err != nil {
				return err
			}
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for _, r := range e.Rows {
			if err := emit(ctx, change(id, sdk.ChangeDelete, rowFromValues(cols, r))); err != nil {
				return err
			}
		}
	}
	return nil
}

func change(id string, kind sdk.ChangeKind, data sdk.Row) sdk.Change {
	return sdk.Change{StreamID: id, Kind: kind, Data: data, Timestamp: time.Now().UTC()}
}

// rowFromValues zips a binlog row's positional values onto column names,
// normalizing each by the column's lake type.
func rowFromValues(cols []model.Column, values []any) sdk.Row {
	row := make(sdk.Row, len(cols))
	for i, col := range cols {
		if i >= len(values) {
			break
		}
		row[col.Name] = cdcNormalize(values[i], col.Type)
	}
	return row
}

// cdcNormalize coerces a go-mysql binlog value into a JSON-friendly lake value,
// mirroring the full-load normalization so CDC and snapshot rows agree.
func cdcNormalize(v any, lake model.DataType) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if lake == model.TypeBinary {
			return x
		}
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

// masterPosition returns the primary's current binlog file and position.
func (c *Connector) masterPosition(ctx context.Context) (string, uint32, error) {
	// SHOW MASTER STATUS returns variable columns across versions; scan the
	// first two (File, Position) and discard the rest.
	rows, err := c.db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return "", 0, fmt.Errorf("show master status: %w", err)
	}
	defer func() { _ = rows.Close() }()
	colNames, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if !rows.Next() {
		return "", 0, fmt.Errorf("binlog not enabled (SHOW MASTER STATUS empty); set log-bin and binlog-format=ROW")
	}
	cells := make([]any, len(colNames))
	ptrs := make([]any, len(colNames))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return "", 0, err
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	file := asString(cells[0])
	pos64, _ := strconv.ParseUint(asString(cells[1]), 10, 32)
	return file, uint32(pos64), nil
}

func asString(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// reached reports whether (curFile, curPos) has caught up to (file, pos). Binlog
// file names sort lexically, so a plain comparison orders coordinates correctly.
func reached(curFile string, curPos uint32, file string, pos uint32) bool {
	if curFile != file {
		return curFile > file
	}
	return curPos >= pos
}

// selectedByID indexes streams by their "namespace.name" id.
func selectedByID(streams []model.Stream) map[string]model.Stream {
	m := make(map[string]model.Stream, len(streams))
	for _, s := range streams {
		m[s.ID()] = s
	}
	return m
}
