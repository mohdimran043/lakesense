package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// cdc.go implements sdk.ChangeStreamer via PostgreSQL logical replication
// (pgoutput, protocol v1). Design follows docs/analysis/postgres-connector.md
// §2: slot/publication posture, snapshot-before-backfill anchoring, and the
// ack-before-state trick — keepalive replies during a run always report the
// run's START position (never the position actually reached), so the slot's
// confirmed_flush_lsn cannot advance mid-run. Only after every change up to
// the target has been handed to the caller do we send a real status update
// and poll the slot until confirmed_flush_lsn catches up. A crash therefore
// always leaves the slot at-or-behind durable state, never ahead of it (safe
// replay, never data loss).

// UnavailableTOASTValue marks an unchanged TOASTed column in a REPLICA
// IDENTITY DEFAULT update: Postgres does not resend it, and the reference
// project's silent-null substitution is a correctness bug we deliberately
// avoid. Downstream writers must treat this sentinel as "no information",
// never as an actual value.
const UnavailableTOASTValue = "__lakesense_unavailable_toast__"

const (
	cdcPluginProtoVersion  = "1"
	defaultCDCIdleTimeoutS = 30
	standbyMessageInterval = 10 * time.Second
	slotAckTimeout         = 30 * time.Second
)

func (c *Connector) cdcSlotName() string {
	if c.cfg.CDCSlotName != "" {
		return c.cfg.CDCSlotName
	}
	return "lakesense_" + sanitizeIdentPart(c.cfg.Database)
}

func (c *Connector) cdcPublicationName() string {
	if c.cfg.CDCPublicationName != "" {
		return c.cfg.CDCPublicationName
	}
	return "lakesense_pub"
}

func (c *Connector) cdcAutoCreate() bool {
	return c.cfg.CDCAutoCreate == nil || *c.cfg.CDCAutoCreate
}

func (c *Connector) cdcIdleTimeout() time.Duration {
	if c.cfg.CDCIdleTimeoutSeconds > 0 {
		return time.Duration(c.cfg.CDCIdleTimeoutSeconds) * time.Second
	}
	return defaultCDCIdleTimeoutS * time.Second
}

// sanitizeIdentPart keeps generated slot names within Postgres identifier
// rules (lowercase letters, digits, underscore).
func sanitizeIdentPart(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// replicationDSN adds replication=database, switching the connection into
// replication-command mode.
func (c *Connector) replicationDSN() string {
	base := c.cfg.dsn()
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	return base + sep + "replication=database"
}

func (c *Connector) openReplicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, c.replicationDSN())
	if err != nil {
		return nil, fmt.Errorf("open replication connection: %w", err)
	}
	return conn, nil
}

// PrepareCDC implements sdk.ChangeStreamer: validates wal_level, ensures the
// publication and slot exist (creating them when permitted), and returns the
// anchor position. Called BEFORE backfill so everything after the anchor is
// replayable regardless of when backfill finishes.
func (c *Connector) PrepareCDC(ctx context.Context, streams []model.Stream) (map[string]string, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("connector not set up")
	}

	var walLevel string
	if err := c.pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return nil, fmt.Errorf("read wal_level: %w", err)
	}
	if walLevel != "logical" {
		return nil, fmt.Errorf("wal_level is %q, CDC requires \"logical\" (set wal_level=logical and restart PostgreSQL)", walLevel)
	}

	if err := c.ensurePublication(ctx, streams); err != nil {
		return nil, err
	}

	pos, err := c.ensureSlot(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"lsn": pos.String()}, nil
}

// ensurePublication creates the publication (or adds missing tables to an
// existing one) when auto-create is allowed; otherwise it trusts a
// manually-managed publication as-is.
func (c *Connector) ensurePublication(ctx context.Context, streams []model.Stream) error {
	pub := c.cdcPublicationName()
	var exists bool
	if err := c.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", pub).Scan(&exists); err != nil {
		return fmt.Errorf("check publication %q: %w", pub, err)
	}

	if !exists {
		if !c.cdcAutoCreate() {
			return fmt.Errorf("publication %q does not exist and cdc_auto_create is false; create it manually with a CREATE PUBLICATION %s FOR TABLE statement", pub, pub)
		}
		if len(streams) == 0 {
			return fmt.Errorf("cannot create publication %q: no streams selected for CDC", pub)
		}
		tables := make([]string, len(streams))
		for i, s := range streams {
			tables[i] = qualifiedTable(s)
		}
		stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdent(pub), strings.Join(tables, ", "))
		if _, err := c.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create publication %q: %w", pub, err)
		}
		return nil
	}

	if !c.cdcAutoCreate() {
		return nil
	}
	for _, s := range streams {
		stmt := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", quoteIdent(pub), qualifiedTable(s))
		if _, err := c.pool.Exec(ctx, stmt); err != nil && !isDuplicateObject(err) {
			return fmt.Errorf("add %s to publication %q: %w", s.ID(), pub, err)
		}
	}
	return nil
}

// isDuplicateObject reports SQLSTATE 42710 (table already a publication member).
func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42710"
}

// ensureSlot returns the resume position of an existing slot, or creates one
// and returns its consistent point.
func (c *Connector) ensureSlot(ctx context.Context) (pglogrepl.LSN, error) {
	slot := c.cdcSlotName()
	var confirmed, restart *string
	var plugin string
	err := c.pool.QueryRow(ctx,
		`SELECT plugin, confirmed_flush_lsn::text, restart_lsn::text
		 FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&plugin, &confirmed, &restart)
	switch {
	case err == nil:
		if plugin != "pgoutput" {
			return 0, fmt.Errorf("replication slot %q exists with plugin %q, expected \"pgoutput\"", slot, plugin)
		}
		lsnText := confirmed
		if lsnText == nil {
			lsnText = restart
		}
		if lsnText == nil {
			return 0, fmt.Errorf("replication slot %q has no usable LSN yet", slot)
		}
		return pglogrepl.ParseLSN(*lsnText)
	case errors.Is(err, pgx.ErrNoRows):
		// fall through to create
	default:
		return 0, fmt.Errorf("check replication slot %q: %w", slot, err)
	}

	if !c.cdcAutoCreate() {
		return 0, fmt.Errorf("replication slot %q does not exist and cdc_auto_create is false; create it manually with output plugin \"pgoutput\"", slot)
	}

	replConn, err := c.openReplicationConn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = replConn.Close(context.WithoutCancel(ctx)) }()

	result, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		return 0, fmt.Errorf("create replication slot %q: %w", slot, err)
	}
	return pglogrepl.ParseLSN(result.ConsistentPoint)
}

// StreamChanges implements sdk.ChangeStreamer: replays pgoutput messages from
// position up to the server's WAL position observed at call time (a bounded
// micro-batch, not a daemon), decoding rows through the same value
// normalization full-load reads use.
func (c *Connector) StreamChanges(ctx context.Context, streams []model.Stream, position map[string]string, emit sdk.ChangeFunc) (map[string]string, error) {
	startLSN, err := pglogrepl.ParseLSN(position["lsn"])
	if err != nil {
		return nil, fmt.Errorf("parse CDC position %q: %w", position["lsn"], err)
	}

	replConn, err := c.openReplicationConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = replConn.Close(context.WithoutCancel(ctx)) }()

	sys, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		return nil, fmt.Errorf("identify system: %w", err)
	}
	targetLSN := sys.XLogPos
	if targetLSN <= startLSN {
		return position, nil // nothing new since the last run
	}

	streamSet := make(map[string]bool, len(streams))
	for _, s := range streams {
		streamSet[s.ID()] = true
	}

	pluginArgs := []string{
		fmt.Sprintf("proto_version '%s'", cdcPluginProtoVersion),
		fmt.Sprintf("publication_names '%s'", c.cdcPublicationName()),
	}
	if err := pglogrepl.StartReplication(ctx, replConn, c.cdcSlotName(), startLSN,
		pglogrepl.StartReplicationOptions{Mode: pglogrepl.LogicalReplication, PluginArgs: pluginArgs}); err != nil {
		return nil, fmt.Errorf("start replication: %w", err)
	}

	dec := &cdcDecoder{relations: map[uint32]*pglogrepl.RelationMessage{}, streams: streamSet, typeMap: pgtype.NewMap()}
	clientPos := startLSN
	nextStandby := time.Now().Add(standbyMessageInterval)
	lastActivity := time.Now()
	idleTimeout := c.cdcIdleTimeout()

	for {
		if time.Now().After(nextStandby) {
			// Ack-before-state: report the START position, never clientPos,
			// so the slot cannot advance mid-run.
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: startLSN}); err != nil {
				return nil, fmt.Errorf("send keepalive: %w", err)
			}
			nextStandby = time.Now().Add(standbyMessageInterval)
		}

		recvCtx, cancel := context.WithTimeout(ctx, standbyMessageInterval)
		msg, err := replConn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				if clientPos >= targetLSN {
					break
				}
				if time.Since(lastActivity) > idleTimeout {
					return nil, fmt.Errorf("no CDC traffic within %s while waiting to reach target LSN %s (at %s)", idleTimeout, targetLSN, clientPos)
				}
				continue
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("receive replication message: %w", err)
		}
		lastActivity = time.Now()

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: startLSN}); err != nil {
					return nil, fmt.Errorf("reply to keepalive: %w", err)
				}
				nextStandby = time.Now().Add(standbyMessageInterval)
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("parse XLogData: %w", err)
			}
			if err := dec.handle(ctx, xld, emit); err != nil {
				return nil, err
			}
			if end := xld.WALStart + pglogrepl.LSN(len(xld.WALData)); end > clientPos {
				clientPos = end
			}
		}

		if clientPos >= targetLSN {
			break
		}
	}

	// Only now — after every change up to target has been handed to the
	// caller — send the real ack and confirm the server applied it.
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientPos, ReplyRequested: true}); err != nil {
		return nil, fmt.Errorf("send final standby status update: %w", err)
	}
	if err := c.waitForSlotAck(ctx, clientPos); err != nil {
		return nil, err
	}

	return map[string]string{"lsn": clientPos.String()}, nil
}

// waitForSlotAck polls pg_replication_slots until confirmed_flush_lsn reaches
// want — the durability check behind ack-before-state.
func (c *Connector) waitForSlotAck(ctx context.Context, want pglogrepl.LSN) error {
	deadline := time.Now().Add(slotAckTimeout)
	for {
		var confirmed *string
		err := c.pool.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", c.cdcSlotName()).Scan(&confirmed)
		if err != nil {
			return fmt.Errorf("poll slot ack: %w", err)
		}
		if confirmed != nil {
			if got, perr := pglogrepl.ParseLSN(*confirmed); perr == nil && got >= want {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("slot %q did not confirm LSN %s within %s", c.cdcSlotName(), want, slotAckTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// cdcDecoder tracks relation metadata across a StreamChanges run and turns
// pgoutput messages into sdk.Change events for the streams under sync.
type cdcDecoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	streams   map[string]bool // "namespace.name" of streams under sync
	typeMap   *pgtype.Map
	txTime    time.Time
}

func (d *cdcDecoder) handle(ctx context.Context, xld pglogrepl.XLogData, emit sdk.ChangeFunc) error {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("decode logical replication message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		d.txTime = m.CommitTime
	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m
	case *pglogrepl.InsertMessage:
		return d.emitChange(ctx, m.RelationID, sdk.ChangeInsert, xld.WALStart, m.Tuple, emit)
	case *pglogrepl.UpdateMessage:
		return d.emitChange(ctx, m.RelationID, sdk.ChangeUpdate, xld.WALStart, m.NewTuple, emit)
	case *pglogrepl.DeleteMessage:
		return d.emitChange(ctx, m.RelationID, sdk.ChangeDelete, xld.WALStart, m.OldTuple, emit)
	case *pglogrepl.TruncateMessage:
		// v0.1: truncate has no row-level representation; the next full verify
		// diverges rather than a silent gap. Roadmapped: dedicated event.
	}
	return nil
}

func (d *cdcDecoder) emitChange(ctx context.Context, relationID uint32, kind sdk.ChangeKind, walStart pglogrepl.LSN, tuple *pglogrepl.TupleData, emit sdk.ChangeFunc) error {
	rel, ok := d.relations[relationID]
	if !ok {
		return fmt.Errorf("CDC message references unknown relation ID %d (no preceding Relation message)", relationID)
	}
	streamID := model.StreamID(rel.Namespace, rel.RelationName)
	if !d.streams[streamID] {
		return nil // publication may include tables outside this run's selection
	}
	if tuple == nil {
		return fmt.Errorf("%s on %s carries no tuple data (check REPLICA IDENTITY)", kind, streamID)
	}

	row, err := d.decodeTuple(rel, tuple)
	if err != nil {
		return fmt.Errorf("decode %s tuple for %s: %w", kind, streamID, err)
	}

	return emit(ctx, sdk.Change{
		StreamID:  streamID,
		Kind:      kind,
		Data:      row,
		Timestamp: d.txTime,
		Position:  map[string]string{"lsn": walStart.String()},
	})
}

func (d *cdcDecoder) decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (sdk.Row, error) {
	row := make(sdk.Row, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break // defensive: relation/tuple column-count mismatch
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[name] = nil
		case pglogrepl.TupleDataTypeToast:
			row[name] = UnavailableTOASTValue
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			format := int16(pgtype.TextFormatCode)
			if col.DataType == pglogrepl.TupleDataTypeBinary {
				format = pgtype.BinaryFormatCode
			}
			var v any
			if err := d.typeMap.Scan(rel.Columns[i].DataType, format, col.Data, &v); err != nil {
				return nil, fmt.Errorf("column %q: %w", name, err)
			}
			row[name] = NormalizeValue(v)
		default:
			return nil, fmt.Errorf("column %q: unknown tuple data type %q", name, col.DataType)
		}
	}
	return row, nil
}
