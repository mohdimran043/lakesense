package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestCDCConfigDefaultsAndOverrides(t *testing.T) {
	base := &Connector{cfg: Config{Database: "My-App DB"}}
	assert.Equal(t, "lakesense_my_app_db", base.cdcSlotName(), "generated slot name is sanitized to Postgres identifier rules")
	assert.Equal(t, "lakesense_pub", base.cdcPublicationName())
	assert.True(t, base.cdcAutoCreate(), "auto-create defaults on when unset")
	assert.Equal(t, 30*time.Second, base.cdcIdleTimeout())

	no := false
	over := &Connector{cfg: Config{
		Database:              "db",
		CDCSlotName:           "custom_slot",
		CDCPublicationName:    "custom_pub",
		CDCAutoCreate:         &no,
		CDCIdleTimeoutSeconds: 5,
	}}
	assert.Equal(t, "custom_slot", over.cdcSlotName())
	assert.Equal(t, "custom_pub", over.cdcPublicationName())
	assert.False(t, over.cdcAutoCreate())
	assert.Equal(t, 5*time.Second, over.cdcIdleTimeout())
}

func TestReplicationDSNAddsReplicationParam(t *testing.T) {
	c := &Connector{}
	require.NoError(t, (&Config{Host: "h", Database: "d", User: "u"}).validate())
	c.cfg = Config{Host: "h", Database: "d", User: "u"}
	require.NoError(t, c.cfg.validate())
	dsn := c.replicationDSN()
	assert.Contains(t, dsn, "replication=database")
	assert.Contains(t, dsn, "?", "the base DSN already carries a query string, so the replication param joins with &")
	assert.Contains(t, dsn, "sslmode=")
}

func TestCDCCapabilityDeclared(t *testing.T) {
	spec := New().Spec()
	assert.Contains(t, spec.Capabilities, sdk.CapCDC, "postgres declares CDC and implements ChangeStreamer")
	require.NoError(t, sdk.ValidateCapabilities(New()))

	_, ok := New().(sdk.ChangeStreamer)
	assert.True(t, ok, "postgres connector implements ChangeStreamer")
}

// relation builds a pgoutput RelationMessage for public.users(id int4, name text).
func relation(id uint32) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   id,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID, Flags: 1},
			{Name: "name", DataType: pgtype.TextOID},
		},
	}
}

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(s)), Data: []byte(s)}
}

func TestDecodeTuple(t *testing.T) {
	dec := &cdcDecoder{typeMap: pgtype.NewMap()}
	rel := relation(1)

	t.Run("text values normalize by oid", func(t *testing.T) {
		row, err := dec.decodeTuple(rel, &pglogrepl.TupleData{
			ColumnNum: 2,
			Columns:   []*pglogrepl.TupleDataColumn{textCol("42"), textCol("alice")},
		})
		require.NoError(t, err)
		assert.Equal(t, int32(42), row["id"])
		assert.Equal(t, "alice", row["name"])
	})

	t.Run("null column", func(t *testing.T) {
		row, err := dec.decodeTuple(rel, &pglogrepl.TupleData{
			ColumnNum: 2,
			Columns:   []*pglogrepl.TupleDataColumn{textCol("7"), {DataType: pglogrepl.TupleDataTypeNull}},
		})
		require.NoError(t, err)
		assert.Equal(t, int32(7), row["id"])
		assert.Nil(t, row["name"])
	})

	t.Run("unchanged toast becomes explicit sentinel not null", func(t *testing.T) {
		row, err := dec.decodeTuple(rel, &pglogrepl.TupleData{
			ColumnNum: 2,
			Columns:   []*pglogrepl.TupleDataColumn{textCol("9"), {DataType: pglogrepl.TupleDataTypeToast}},
		})
		require.NoError(t, err)
		assert.Equal(t, UnavailableTOASTValue, row["name"])
		assert.NotNil(t, row["name"], "unavailable TOAST must never be silently nulled")
	})

	t.Run("column count mismatch does not panic", func(t *testing.T) {
		row, err := dec.decodeTuple(rel, &pglogrepl.TupleData{
			ColumnNum: 3,
			Columns:   []*pglogrepl.TupleDataColumn{textCol("1"), textCol("bob"), textCol("extra")},
		})
		require.NoError(t, err)
		assert.Len(t, row, 2, "extra tuple columns beyond the relation are ignored defensively")
	})
}

func TestEmitChange(t *testing.T) {
	rel := relation(1)
	newDecoder := func() *cdcDecoder {
		return &cdcDecoder{
			relations: map[uint32]*pglogrepl.RelationMessage{1: rel},
			streams:   map[string]bool{"public.users": true},
			typeMap:   pgtype.NewMap(),
			txTime:    time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		}
	}
	tuple := &pglogrepl.TupleData{ColumnNum: 2, Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol("carol")}}

	t.Run("emits change with kind position and timestamp", func(t *testing.T) {
		var got []sdk.Change
		err := newDecoder().emitChange(context.Background(), 1, sdk.ChangeInsert, pglogrepl.LSN(0x1A2B), tuple,
			func(_ context.Context, ch sdk.Change) error { got = append(got, ch); return nil })
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, sdk.ChangeInsert, got[0].Kind)
		assert.Equal(t, "public.users", got[0].StreamID)
		assert.Equal(t, int32(1), got[0].Data["id"])
		assert.Equal(t, "carol", got[0].Data["name"])
		assert.Equal(t, pglogrepl.LSN(0x1A2B).String(), got[0].Position["lsn"])
		assert.Equal(t, time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC), got[0].Timestamp)
	})

	t.Run("skips streams outside the sync selection", func(t *testing.T) {
		d := newDecoder()
		d.streams = map[string]bool{"public.orders": true}
		var got []sdk.Change
		err := d.emitChange(context.Background(), 1, sdk.ChangeInsert, 1, tuple,
			func(_ context.Context, ch sdk.Change) error { got = append(got, ch); return nil })
		require.NoError(t, err)
		assert.Empty(t, got, "publication may cover tables this run did not select")
	})

	t.Run("unknown relation id is a hard error", func(t *testing.T) {
		err := newDecoder().emitChange(context.Background(), 999, sdk.ChangeInsert, 1, tuple,
			func(context.Context, sdk.Change) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown relation ID")
	})

	t.Run("nil tuple (missing replica identity) errors", func(t *testing.T) {
		err := newDecoder().emitChange(context.Background(), 1, sdk.ChangeDelete, 1, nil,
			func(context.Context, sdk.Change) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REPLICA IDENTITY")
	})
}
