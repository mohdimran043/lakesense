package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader. MongoDB documents have no single
// integer key to partition on cheaply, so v1 returns one open chunk — a correct,
// unpartitioned scan ordered by _id (resumable by _id range is a later refinement).
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: streams the whole collection ordered by _id.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	cur, err := c.db().Collection(stream.Name).Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return fmt.Errorf("find %s: %w", stream.ID(), err)
	}
	defer func() { _ = cur.Close(ctx) }()
	return scanCursor(ctx, cur, stream, emit)
}

// scanCursor decodes each document into an engine row.
func scanCursor(ctx context.Context, cur cursor, stream model.Stream, emit sdk.RowFunc) error {
	for cur.Next(ctx) {
		var doc bson.D
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode document: %w", err)
		}
		if err := emit(ctx, docToRow(doc)); err != nil {
			return err
		}
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate %s: %w", stream.ID(), err)
	}
	return nil
}

// cursor is the subset of *mongo.Cursor the scanner needs (also satisfied by fakes).
type cursor interface {
	Next(context.Context) bool
	Decode(any) error
	Err() error
}

// docToRow flattens a BSON document into an engine row: top-level fields become
// columns; nested documents/arrays are JSON-encoded; ObjectIDs and dates become
// stable strings so the destination and checksum see readable, deterministic values.
func docToRow(doc bson.D) sdk.Row {
	row := make(sdk.Row, len(doc))
	for _, e := range doc {
		row[e.Key] = convertValue(e.Value)
	}
	return row
}

// convertValue normalizes one BSON value to a JSON-friendly lake value.
func convertValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case primitive.ObjectID:
		return x.Hex()
	case primitive.DateTime:
		return x.Time().UTC().Format(time.RFC3339Nano)
	case primitive.Decimal128:
		return x.String()
	case primitive.Binary:
		return x.Data
	case bson.D, bson.A, bson.M:
		b, err := bson.MarshalExtJSON(bson.M{"v": v}, false, false)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		// Unwrap the {"v": …} envelope to the value's canonical JSON form.
		var wrap map[string]json.RawMessage
		if json.Unmarshal(b, &wrap) == nil {
			if raw, ok := wrap["v"]; ok {
				return string(raw)
			}
		}
		return string(b)
	default:
		return v
	}
}
