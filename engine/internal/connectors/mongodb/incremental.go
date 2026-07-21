package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// MaxCursor implements sdk.IncrementalReader: the current maximum cursor value,
// captured before full load so the later increment is gap-free.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	opts := options.FindOne().SetSort(bson.D{{Key: cursorField, Value: -1}})
	var doc bson.D
	err := c.db().Collection(stream.Name).FindOne(ctx, bson.M{}, opts).Decode(&doc)
	if err != nil {
		return "", nil //nolint:nilerr // empty collection / no documents → no watermark, not an error
	}
	for _, e := range doc {
		if e.Key == cursorField {
			return fmt.Sprintf("%v", convertValue(e.Value)), nil
		}
	}
	return "", nil
}

// ReadIncrement implements sdk.IncrementalReader: documents with cursor > since
// (all when since is empty), returning the new high watermark. The since value is
// coerced to the cursor field's stored type (_id → ObjectID, else its raw form).
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	filter := bson.M{}
	if since != "" {
		filter = bson.M{cursorField: bson.M{"$gt": coerceCursor(stream, cursorField, since)}}
	}
	opts := options.Find().SetSort(bson.D{{Key: cursorField, Value: 1}})
	cur, err := c.db().Collection(stream.Name).Find(ctx, filter, opts)
	if err != nil {
		return "", fmt.Errorf("incremental find %s: %w", stream.ID(), err)
	}
	defer func() { _ = cur.Close(ctx) }()

	newCursor := since
	err = scanCursor(ctx, cur, stream, func(ctx context.Context, row sdk.Row) error {
		if v, ok := row[cursorField]; ok && v != nil {
			newCursor = fmt.Sprintf("%v", v)
		}
		return emit(ctx, row)
	})
	if err != nil {
		return "", err
	}
	return newCursor, nil
}

// coerceCursor converts a stored string watermark back into a value MongoDB can
// compare against the field. _id watermarks are hex ObjectIDs; everything else
// compares as its string form (adequate for timestamps stored as RFC3339 and for
// numeric/string cursors in v1).
func coerceCursor(stream model.Stream, cursorField, since string) any {
	if col, ok := stream.Schema.Column(cursorField); ok && cursorField == "_id" && col.Type == model.TypeString {
		if oid, err := primitive.ObjectIDFromHex(since); err == nil {
			return oid
		}
	}
	return since
}
