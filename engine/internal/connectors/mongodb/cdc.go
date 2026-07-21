package mongodb

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// cdcDrainIdle bounds a CDC micro-batch: once no change arrives within this
// window the stream is considered caught up and StreamChanges returns.
const cdcDrainIdle = 2 * time.Second

// PrepareCDC implements sdk.ChangeStreamer: it opens a database-level change
// stream to capture the current resume token as the anchor, then closes it —
// nothing committed after this point is missed. Requires a replica set (change
// streams are unavailable on standalone MongoDB).
func (c *Connector) PrepareCDC(ctx context.Context, _ []model.Stream) (map[string]string, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	cs, err := c.db().Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("open change stream (replica set required for CDC): %w", err)
	}
	defer func() { _ = cs.Close(ctx) }()
	token := cs.ResumeToken()
	if len(token) == 0 {
		return map[string]string{}, nil
	}
	return map[string]string{"token": base64.StdEncoding.EncodeToString(token)}, nil
}

// StreamChanges implements sdk.ChangeStreamer: it resumes the change stream after
// the stored token and drains all currently-available changes for the selected
// collections (a bounded micro-batch — it stops when the stream goes idle),
// returning the new resume token. update/replace carry the full document
// (UpdateLookup); delete carries the identity (_id).
func (c *Connector) StreamChanges(ctx context.Context, streams []model.Stream, position map[string]string, emit sdk.ChangeFunc) (map[string]string, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if tok := position["token"]; tok != "" {
		raw, err := base64.StdEncoding.DecodeString(tok)
		if err != nil {
			return nil, fmt.Errorf("decode resume token: %w", err)
		}
		opts.SetResumeAfter(bson.Raw(raw))
	}

	cs, err := c.db().Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return nil, fmt.Errorf("resume change stream: %w", err)
	}
	defer func() { _ = cs.Close(ctx) }()

	selected := selectedByName(streams)
	newToken := position["token"]

	for {
		drainCtx, cancel := context.WithTimeout(ctx, cdcDrainIdle)
		hasNext := cs.TryNext(drainCtx)
		cancel()
		if !hasNext {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			break // idle: caught up
		}
		var ev changeEvent
		if err := cs.Decode(&ev); err != nil {
			return nil, fmt.Errorf("decode change event: %w", err)
		}
		if stream, ok := selected[ev.NS.Coll]; ok {
			if ch, emitOK := ev.toChange(stream); emitOK {
				if err := emit(ctx, ch); err != nil {
					return nil, err
				}
			}
		}
		if t := cs.ResumeToken(); len(t) > 0 {
			newToken = base64.StdEncoding.EncodeToString(t)
		}
	}

	return map[string]string{"token": newToken}, nil
}

// changeEvent is the subset of a MongoDB change-stream event the connector uses.
type changeEvent struct {
	OperationType string `bson:"operationType"`
	NS            struct {
		Coll string `bson:"coll"`
	} `bson:"ns"`
	FullDocument bson.D `bson:"fullDocument"`
	DocumentKey  bson.D `bson:"documentKey"`
}

// toChange maps a change event to an engine Change. emitOK is false for event
// types the connector does not replicate (e.g. drop, rename).
func (e changeEvent) toChange(stream model.Stream) (sdk.Change, bool) {
	id := stream.ID()
	switch e.OperationType {
	case "insert", "replace":
		return sdk.Change{StreamID: id, Kind: sdk.ChangeInsert, Data: docToRow(e.FullDocument), Timestamp: time.Now().UTC()}, true
	case "update":
		return sdk.Change{StreamID: id, Kind: sdk.ChangeUpdate, Data: docToRow(e.FullDocument), Timestamp: time.Now().UTC()}, true
	case "delete":
		return sdk.Change{StreamID: id, Kind: sdk.ChangeDelete, Data: docToRow(e.DocumentKey), Timestamp: time.Now().UTC()}, true
	default:
		return sdk.Change{}, false
	}
}

// selectedByName indexes streams by collection name.
func selectedByName(streams []model.Stream) map[string]model.Stream {
	m := make(map[string]model.Stream, len(streams))
	for _, s := range streams {
		m[s.Name] = s
	}
	return m
}
