package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// keepAlive is how long a point-in-time context is retained between pages. Each
// search extends it, so it only needs to outlast one round trip.
const keepAlive = "2m"

// SplitChunks implements sdk.FullLoader: a single chunk. search_after streams the
// whole index within ReadChunk, so range chunking is unnecessary.
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: streams every document of the index under
// a point-in-time for a consistent snapshot.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	_, err := c.scanSearchAfter(ctx, stream.Name, nil, "", "", emit)
	return err
}

// MaxCursor implements sdk.IncrementalReader: a max aggregation over the cursor
// field. An empty result (no documents) yields an empty watermark.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	body := map[string]any{
		"size": 0,
		"aggs": map[string]any{
			"max_cursor": map[string]any{"max": map[string]any{"field": cursorField}},
		},
	}
	var resp searchResponse
	if err := c.do(ctx, stream.Name, body, &resp); err != nil {
		return "", err
	}
	return cursorString(stream, cursorField, resp), nil
}

// ReadIncrement implements sdk.IncrementalReader: streams documents whose cursor
// field is greater than since (or every document when since is empty), ordered by
// the cursor with a _shard_doc tiebreak, and returns the new high watermark.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.client == nil {
		return "", fmt.Errorf("connector not set up")
	}
	var query map[string]any
	if since != "" {
		query = map[string]any{
			"range": map[string]any{cursorField: map[string]any{"gt": since}},
		}
	}
	return c.scanSearchAfter(ctx, stream.Name, query, cursorField, since, emit)
}

// scanSearchAfter streams every hit matching query (nil = match_all) from index,
// paging with a point-in-time and search_after so the view stays consistent and
// resumable across pages. When cursorField is set, hits are ordered by it (then a
// _shard_doc tiebreak) and the maximum value seen is returned; otherwise only the
// _shard_doc order is used and since is returned unchanged.
func (c *Connector) scanSearchAfter(ctx context.Context, index string, query map[string]any, cursorField, since string, emit sdk.RowFunc) (string, error) {
	pitID, err := c.openPIT(ctx, index)
	if err != nil {
		return "", err
	}
	// Close on a fresh context so a cancelled read still releases the PIT.
	defer func() { _ = c.closePIT(context.WithoutCancel(ctx), pitID) }()

	sortSpec := make([]any, 0, 2)
	if cursorField != "" {
		sortSpec = append(sortSpec, map[string]any{cursorField: "asc"})
	}
	sortSpec = append(sortSpec, map[string]any{"_shard_doc": "asc"})

	maxCursor := since
	var searchAfter []any
	for {
		body := map[string]any{
			"size": c.cfg.PageSize,
			"sort": sortSpec,
			"pit":  map[string]any{"id": pitID, "keep_alive": keepAlive},
		}
		if query != nil {
			body["query"] = query
		}
		if searchAfter != nil {
			body["search_after"] = searchAfter
		}

		var resp searchResponse
		if err := c.do(ctx, "", body, &resp); err != nil {
			return "", err
		}
		if resp.PitID != "" {
			pitID = resp.PitID
		}
		hits := resp.Hits.Hits
		if len(hits) == 0 {
			break
		}
		for _, h := range hits {
			row, err := hitToRow(h.ID, h.Source)
			if err != nil {
				return "", err
			}
			if cursorField != "" {
				if v, ok := row[cursorField]; ok && v != nil {
					maxCursor = fmt.Sprintf("%v", v)
				}
			}
			if err := emit(ctx, row); err != nil {
				return "", err
			}
		}
		searchAfter = hits[len(hits)-1].Sort
		if len(hits) < c.cfg.PageSize {
			break
		}
	}
	return maxCursor, nil
}

// openPIT opens a point-in-time over index and returns its id.
func (c *Connector) openPIT(ctx context.Context, index string) (string, error) {
	res, err := c.client.OpenPointInTime(
		[]string{index}, keepAlive,
		c.client.OpenPointInTime.WithContext(ctx),
	)
	if err != nil {
		return "", fmt.Errorf("open point-in-time for %s: %w", index, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return "", fmt.Errorf("open point-in-time for %s: %s", index, res.Status())
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode point-in-time id: %w", err)
	}
	if body.ID == "" {
		return "", fmt.Errorf("open point-in-time for %s: empty id", index)
	}
	return body.ID, nil
}

// closePIT releases a point-in-time. Errors are advisory: the context expires on
// its own keep_alive, so callers defer this best-effort.
func (c *Connector) closePIT(ctx context.Context, id string) error {
	body, err := json.Marshal(map[string]any{"id": id})
	if err != nil {
		return err
	}
	res, err := c.client.ClosePointInTime(
		c.client.ClosePointInTime.WithContext(ctx),
		c.client.ClosePointInTime.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	return nil
}

// cursorString renders a max aggregation result as a stored watermark: the
// formatted date for temporal cursors, else the exact numeric text.
func cursorString(stream model.Stream, cursorField string, resp searchResponse) string {
	agg := resp.Aggregations.MaxCursor
	if agg.Value == nil {
		return ""
	}
	if col, ok := stream.Schema.Column(cursorField); ok {
		if col.Type == model.TypeTimestamp || col.Type == model.TypeDate {
			if agg.ValueAsString != "" {
				return agg.ValueAsString
			}
		}
	}
	return strconv.FormatFloat(*agg.Value, 'f', -1, 64)
}
