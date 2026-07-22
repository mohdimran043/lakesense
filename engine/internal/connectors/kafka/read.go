package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader: a single chunk. Partitions are streamed
// within ReadChunk.
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: reads every retained message of every
// partition, from its earliest offset to the current end offset.
func (c *Connector) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	parts, err := c.partitions(ctx)
	if err != nil {
		return err
	}
	for _, p := range parts {
		first, end, err := c.bounds(ctx, p)
		if err != nil {
			return err
		}
		if err := c.readPartition(ctx, p, first, end, emit); err != nil {
			return err
		}
	}
	return nil
}

// MaxCursor implements sdk.IncrementalReader: the current end offset of every
// partition, JSON-encoded. Captured before full load so the later increment is
// gap-free.
func (c *Connector) MaxCursor(ctx context.Context, _ model.Stream, _ string) (string, error) {
	parts, err := c.partitions(ctx)
	if err != nil {
		return "", err
	}
	ends := map[int]int64{}
	for _, p := range parts {
		_, end, err := c.bounds(ctx, p)
		if err != nil {
			return "", err
		}
		ends[p] = end
	}
	return encodeOffsets(ends), nil
}

// ReadIncrement implements sdk.IncrementalReader: reads each partition from the
// stored offset (or its earliest, when unset) up to the end offset captured now,
// returning the advanced per-partition offsets. Bounding to the captured end
// guarantees termination even under continuous production.
func (c *Connector) ReadIncrement(ctx context.Context, _ model.Stream, _ string, since string, emit sdk.RowFunc) (string, error) {
	starts, err := decodeOffsets(since)
	if err != nil {
		return "", err
	}
	parts, err := c.partitions(ctx)
	if err != nil {
		return "", err
	}
	next := map[int]int64{}
	for _, p := range parts {
		first, end, err := c.bounds(ctx, p)
		if err != nil {
			return "", err
		}
		start, ok := starts[p]
		if !ok || start < first {
			start = first // new partition, or stored offset fell off retention
		}
		if start > end {
			start = end // topic truncated/recreated below the stored offset
		}
		if err := c.readPartition(ctx, p, start, end, emit); err != nil {
			return "", err
		}
		next[p] = end
	}
	// Preserve offsets for partitions that briefly disappeared from metadata.
	for p, off := range starts {
		if _, seen := next[p]; !seen {
			next[p] = off
		}
	}
	return encodeOffsets(next), nil
}

// bounds returns the earliest and end (high-water-mark) offsets of a partition.
func (c *Connector) bounds(ctx context.Context, partition int) (first, end int64, err error) {
	conn, err := kafka.DialLeader(ctx, "tcp", c.cfg.Brokers[0], c.cfg.Topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("dial leader p%d: %w", partition, err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(c.timeout()))
	if first, err = conn.ReadFirstOffset(); err != nil {
		return 0, 0, fmt.Errorf("first offset p%d: %w", partition, err)
	}
	if end, err = conn.ReadLastOffset(); err != nil {
		return 0, 0, fmt.Errorf("last offset p%d: %w", partition, err)
	}
	return first, end, nil
}

// readPartition emits every message in [start, end) of one partition. It stops as
// soon as end is reached, so it never blocks waiting for an unwritten message.
func (c *Connector) readPartition(ctx context.Context, partition int, start, end int64, emit sdk.RowFunc) error {
	if start >= end {
		return nil
	}
	conn, err := kafka.DialLeader(ctx, "tcp", c.cfg.Brokers[0], c.cfg.Topic, partition)
	if err != nil {
		return fmt.Errorf("dial leader p%d: %w", partition, err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Seek(start, kafka.SeekAbsolute); err != nil {
		return fmt.Errorf("seek p%d to %d: %w", partition, start, err)
	}

	offset := start
	for offset < end {
		_ = conn.SetReadDeadline(time.Now().Add(c.timeout()))
		batch := conn.ReadBatch(1, c.cfg.MaxBytes)
		for offset < end {
			m, err := batch.ReadMessage()
			if err != nil {
				break // batch drained (io.EOF) → fetch the next one
			}
			if err := emit(ctx, messageRow(m)); err != nil {
				_ = batch.Close()
				return err
			}
			offset = m.Offset + 1
		}
		if err := batch.Close(); err != nil {
			return fmt.Errorf("read batch p%d: %w", partition, err)
		}
	}
	return nil
}

// messageRow flattens a Kafka message into the envelope row.
func messageRow(m kafka.Message) sdk.Row {
	row := sdk.Row{
		colPartition: int32(m.Partition),
		colOffset:    m.Offset,
		colValue:     valueJSON(m.Value),
		colTimestamp: m.Time.UTC().Format(time.RFC3339Nano),
		colHeaders:   headersJSON(m.Headers),
	}
	if m.Key != nil {
		row[colKey] = string(m.Key)
	} else {
		row[colKey] = nil
	}
	return row
}

// valueJSON keeps a message value that is already JSON as-is; otherwise it wraps
// the raw bytes as a JSON string, so the JSON column is always valid JSON.
func valueJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		return string(b)
	}
	enc, _ := json.Marshal(string(b))
	return string(enc)
}

// headersJSON serializes message headers as a JSON object (last value wins on
// duplicate keys, matching typical consumer behavior).
func headersJSON(hs []kafka.Header) any {
	if len(hs) == 0 {
		return nil
	}
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[h.Key] = string(h.Value)
	}
	b, _ := json.Marshal(m)
	return string(b)
}
