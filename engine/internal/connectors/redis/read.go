package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader: a single chunk. SCAN iterates the whole
// keyspace within ReadChunk, so range chunking does not apply.
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: SCANs every key matching the pattern and
// emits one row per key (key, type, JSON value, ttl). Keys that vanish mid-scan
// (TYPE "none") are skipped — a benign consequence of SCAN not being a snapshot.
func (c *Connector) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	var cursor uint64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, c.cfg.Pattern, c.cfg.ScanCount).Result()
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		for _, key := range keys {
			row, ok, err := c.readKey(ctx, key)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := emit(ctx, row); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// readKey builds the row for one key. ok is false when the key disappeared
// between SCAN and read (TYPE "none").
func (c *Connector) readKey(ctx context.Context, key string) (sdk.Row, bool, error) {
	kind, err := c.client.Type(ctx, key).Result()
	if err != nil {
		return nil, false, fmt.Errorf("type %q: %w", key, err)
	}
	if kind == "none" {
		return nil, false, nil
	}
	value, err := c.readValue(ctx, key, kind)
	if err != nil {
		return nil, false, err
	}
	return sdk.Row{
		colKey:   key,
		colType:  kind,
		colValue: value,
		colTTL:   ttlSeconds(c.client.TTL(ctx, key).Val()),
	}, true, nil
}

// readValue reads a key's value per its type and returns it JSON-encoded, so a
// single JSON column carries every Redis value shape losslessly.
func (c *Connector) readValue(ctx context.Context, key, kind string) (string, error) {
	switch kind {
	case "string":
		s, err := c.client.Get(ctx, key).Result()
		if err != nil {
			return "", wrap(key, err)
		}
		return marshal(s)
	case "list":
		v, err := c.client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return "", wrap(key, err)
		}
		return marshal(v)
	case "set":
		v, err := c.client.SMembers(ctx, key).Result()
		if err != nil {
			return "", wrap(key, err)
		}
		sort.Strings(v) // sets are unordered; stabilize for reproducible digests
		return marshal(v)
	case "hash":
		v, err := c.client.HGetAll(ctx, key).Result()
		if err != nil {
			return "", wrap(key, err)
		}
		return marshal(v)
	case "zset":
		zs, err := c.client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return "", wrap(key, err)
		}
		members := make([]map[string]any, len(zs))
		for i, z := range zs {
			members[i] = map[string]any{"member": z.Member, "score": z.Score}
		}
		return marshal(members)
	case "stream":
		msgs, err := c.client.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return "", wrap(key, err)
		}
		entries := make([]map[string]any, len(msgs))
		for i, m := range msgs {
			entries[i] = map[string]any{"id": m.ID, "values": m.Values}
		}
		return marshal(entries)
	default:
		// Unknown/module types (e.g. ReJSON): record the type, leave value null.
		return "", nil
	}
}

// ttlSeconds normalizes a TTL duration to whole seconds; a key with no expiry
// (or one deleted mid-scan) reports -1.
func ttlSeconds(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return int64(d / time.Second)
}

func marshal(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("encode value: %w", err)
	}
	return string(b), nil
}

func wrap(key string, err error) error {
	if err == goredis.Nil { // key raced away between TYPE and read
		return nil
	}
	return fmt.Errorf("read %q: %w", key, err)
}
