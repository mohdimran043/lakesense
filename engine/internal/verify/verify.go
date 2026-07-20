// Package verify re-checks that a destination's current state matches its
// source, order-independently, and drills into offending id ranges on mismatch.
// It backs `lsengine verify` and the closing check of a backfill.
//
// The destination merge key (_ls_id) is a hash of the primary key, so it does
// not correspond to a source PK range — bisection therefore runs in memory over
// the union of ids read from both sides rather than re-reading source ranges.
// Each side is read exactly once; the drill-down (offending ranges + sample
// mismatched PKs) is derived from those two in-memory views.
package verify

import (
	"context"
	"fmt"
	"sort"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
)

// StreamInput is everything VerifyStream needs for one stream.
type StreamInput struct {
	Stream          model.Stream
	Source          sdk.FullLoader       // re-reads the whole source
	DestReader      syncrun.StreamReader // current-state view of the destination
	SampleThreshold int                  // id-range size at which to enumerate (default 100)
	SampleCap       int                  // max sample PKs to report (default 20)
}

func (in *StreamInput) defaults() {
	if in.SampleThreshold <= 0 {
		in.SampleThreshold = 100
	}
	if in.SampleCap <= 0 {
		in.SampleCap = 20
	}
}

// entry is one side's view of a record: the row hash and a human-readable PK.
type entry struct {
	hash uint64
	pk   string
}

// VerifyStream compares source and destination current state and returns the
// result. On mismatch it fills MismatchedRanges and SamplePKs by bisecting the
// id union in memory.
func VerifyStream(ctx context.Context, in StreamInput) (events.VerifyResult, error) {
	in.defaults()
	cols := syncrun.DataColumns(in.Stream)
	pk := in.Stream.Schema.PrimaryKey()

	srcMap, err := readSource(ctx, in.Source, in.Stream, cols, pk)
	if err != nil {
		return events.VerifyResult{}, err
	}
	dstMap, err := readDest(ctx, in.DestReader, cols, pk)
	if err != nil {
		return events.VerifyResult{}, err
	}

	res := events.VerifyResult{
		SourceRows:      int64(len(srcMap)),
		DestinationRows: int64(len(dstMap)),
		Match:           digestOf(srcMap) == digestOf(dstMap) && len(srcMap) == len(dstMap),
	}
	if res.Match {
		return res, nil
	}
	bisect(sortedUnion(srcMap, dstMap), srcMap, dstMap, in.SampleThreshold, in.SampleCap, &res)
	return res, nil
}

// readSource replays every chunk of the source, keying rows by the same record
// id the writer used (a hash of the PK).
func readSource(ctx context.Context, fl sdk.FullLoader, stream model.Stream, cols, pk []string) (map[string]entry, error) {
	chunks, err := fl.SplitChunks(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("split source for verify: %w", err)
	}
	if len(chunks) == 0 {
		chunks = []state.Chunk{{}}
	}
	m := map[string]entry{}
	for _, chunk := range chunks {
		err := fl.ReadChunk(ctx, stream, chunk, func(_ context.Context, row sdk.Row) error {
			h, err := syncrun.HashDataColumns(row, cols)
			if err != nil {
				return fmt.Errorf("hash source row: %w", err)
			}
			id := syncrun.RecordID(row, pk)
			m[id] = entry{hash: h, pk: pkDisplay(row, pk, id)}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("read source chunk for verify: %w", err)
		}
	}
	return m, nil
}

// readDest materializes the destination's current state, keyed by its stored id.
func readDest(ctx context.Context, rd syncrun.StreamReader, cols, pk []string) (map[string]entry, error) {
	m := map[string]entry{}
	err := rd.Read(ctx, nil, func(_ context.Context, row sdk.Row) error {
		h, err := syncrun.HashDataColumns(row, cols)
		if err != nil {
			return fmt.Errorf("hash destination row: %w", err)
		}
		id, _ := row[model.ColRecordID].(string)
		m[id] = entry{hash: h, pk: pkDisplay(row, pk, id)}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// digestOf folds an id→entry map into the order-independent aggregate hex.
func digestOf(m map[string]entry) string {
	agg := &syncrun.AggregateDigest{}
	for _, e := range m {
		agg.Add(e.hash)
	}
	return agg.Hex()
}

// bisect narrows the sorted id slice until a range is small enough to
// enumerate, recording offending ranges and up to cap sample PKs. It descends
// only into halves whose per-range digest or count differs.
func bisect(ids []string, src, dst map[string]entry, threshold, cap int, out *events.VerifyResult) {
	if len(ids) == 0 {
		return
	}
	if rangeMatches(ids, src, dst) {
		return
	}
	if len(ids) <= threshold {
		enumerate(ids, src, dst, cap, out)
		return
	}
	mid := len(ids) / 2
	bisect(ids[:mid], src, dst, threshold, cap, out)
	bisect(ids[mid:], src, dst, threshold, cap, out)
}

// rangeMatches reports whether the two sides agree over exactly this id set.
func rangeMatches(ids []string, src, dst map[string]entry) bool {
	var sSum, dSum uint64
	var sN, dN int
	for _, id := range ids {
		if e, ok := src[id]; ok {
			sSum += e.hash
			sN++
		}
		if e, ok := dst[id]; ok {
			dSum += e.hash
			dN++
		}
	}
	return sSum == dSum && sN == dN
}

// enumerate records this id range and appends sample PKs for every id missing
// on one side or whose hash differs, honoring the sample cap.
func enumerate(ids []string, src, dst map[string]entry, cap int, out *events.VerifyResult) {
	out.MismatchedRanges = append(out.MismatchedRanges, events.Range{Min: ids[0], Max: ids[len(ids)-1]})
	for _, id := range ids {
		s, hasS := src[id]
		d, hasD := dst[id]
		if hasS && hasD && s.hash == d.hash {
			continue
		}
		if len(out.SamplePKs) >= cap {
			return
		}
		pk := s.pk
		if !hasS {
			pk = d.pk
		}
		out.SamplePKs = append(out.SamplePKs, pk)
	}
}

// sortedUnion returns the sorted set of ids present on either side.
func sortedUnion(a, b map[string]entry) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for id := range a {
		seen[id] = struct{}{}
	}
	for id := range b {
		seen[id] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// pkDisplay renders a readable primary key for sample reporting: the PK column
// values joined, or the record id when the stream has no primary key.
func pkDisplay(row sdk.Row, pk []string, id string) string {
	if len(pk) == 0 {
		return id
	}
	parts := make([]string, 0, len(pk))
	for _, col := range pk {
		parts = append(parts, fmt.Sprintf("%v", row[col]))
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return fmt.Sprintf("%v", parts)
}
