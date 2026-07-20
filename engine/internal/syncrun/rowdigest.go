package syncrun

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// rowdigest provides the canonical, order-independent row accounting that
// backs the data-diff badge (Phase 2.7): both the rows read from source and
// the rows written to destination are hashed with the same function, so a
// drop or corruption anywhere on the write path shows up as a checksum
// mismatch. Engine-owned metadata columns (_ls_*) are excluded so ingestion
// timestamps and op tags never perturb the comparison.

// hashRow returns a 64-bit FNV-1a hash over the row's data columns in a
// canonical form. columns, when non-empty, fixes which columns participate
// (sorted); otherwise every non-metadata column is used. Values are encoded
// via JSON, whose deterministic map-key ordering and stable scalar rendering
// make the hash reproducible across runs and machines.
func hashRow(row sdk.Row, columns []string) (uint64, error) {
	names := columns
	if len(names) == 0 {
		names = make([]string, 0, len(row))
		for k := range row {
			if !isMetadataColumn(k) {
				names = append(names, k)
			}
		}
	}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	h := fnv.New64a()
	for _, name := range sorted {
		v := row[name]
		enc, err := json.Marshal(v)
		if err != nil {
			return 0, fmt.Errorf("hash column %q: %w", name, err)
		}
		_, _ = h.Write([]byte(name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(enc)
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64(), nil
}

// isMetadataColumn reports engine-injected columns, which never participate in
// checksums (their values are non-deterministic or bookkeeping).
func isMetadataColumn(name string) bool {
	return strings.HasPrefix(name, "_ls_")
}

// recordID is the idempotency key for destination merges: a stable hex hash of
// the primary-key values (or, absent a PK, of every data column). Two reads of
// the same source row — full load and a later CDC replay — produce the same
// record ID, which is what upgrades at-least-once delivery into
// effectively-once destination rows.
func recordID(row sdk.Row, pk []string) string {
	cols := pk
	if len(cols) == 0 {
		for k := range row {
			if !isMetadataColumn(k) {
				cols = append(cols, k)
			}
		}
		sort.Strings(cols)
	}
	h := fnv.New64a()
	for _, name := range cols {
		enc, _ := json.Marshal(row[name])
		_, _ = h.Write([]byte(name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(enc)
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// digest accumulates an order-independent aggregate over row hashes plus a
// count. Summation (rather than XOR) is used so identical rows reinforce the
// aggregate instead of cancelling out. It is not safe for concurrent use; the
// orchestrator holds one per stream/side.
type digest struct {
	sum  uint64
	rows int64
}

// add folds one row's hash into the aggregate.
func (d *digest) add(h uint64) {
	d.sum += h
	d.rows++
}

// Rows returns the number of rows folded in.
func (d *digest) Rows() int64 { return d.rows }

// Hex renders the aggregate as a fixed-width hex string for the checksum event.
func (d *digest) Hex() string {
	return fmt.Sprintf("%016x", d.sum)
}

// RecordID exposes the destination merge key (stable hash of the primary-key
// values) so verify can compute a source row's id the same way the writer did
// for the destination — the two sides only line up if the id function is shared.
func RecordID(row sdk.Row, pk []string) string { return recordID(row, pk) }

// HashDataColumns hashes a row over the given data columns (empty = all
// non-metadata columns). Exported so verify and backfill compute digests
// identically to the writer — one hash function, no drift.
func HashDataColumns(row sdk.Row, columns []string) (uint64, error) {
	return hashRow(row, columns)
}

// DataColumns returns a stream's non-metadata column names in schema order
// (exported wrapper of dataColumns).
func DataColumns(s model.Stream) []string { return dataColumns(s) }

// AggregateDigest is the exported, order-independent aggregate verify uses for
// per-range and whole-stream sums. It wraps the same digest the writer folds
// row hashes into.
type AggregateDigest struct{ d digest }

// Add folds one row hash into the aggregate.
func (a *AggregateDigest) Add(h uint64) { a.d.add(h) }

// Rows returns the number of rows folded in.
func (a *AggregateDigest) Rows() int64 { return a.d.Rows() }

// Hex renders the aggregate as a fixed-width hex string.
func (a *AggregateDigest) Hex() string { return a.d.Hex() }

// dataColumns returns a stream's non-metadata column names in schema order —
// the fixed column set both sides checksum over.
func dataColumns(s model.Stream) []string {
	names := make([]string, 0, len(s.Schema.Columns))
	for _, c := range s.Schema.Columns {
		if !isMetadataColumn(c.Name) {
			names = append(names, c.Name)
		}
	}
	return names
}
