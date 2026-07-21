package syncrun

import (
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// distinctCap bounds the exact-distinct set per column so column-stats
// accumulation never blows up memory on a high-cardinality column; past the cap
// Distinct is reported as the cap (an honest ">= cap" signal).
const distinctCap = 10000

// colAccum accumulates one column's stats as rows stream past.
type colAccum struct {
	rows      int64
	nulls     int64
	distinct  map[string]struct{}
	capped    bool
	min, max  string
	hasMinMax bool
}

// streamColStats accumulates per-column stats for one stream. Min/max are by
// string comparison (informational); the monitors that matter (null-rate,
// volume) use rows/nulls, which are exact.
type streamColStats struct {
	cols  map[string]*colAccum
	order []string
}

func newStreamColStats() *streamColStats {
	return &streamColStats{cols: map[string]*colAccum{}}
}

// observe folds one row's data columns into the accumulator.
func (s *streamColStats) observe(row sdk.Row, columns []string) {
	for _, c := range columns {
		a := s.cols[c]
		if a == nil {
			a = &colAccum{distinct: map[string]struct{}{}}
			s.cols[c] = a
			s.order = append(s.order, c)
		}
		a.rows++
		v, present := row[c]
		if !present || v == nil {
			a.nulls++
			continue
		}
		str := fmt.Sprintf("%v", v)
		if !a.capped {
			a.distinct[str] = struct{}{}
			if len(a.distinct) > distinctCap {
				a.capped = true
				a.distinct = nil // free memory; count is pinned at the cap
			}
		}
		if !a.hasMinMax {
			a.min, a.max, a.hasMinMax = str, str, true
		} else {
			if str < a.min {
				a.min = str
			}
			if str > a.max {
				a.max = str
			}
		}
	}
}

// result renders the accumulated stats as the emitted payload.
func (s *streamColStats) result() events.ColumnStats {
	out := events.ColumnStats{Columns: make([]events.ColumnStat, 0, len(s.order))}
	for _, c := range s.order {
		a := s.cols[c]
		distinct := int64(len(a.distinct))
		if a.capped {
			distinct = distinctCap
		}
		out.Columns = append(out.Columns, events.ColumnStat{
			Column: c, Rows: a.rows, Nulls: a.nulls, Distinct: distinct, Min: a.min, Max: a.max,
		})
	}
	return out
}
