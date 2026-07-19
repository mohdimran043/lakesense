// Package state implements the engine's resumable sync state: the chunk-set
// protocol for full loads, cursor watermarks for incremental, and a global
// section for log-anchored CDC positions. Semantics follow
// docs/analysis/state-and-recovery.md.
package state

import (
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"github.com/lakesense/lakesense/engine/internal/config"
)

// CurrentVersion is written into new state documents.
const CurrentVersion = 1

// Chunk is one unit of resumable full-load work: a half-open [Min, Max) range
// over the chunking key. Empty Min/Max means unbounded on that side. Values
// are stored as strings so they round-trip JSON losslessly.
type Chunk struct {
	Min string `json:"min,omitempty"`
	Max string `json:"max,omitempty"`
}

// StreamState is per-stream progress.
type StreamState struct {
	Namespace string `json:"namespace"`
	Stream    string `json:"stream"`
	Mode      string `json:"mode,omitempty"`
	// Chunks is the REMAINING full-load work. Present-but-empty means the
	// full load completed; absent means chunking has not run yet.
	Chunks *[]Chunk `json:"chunks,omitempty"`
	// Cursors holds incremental watermarks keyed by cursor field, stored as
	// normalized strings (timestamps in RFC3339Nano UTC).
	Cursors map[string]string `json:"cursors,omitempty"`
	// BackfillChunks tracks bounded-backfill work separately so a backfill
	// can never clobber full-load or CDC progress.
	BackfillChunks *[]Chunk `json:"backfill_chunks,omitempty"`
}

// GlobalState anchors log-based CDC sources (WAL LSN, binlog coordinates).
type GlobalState struct {
	// Position is the connector-specific CDC position document.
	Position map[string]string `json:"position,omitempty"`
	// Streams lists stream IDs whose initial backfill completed under this
	// anchor — how "backfill done" is decided for global-state sources.
	Streams []string `json:"streams,omitempty"`
}

// Document is the persisted state file. Method access is mutex-guarded; the
// file is rewritten atomically after every mutation so on-disk state always
// reflects the latest durable progress.
type Document struct {
	mu      sync.Mutex
	path    string         // empty = in-memory only (tests)
	Version int            `json:"version"`
	Global  *GlobalState   `json:"global,omitempty"`
	Streams []*StreamState `json:"streams,omitempty"`
}

// Load reads a state document from path, or returns a fresh one when the file
// does not exist. The returned document saves back to the same path.
func Load(path string) (*Document, error) {
	doc := &Document{Version: CurrentVersion}
	err := config.LoadJSONLenient(path, doc)
	if err != nil {
		if isNotExist(err) {
			doc.path = path
			return doc, nil
		}
		return nil, fmt.Errorf("load state: %w", err)
	}
	if doc.Version == 0 {
		doc.Version = CurrentVersion
	}
	doc.path = path
	return doc, nil
}

// NewInMemory returns a state document that never touches disk (tests, dry runs).
func NewInMemory() *Document {
	return &Document{Version: CurrentVersion}
}

// save persists under an already-held lock.
func (d *Document) save() error {
	if d.path == "" {
		return nil
	}
	if err := config.SaveJSONAtomic(d.path, d); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	return nil
}

// stream finds or creates the per-stream entry. Caller must hold d.mu.
func (d *Document) stream(namespace, name string) *StreamState {
	for _, s := range d.Streams {
		if s.Namespace == namespace && s.Stream == name {
			return s
		}
	}
	s := &StreamState{Namespace: namespace, Stream: name}
	d.Streams = append(d.Streams, s)
	return s
}

// SetChunks records the full chunk plan before any chunk is read.
func (d *Document) SetChunks(namespace, name string, chunks []Chunk) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]Chunk, len(chunks))
	copy(cp, chunks)
	d.stream(namespace, name).Chunks = &cp
	return d.save()
}

// RemainingChunks returns the outstanding chunks, or nil when chunking has
// not run yet. done reports whether the full load already completed.
func (d *Document) RemainingChunks(namespace, name string) (chunks []Chunk, done bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stream(namespace, name)
	if s.Chunks == nil {
		return nil, false
	}
	cp := make([]Chunk, len(*s.Chunks))
	copy(cp, *s.Chunks)
	return cp, len(cp) == 0
}

// CompleteChunk removes one finished chunk and persists. It returns the
// number of chunks remaining.
func (d *Document) CompleteChunk(namespace, name string, chunk Chunk) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stream(namespace, name)
	if s.Chunks == nil {
		return 0, fmt.Errorf("stream %s.%s: no chunk plan in state", namespace, name)
	}
	kept := (*s.Chunks)[:0]
	found := false
	for _, c := range *s.Chunks {
		if c == chunk && !found {
			found = true
			continue
		}
		kept = append(kept, c)
	}
	if !found {
		return len(kept), fmt.Errorf("stream %s.%s: chunk [%s,%s) not in state", namespace, name, chunk.Min, chunk.Max)
	}
	*s.Chunks = kept
	return len(kept), d.save()
}

// SetCursor stores an incremental watermark (normalized string form).
func (d *Document) SetCursor(namespace, name, field, value string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stream(namespace, name)
	if s.Cursors == nil {
		s.Cursors = map[string]string{}
	}
	s.Cursors[field] = value
	return d.save()
}

// Cursor returns the stored watermark for field, if any.
func (d *Document) Cursor(namespace, name, field string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stream(namespace, name)
	v, ok := s.Cursors[field]
	return v, ok
}

// SetGlobalPosition stores the CDC anchor and optionally marks streams as
// backfilled under it.
func (d *Document) SetGlobalPosition(position map[string]string, backfilled ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Global == nil {
		d.Global = &GlobalState{}
	}
	if position != nil {
		d.Global.Position = position
	}
	for _, id := range backfilled {
		if !contains(d.Global.Streams, id) {
			d.Global.Streams = append(d.Global.Streams, id)
		}
	}
	return d.save()
}

// GlobalPosition returns the stored CDC anchor, if any.
func (d *Document) GlobalPosition() (map[string]string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Global == nil || d.Global.Position == nil {
		return nil, false
	}
	cp := make(map[string]string, len(d.Global.Position))
	for k, v := range d.Global.Position {
		cp[k] = v
	}
	return cp, true
}

// BackfilledGlobally reports whether the stream completed its initial
// backfill under the current global anchor.
func (d *Document) BackfilledGlobally(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Global != nil && contains(d.Global.Streams, id)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func isNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}
