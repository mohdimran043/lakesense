// Package sdk defines the LakeSense Connector SDK: the interface every source
// implements, capability declarations that drive UI badges and docs matrices,
// and the connector registry.
//
// Division of labor (docs/analysis/engine-protocol.md §3): the engine core
// owns chunk orchestration, cursor logic, CDC phasing, retries, and
// concurrency. Connectors implement only narrow leaf operations. A connector
// implements Connector always, and whichever of FullLoader,
// IncrementalReader, and ChangeStreamer its capabilities declare.
package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// Capability is a machine-readable declaration of what a connector supports.
// It is the single source of truth behind UI badges and the docs matrix.
type Capability string

const (
	CapFullLoad    Capability = "full_load"
	CapIncremental Capability = "incremental"
	CapCDC         Capability = "cdc"
)

// Maturity is the tested-quality badge. It must reflect the test battery the
// connector actually passes (enforced by the harness, not by prose).
type Maturity string

const (
	MaturityCertified Maturity = "certified" // Tier A battery
	MaturityStable    Maturity = "stable"    // Tier B battery
	MaturityBeta      Maturity = "beta"      // Tier C battery
)

// Spec describes a connector to UIs and docs: config schema, capabilities,
// and identity.
type Spec struct {
	Type         string          `json:"type"`         // e.g. "postgres"
	DisplayName  string          `json:"display_name"` // e.g. "PostgreSQL"
	Capabilities []Capability    `json:"capabilities"`
	Maturity     Maturity        `json:"maturity"`
	ConfigSchema json.RawMessage `json:"config_schema"` // JSON Schema for the config form
	// Presets lists wire-compatible variants served by this connector
	// (e.g. postgres → cockroachdb, timescaledb), each with its own
	// capability overrides where the variant differs.
	Presets []Preset `json:"presets,omitempty"`
}

// Preset is a named wire-compatible variant of a connector.
type Preset struct {
	Name         string       `json:"name"`                   // e.g. "cockroachdb"
	DisplayName  string       `json:"display_name"`           // e.g. "CockroachDB"
	Capabilities []Capability `json:"capabilities,omitempty"` // override when narrower
	Maturity     Maturity     `json:"maturity,omitempty"`
	Notes        string       `json:"notes,omitempty"` // honest quirk summary
}

// Row is one record read from a source, keyed by column name.
type Row map[string]any

// RowFunc receives rows during chunk and incremental reads. Implementations
// must respect ctx cancellation.
type RowFunc func(ctx context.Context, row Row) error

// ChangeKind is the CDC operation type.
type ChangeKind string

const (
	ChangeInsert ChangeKind = "insert"
	ChangeUpdate ChangeKind = "update"
	ChangeDelete ChangeKind = "delete"
)

// Change is one CDC event.
type Change struct {
	StreamID  string // namespace.name
	Kind      ChangeKind
	Data      Row       // after-image; deletes carry identity columns
	Timestamp time.Time // source-side commit/change time
	// Position is the connector-specific location of this change (LSN,
	// binlog coordinates, resume token), recorded as metadata columns.
	Position map[string]string
}

// ChangeFunc receives CDC events.
type ChangeFunc func(ctx context.Context, change Change) error

// Connector is the minimal surface every source implements.
type Connector interface {
	// Spec returns identity, capabilities, and the config JSON schema.
	// It must work before Setup.
	Spec() Spec
	// Setup parses and validates raw config and establishes connectivity.
	Setup(ctx context.Context, rawConfig json.RawMessage) error
	// Check verifies the connection and required source-side prerequisites,
	// returning actionable errors ("wal_level is 'replica', need 'logical'").
	Check(ctx context.Context) error
	// Discover lists streams with schemas and per-stream supported modes.
	Discover(ctx context.Context) ([]model.Stream, error)
	// Close releases connections. Safe to call after failed Setup.
	Close(ctx context.Context) error
}

// FullLoader is implemented by connectors declaring CapFullLoad.
type FullLoader interface {
	// SplitChunks partitions the stream into resumable chunks. Called once
	// per stream; the plan is persisted before any chunk is read.
	SplitChunks(ctx context.Context, stream model.Stream) ([]state.Chunk, error)
	// ReadChunk streams every row of one chunk. Chunks may be retried whole;
	// reads must be side-effect free.
	ReadChunk(ctx context.Context, stream model.Stream, chunk state.Chunk, emit RowFunc) error
}

// IncrementalReader is implemented by connectors declaring CapIncremental.
type IncrementalReader interface {
	// MaxCursor returns the current maximum cursor value as a normalized
	// string (captured BEFORE full load so the later increment is gap-free).
	MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error)
	// ReadIncrement streams rows with cursor > since (or all rows when since
	// is empty), returning the new high watermark.
	ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit RowFunc) (newCursor string, err error)
}

// ChangeStreamer is implemented by connectors declaring CapCDC.
type ChangeStreamer interface {
	// PrepareCDC establishes/validates the replication anchor BEFORE any
	// backfill and returns the starting position. Everything after the
	// anchor must be replayable.
	PrepareCDC(ctx context.Context, streams []model.Stream) (position map[string]string, err error)
	// StreamChanges replays changes from position until caught up to the
	// bounded target captured at call time, returning the final position.
	// The engine persists the returned position only after destination
	// commit (ack-before-state discipline).
	StreamChanges(ctx context.Context, streams []model.Stream, position map[string]string, emit ChangeFunc) (final map[string]string, err error)
}

// Factory constructs an unconfigured connector instance.
type Factory func() Connector

// Registry maps connector type names to factories.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: map[string]Factory{}}
}

// Register adds a connector factory. Duplicate registration is a programmer
// error and panics at init time.
func (r *Registry) Register(name string, f Factory) {
	if _, exists := r.factories[name]; exists {
		panic(fmt.Sprintf("connector %q registered twice", name))
	}
	r.factories[name] = f
}

// New instantiates a connector by type name.
func (r *Registry) New(name string) (Connector, error) {
	f, ok := r.factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown connector %q (available: %v)", name, r.Names())
	}
	return f(), nil
}

// Names lists registered connector types, sorted.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.factories))
	for n := range r.factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ValidateCapabilities checks that a connector's declared capabilities match
// the read interfaces it actually implements — declarations are enforced in
// code, never just prose. Run by the registry harness and tests.
func ValidateCapabilities(c Connector) error {
	spec := c.Spec()
	for _, cap := range spec.Capabilities {
		switch cap {
		case CapFullLoad:
			if _, ok := c.(FullLoader); !ok {
				return fmt.Errorf("connector %s declares %s but does not implement FullLoader", spec.Type, cap)
			}
		case CapIncremental:
			if _, ok := c.(IncrementalReader); !ok {
				return fmt.Errorf("connector %s declares %s but does not implement IncrementalReader", spec.Type, cap)
			}
		case CapCDC:
			if _, ok := c.(ChangeStreamer); !ok {
				return fmt.Errorf("connector %s declares %s but does not implement ChangeStreamer", spec.Type, cap)
			}
		default:
			return fmt.Errorf("connector %s declares unknown capability %q", spec.Type, cap)
		}
	}
	if _, ok := c.(FullLoader); ok && !hasCap(spec.Capabilities, CapFullLoad) {
		return fmt.Errorf("connector %s implements FullLoader but does not declare %s", spec.Type, CapFullLoad)
	}
	if _, ok := c.(IncrementalReader); ok && !hasCap(spec.Capabilities, CapIncremental) {
		return fmt.Errorf("connector %s implements IncrementalReader but does not declare %s", spec.Type, CapIncremental)
	}
	if _, ok := c.(ChangeStreamer); ok && !hasCap(spec.Capabilities, CapCDC) {
		return fmt.Errorf("connector %s implements ChangeStreamer but does not declare %s", spec.Type, CapCDC)
	}
	return nil
}

func hasCap(caps []Capability, c Capability) bool {
	for _, x := range caps {
		if x == c {
			return true
		}
	}
	return false
}
