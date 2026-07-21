// Package pipelines is the control plane's pipeline write path: create, update,
// pause, and delete pipelines, each mutation composing a versioned config
// (configver) and an append-only audit entry (audit) atomically. It is the
// keystone the create-pipeline UI, the runner (B2), and the live workers (B3)
// build on.
package pipelines

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

// Endpoint is a source or destination in a create/update request.
type Endpoint struct {
	Type     string            `json:"type"`
	Settings map[string]string `json:"settings,omitempty"`
}

// Stream is one selected stream in a request.
type Stream struct {
	Name        string `json:"name"`
	Mode        string `json:"mode"`
	CursorField string `json:"cursor_field,omitempty"`
}

// CreatePipelineRequest is the POST body; it maps directly onto configver.Config.
type CreatePipelineRequest struct {
	Name        string   `json:"name"`
	Environment string   `json:"environment"`
	Source      Endpoint `json:"source"`
	Destination Endpoint `json:"destination"`
	Schedule    string   `json:"schedule"`
	Streams     []Stream `json:"streams"`
}

// Pipeline is the write-side view returned by create/update.
type Pipeline struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	Slug            string `json:"slug"`
	Environment     string `json:"environment"`
	SourceType      string `json:"source_type"`
	DestinationType string `json:"destination_type"`
	Status          string `json:"status"`
	Schedule        string `json:"schedule"`
	CurrentVersion  int    `json:"current_version"`
}

// PipelineRow is the persisted shape the Repo writes.
type PipelineRow struct {
	Name              string
	Slug              string
	SourceType        string
	DestinationType   string
	Schedule          string
	Status            string
	SourceConfig      []byte // JSONB
	DestinationConfig []byte // JSONB
	Catalog           []byte // JSONB
	CurrentVersion    int
}

// ValidationError is a 400-mapped rejection of a bad request.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// NotFoundError is a 404-mapped missing-entity error.
type NotFoundError struct{ ID int64 }

func (e *NotFoundError) Error() string { return fmt.Sprintf("pipeline %d not found", e.ID) }

var validModes = map[string]bool{"full_load": true, "incremental": true, "cdc": true}

// validate checks a request, returning a *ValidationError on the first problem.
func validate(req CreatePipelineRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return &ValidationError{"name is required"}
	}
	if req.Source.Type == "" {
		return &ValidationError{"source.type is required"}
	}
	if req.Destination.Type == "" {
		return &ValidationError{"destination.type is required"}
	}
	if len(req.Streams) == 0 {
		return &ValidationError{"at least one stream is required"}
	}
	for _, s := range req.Streams {
		if s.Name == "" {
			return &ValidationError{"every stream needs a name"}
		}
		if !validModes[s.Mode] {
			return &ValidationError{fmt.Sprintf("stream %s: mode must be full_load, incremental, or cdc", s.Name)}
		}
		if s.Mode == "incremental" && s.CursorField == "" {
			return &ValidationError{fmt.Sprintf("stream %s: incremental mode requires a cursor_field", s.Name)}
		}
	}
	return nil
}

// toConfig maps a request onto the canonical configver.Config.
func toConfig(req CreatePipelineRequest) configver.Config {
	streams := make([]configver.Stream, len(req.Streams))
	for i, s := range req.Streams {
		streams[i] = configver.Stream{Name: s.Name, Mode: s.Mode, CursorField: s.CursorField}
	}
	return configver.Config{
		Name:        req.Name,
		Source:      configver.Endpoint{Type: req.Source.Type, Settings: req.Source.Settings},
		Destination: configver.Endpoint{Type: req.Destination.Type, Settings: req.Destination.Settings},
		Schedule:    req.Schedule,
		Streams:     streams,
	}
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

// slugify makes a URL-safe, lowercase slug from a name.
func slugify(name string) string {
	s := slugRe.ReplaceAllString(strings.ToLower(name), "-")
	return strings.Trim(s, "-")
}
