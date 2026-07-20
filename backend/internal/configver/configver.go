// Package configver is pipeline-as-code: every pipeline config is serialized to
// canonical YAML, each change is a numbered version, and versions can be diffed
// (git-style) and rolled back. This is the "config versioning with rollback"
// that enterprise APIs charge for — free here, and the backbone of environment
// promotion (4.14) and the audit trail (4.10).
package configver

import (
	"bytes"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the canonical, serializable shape of a pipeline. Field order is
// fixed (struct order), so the same logical config always yields byte-identical
// YAML — the property that makes diffs meaningful and versions deduplicable.
type Config struct {
	Name        string   `yaml:"name"`
	Source      Endpoint `yaml:"source"`
	Destination Endpoint `yaml:"destination"`
	Schedule    string   `yaml:"schedule"`
	Streams     []Stream `yaml:"streams"`
}

// Endpoint is a source or destination, with connector-specific settings kept in
// a sorted map so serialization stays deterministic.
type Endpoint struct {
	Type     string            `yaml:"type"`
	Settings map[string]string `yaml:"settings,omitempty"`
}

// Stream is one selected stream and its sync mode.
type Stream struct {
	Name        string `yaml:"name"`
	Mode        string `yaml:"mode"`
	CursorField string `yaml:"cursor_field,omitempty"`
}

// Version is one immutable snapshot of a pipeline's config.
type Version struct {
	Number    int
	YAML      string
	Note      string
	CreatedBy string
	CreatedAt time.Time
}

// CanonicalYAML renders a config to deterministic YAML. yaml.v3 preserves struct
// field order and sorts map keys, so equal configs render identically.
func CanonicalYAML(c Config) (string, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(c); err != nil {
		return "", fmt.Errorf("marshal config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("close encoder: %w", err)
	}
	return buf.String(), nil
}

// Parse decodes canonical YAML back into a Config (for import/apply).
func Parse(y string) (Config, error) {
	var c Config
	if err := yaml.Unmarshal([]byte(y), &c); err != nil {
		return Config{}, fmt.Errorf("parse config yaml: %w", err)
	}
	return c, nil
}

// NewVersion produces the next version for a config. When the canonical YAML is
// byte-identical to the latest version, it returns (latest, false) — no-op
// changes never create a version.
func NewVersion(history []Version, c Config, note, by string, now time.Time) (Version, bool, error) {
	y, err := CanonicalYAML(c)
	if err != nil {
		return Version{}, false, err
	}
	if n := len(history); n > 0 && history[n-1].YAML == y {
		return history[n-1], false, nil
	}
	return Version{
		Number:    len(history) + 1,
		YAML:      y,
		Note:      note,
		CreatedBy: by,
		CreatedAt: now,
	}, true, nil
}

// Rollback creates a new version whose config equals a prior version's — history
// is append-only, so a rollback moves forward to a version that restores old
// content rather than mutating the past.
func Rollback(history []Version, target int, by string, now time.Time) (Version, error) {
	var src *Version
	for i := range history {
		if history[i].Number == target {
			src = &history[i]
			break
		}
	}
	if src == nil {
		return Version{}, fmt.Errorf("version %d not found", target)
	}
	return Version{
		Number:    len(history) + 1,
		YAML:      src.YAML,
		Note:      fmt.Sprintf("rollback to v%d", target),
		CreatedBy: by,
		CreatedAt: now,
	}, nil
}
