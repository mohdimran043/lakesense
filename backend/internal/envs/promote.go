// Package envs handles multi-environment promotion: cloning a pipeline's config
// version from one environment (dev/staging) to another (prod) with the
// connection credentials overridden per target. Stream selections, sync modes,
// and schedule carry over unchanged — only the endpoint settings that must
// differ between environments are swapped. The "multi-environment promotion"
// team/enterprise tiers charge for — free here, and itself audited (4.10).
package envs

import (
	"fmt"
	"sort"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

// Overrides are the per-target endpoint setting replacements applied during
// promotion (e.g. the prod host/password for the source).
type Overrides struct {
	Source      map[string]string
	Destination map[string]string
}

// Promote clones a config for a target environment, applying credential/setting
// overrides. It returns the new config; the streams, modes, and schedule are
// preserved exactly so promotion changes wiring, never behavior.
func Promote(cfg configver.Config, o Overrides) configver.Config {
	out := cfg
	out.Source = applyOverrides(cfg.Source, o.Source)
	out.Destination = applyOverrides(cfg.Destination, o.Destination)
	// Streams and schedule are value-copied with the struct; make the slice
	// independent so callers can't mutate the source config's streams.
	if cfg.Streams != nil {
		out.Streams = append([]configver.Stream(nil), cfg.Streams...)
	}
	return out
}

// applyOverrides merges overrides onto an endpoint's settings (override wins),
// leaving unspecified settings intact.
func applyOverrides(e configver.Endpoint, over map[string]string) configver.Endpoint {
	merged := map[string]string{}
	for k, v := range e.Settings {
		merged[k] = v
	}
	for k, v := range over {
		merged[k] = v
	}
	if len(merged) == 0 {
		merged = nil
	}
	return configver.Endpoint{Type: e.Type, Settings: merged}
}

// MissingCredentials reports which of the sensitive settings present in the
// source config were NOT overridden for the target — the prompt the promotion
// UI shows so no dev credential silently leaks into prod. Returns sorted names.
func MissingCredentials(cfg configver.Config, o Overrides) []string {
	var missing []string
	missing = append(missing, missingIn(cfg.Source.Settings, o.Source, "source")...)
	missing = append(missing, missingIn(cfg.Destination.Settings, o.Destination, "destination")...)
	sort.Strings(missing)
	return missing
}

// sensitiveKeys are settings that must be re-specified per environment.
var sensitiveKeys = map[string]bool{
	"host": true, "password": true, "user": true, "database": true,
	"bucket": true, "token": true, "url": true, "path": true,
}

func missingIn(settings, over map[string]string, side string) []string {
	var out []string
	for k := range settings {
		if sensitiveKeys[k] {
			if _, ok := over[k]; !ok {
				out = append(out, fmt.Sprintf("%s.%s", side, k))
			}
		}
	}
	return out
}
