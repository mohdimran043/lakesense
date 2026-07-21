package runner

import (
	"encoding/json"
	"fmt"
	"strings"
)

// StreamSelection is one selected stream from a pipeline's stored catalog.
type StreamSelection struct {
	Namespace   string
	Name        string
	Mode        string
	CursorField string
}

// flattenEndpoint turns a stored endpoint ({"type":..,"settings":{..}}) into the
// flat config document lsengine expects ({"type":.., ...settings}). The type key
// always wins over a settings collision.
func flattenEndpoint(raw []byte) (map[string]any, error) {
	var ep struct {
		Type     string            `json:"type"`
		Settings map[string]string `json:"settings"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil {
		return nil, fmt.Errorf("parse endpoint config: %w", err)
	}
	out := make(map[string]any, len(ep.Settings)+1)
	for k, v := range ep.Settings {
		out[k] = v
	}
	out["type"] = ep.Type
	return out, nil
}

// parseSelections reads a pipeline's stored catalog ({"streams":[{name,mode,
// cursor_field}]}) into StreamSelections, splitting "namespace.name".
func parseSelections(catalogJSONB []byte) ([]StreamSelection, error) {
	var doc struct {
		Streams []struct {
			Name        string `json:"name"`
			Mode        string `json:"mode"`
			CursorField string `json:"cursor_field"`
		} `json:"streams"`
	}
	if len(catalogJSONB) > 0 {
		if err := json.Unmarshal(catalogJSONB, &doc); err != nil {
			return nil, fmt.Errorf("parse stored catalog: %w", err)
		}
	}
	sels := make([]StreamSelection, 0, len(doc.Streams))
	for _, s := range doc.Streams {
		ns, name := splitStream(s.Name)
		mode := s.Mode
		if mode == "" {
			mode = "full_load"
		}
		sels = append(sels, StreamSelection{Namespace: ns, Name: name, Mode: mode, CursorField: s.CursorField})
	}
	return sels, nil
}

// splitStream splits "namespace.name" on the first dot; an undotted value has an
// empty namespace.
func splitStream(id string) (ns, name string) {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[:i], id[i+1:]
	}
	return "", id
}

// buildCatalog parses the discovered catalog and attaches selected_streams built
// from sels, returning the catalog document lsengine sync consumes. The
// discovered streams (with their schemas) are preserved verbatim.
func buildCatalog(discovered []byte, sels []StreamSelection) ([]byte, error) {
	var cat map[string]json.RawMessage
	if err := json.Unmarshal(discovered, &cat); err != nil {
		return nil, fmt.Errorf("parse discovered catalog: %w", err)
	}
	selected := make([]map[string]any, 0, len(sels))
	for _, s := range sels {
		m := map[string]any{"namespace": s.Namespace, "name": s.Name, "mode": s.Mode}
		if s.CursorField != "" {
			m["cursor_field"] = s.CursorField
		}
		selected = append(selected, m)
	}
	raw, err := json.Marshal(selected)
	if err != nil {
		return nil, fmt.Errorf("marshal selected streams: %w", err)
	}
	cat["selected_streams"] = raw
	out, err := json.Marshal(cat)
	if err != nil {
		return nil, fmt.Errorf("marshal catalog: %w", err)
	}
	return out, nil
}
