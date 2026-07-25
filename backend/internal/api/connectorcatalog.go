package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// Field is one normalized config-form input the wizard renders. It is a flat
// projection of a JSON-Schema property, typed for the UI.
type Field struct {
	Key      string   `json:"key"`
	Label    string   `json:"label"`
	Type     string   `json:"type"` // string | integer | boolean | enum | string-list
	Secret   bool     `json:"secret,omitempty"`
	Required bool     `json:"required,omitempty"`
	Default  string   `json:"default,omitempty"`
	Enum     []string `json:"enum,omitempty"`
	Hint     string   `json:"hint,omitempty"`
}

// Preset is a named wire-compatible variant of a connector (e.g. CockroachDB).
type Preset struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Maturity string `json:"maturity,omitempty"`
	Notes    string `json:"notes,omitempty"`
}

// ConnectorCatalog is one connector as the wizard consumes it: identity,
// maturity badge, capabilities, presets, and normalized config fields.
type ConnectorCatalog struct {
	Type         string   `json:"type"`
	Label        string   `json:"label"`
	Maturity     string   `json:"maturity"`
	Capabilities []string `json:"capabilities"`
	Presets      []Preset `json:"presets,omitempty"`
	Fields       []Field  `json:"fields"`
}

// specDoc is the subset of the engine's sdk.Spec JSON the catalog needs. The
// backend is a separate module, so we decode with local types rather than
// importing the engine.
type specDoc struct {
	Type         string          `json:"type"`
	DisplayName  string          `json:"display_name"`
	Capabilities []string        `json:"capabilities"`
	Maturity     string          `json:"maturity"`
	ConfigSchema json.RawMessage `json:"config_schema"`
	Presets      []struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		Maturity    string `json:"maturity"`
		Notes       string `json:"notes"`
	} `json:"presets"`
}

// schemaDoc is the JSON-Schema subset a config_schema uses.
type schemaDoc struct {
	Required   []string        `json:"required"`
	Properties json.RawMessage `json:"properties"`
}

// propSchema is one property in a config_schema.
type propSchema struct {
	Type        string          `json:"type"`
	Title       string          `json:"title"`
	Description string          `json:"description"`
	Format      string          `json:"format"`
	Enum        []string        `json:"enum"`
	Default     json.RawMessage `json:"default"`
	Items       struct {
		Type string `json:"type"`
	} `json:"items"`
}

// normalizeCatalog turns the engine's []Spec JSON into wizard-ready connectors.
// A connector whose schema fails to parse is still listed (with no fields), so
// one bad schema can never blank the whole wizard.
func normalizeCatalog(specsJSON []byte) ([]ConnectorCatalog, error) {
	var specs []specDoc
	if err := json.Unmarshal(specsJSON, &specs); err != nil {
		return nil, fmt.Errorf("parse connector catalog: %w", err)
	}
	out := make([]ConnectorCatalog, 0, len(specs))
	for _, sp := range specs {
		fields, err := normalizeFields(sp.ConfigSchema)
		if err != nil {
			slog.Default().Warn("connector schema parse failed; listing with no fields",
				"connector", sp.Type, "err", err)
			fields = nil
		}
		cc := ConnectorCatalog{
			Type:         sp.Type,
			Label:        sp.DisplayName,
			Maturity:     sp.Maturity,
			Capabilities: sp.Capabilities,
			Fields:       fields,
		}
		for _, pr := range sp.Presets {
			cc.Presets = append(cc.Presets, Preset{
				Name: pr.Name, Label: pr.DisplayName, Maturity: pr.Maturity, Notes: pr.Notes,
			})
		}
		out = append(out, cc)
	}
	return out, nil
}

// normalizeFields flattens a config_schema's properties into typed Fields, in
// declaration order (Go maps don't preserve order, so keys are read off the raw
// token stream).
func normalizeFields(schema json.RawMessage) ([]Field, error) {
	if len(schema) == 0 {
		return nil, nil
	}
	var doc schemaDoc
	if err := json.Unmarshal(schema, &doc); err != nil {
		return nil, err
	}
	if len(doc.Properties) == 0 {
		return nil, nil
	}
	required := map[string]bool{}
	for _, r := range doc.Required {
		required[r] = true
	}
	keys, err := orderedKeys(doc.Properties)
	if err != nil {
		return nil, err
	}
	var props map[string]propSchema
	if err := json.Unmarshal(doc.Properties, &props); err != nil {
		return nil, err
	}
	fields := make([]Field, 0, len(keys))
	for _, k := range keys {
		p := props[k]
		f := Field{
			Key:      k,
			Label:    orDefault(p.Title, k),
			Type:     fieldType(p),
			Secret:   p.Format == "password",
			Required: required[k],
			Default:  stringifyDefault(p.Default),
			Hint:     p.Description,
		}
		if f.Type == "enum" {
			f.Enum = p.Enum
		}
		fields = append(fields, f)
	}
	return fields, nil
}

// fieldType maps a JSON-Schema property to the wizard's input type.
func fieldType(p propSchema) string {
	switch {
	case len(p.Enum) > 0:
		return "enum"
	case p.Type == "array" && p.Items.Type == "string":
		return "string-list"
	case p.Type == "integer":
		return "integer"
	case p.Type == "boolean":
		return "boolean"
	default:
		return "string"
	}
}

// orderedKeys returns a JSON object's keys in declaration order. It reads the
// key tokens off a decoder and consumes each value with Decode so nested
// objects/arrays are skipped correctly.
func orderedKeys(obj json.RawMessage) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("properties is not an object")
	}
	var keys []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected string property key, got %T", keyTok)
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// stringifyDefault renders a JSON default value as a string for the form: a JSON
// string is unquoted; anything else (number, bool) uses its raw text.
func stringifyDefault(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
