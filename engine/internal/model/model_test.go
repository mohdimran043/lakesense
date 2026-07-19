package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCatalog() Catalog {
	return Catalog{
		Streams: []Stream{
			{
				Namespace: "public",
				Name:      "users",
				Schema: Schema{Columns: []Column{
					{Name: "id", Type: TypeInt64, PrimaryKey: true},
					{Name: "email", Type: TypeString},
					{Name: "updated_at", Type: TypeTimestamp},
				}},
				SupportedSyncModes: []SyncMode{ModeFullLoad, ModeIncremental, ModeCDC},
			},
			{
				Namespace:          "public",
				Name:               "logs",
				Schema:             Schema{Columns: []Column{{Name: "line", Type: TypeString}}},
				SupportedSyncModes: []SyncMode{ModeFullLoad},
			},
		},
	}
}

func TestCatalogValidate(t *testing.T) {
	tests := []struct {
		name     string
		selected []SelectedStream
		wantErr  string
	}{
		{
			name:     "valid cdc selection",
			selected: []SelectedStream{{Namespace: "public", Name: "users", Mode: ModeCDC}},
		},
		{
			name: "valid incremental with cursor",
			selected: []SelectedStream{
				{Namespace: "public", Name: "users", Mode: ModeIncremental, CursorField: "updated_at"},
			},
		},
		{
			name:     "unknown stream",
			selected: []SelectedStream{{Namespace: "public", Name: "ghost", Mode: ModeFullLoad}},
			wantErr:  "not found",
		},
		{
			name:     "unsupported mode",
			selected: []SelectedStream{{Namespace: "public", Name: "logs", Mode: ModeCDC}},
			wantErr:  "does not support",
		},
		{
			name:     "incremental without cursor",
			selected: []SelectedStream{{Namespace: "public", Name: "users", Mode: ModeIncremental}},
			wantErr:  "requires cursor_field",
		},
		{
			name: "incremental cursor not in schema",
			selected: []SelectedStream{
				{Namespace: "public", Name: "users", Mode: ModeIncremental, CursorField: "nope"},
			},
			wantErr: "not in schema",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testCatalog()
			c.Selected = tt.selected
			err := c.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestSchemaHelpers(t *testing.T) {
	s := testCatalog().Streams[0]
	assert.Equal(t, "public.users", s.ID())
	assert.Equal(t, []string{"id"}, s.Schema.PrimaryKey())

	col, ok := s.Schema.Column("email")
	require.True(t, ok)
	assert.Equal(t, TypeString, col.Type)

	_, ok = s.Schema.Column("absent")
	assert.False(t, ok)
}
