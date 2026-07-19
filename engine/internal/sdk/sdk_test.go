package sdk

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// fakeConnector implements Connector plus whichever facets the test enables.
type fakeConnector struct {
	caps []Capability
}

func (f *fakeConnector) Spec() Spec {
	return Spec{Type: "fake", DisplayName: "Fake", Capabilities: f.caps, Maturity: MaturityBeta}
}
func (f *fakeConnector) Setup(context.Context, json.RawMessage) error     { return nil }
func (f *fakeConnector) Check(context.Context) error                      { return nil }
func (f *fakeConnector) Discover(context.Context) ([]model.Stream, error) { return nil, nil }
func (f *fakeConnector) Close(context.Context) error                      { return nil }

type fakeFullLoader struct{ fakeConnector }

func (f *fakeFullLoader) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) {
	return nil, nil
}
func (f *fakeFullLoader) ReadChunk(context.Context, model.Stream, state.Chunk, RowFunc) error {
	return nil
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	r.Register("fake", func() Connector { return &fakeConnector{} })

	c, err := r.New("fake")
	require.NoError(t, err)
	assert.Equal(t, "fake", c.Spec().Type)

	_, err = r.New("absent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fake", "error must list available connectors")

	assert.Panics(t, func() { r.Register("fake", func() Connector { return &fakeConnector{} }) })
	assert.Equal(t, []string{"fake"}, r.Names())
}

func TestValidateCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		conn    Connector
		wantErr string
	}{
		{
			name: "declares nothing implements nothing",
			conn: &fakeConnector{},
		},
		{
			name: "declares and implements full_load",
			conn: &fakeFullLoader{fakeConnector{caps: []Capability{CapFullLoad}}},
		},
		{
			name:    "declares full_load without implementing",
			conn:    &fakeConnector{caps: []Capability{CapFullLoad}},
			wantErr: "does not implement FullLoader",
		},
		{
			name:    "declares cdc without implementing",
			conn:    &fakeConnector{caps: []Capability{CapCDC}},
			wantErr: "does not implement ChangeStreamer",
		},
		{
			name:    "implements full_load without declaring",
			conn:    &fakeFullLoader{},
			wantErr: "does not declare full_load",
		},
		{
			name:    "unknown capability",
			conn:    &fakeConnector{caps: []Capability{"telepathy"}},
			wantErr: "unknown capability",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCapabilities(tt.conn)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
