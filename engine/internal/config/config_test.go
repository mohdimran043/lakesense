package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sample struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func TestLoadJSON(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
		want    sample
	}{
		{name: "valid", content: `{"host":"db","port":5432}`, want: sample{Host: "db", Port: 5432}},
		{name: "unknown field rejected", content: `{"host":"db","prot":1}`, wantErr: "prot"},
		{name: "malformed", content: `{`, wantErr: "parse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.json")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			var got sample
			err := LoadJSON(path, &got)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadJSONMissingFile(t *testing.T) {
	var got sample
	err := LoadJSON(filepath.Join(t.TempDir(), "absent.json"), &got)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read")
}

func TestSaveJSONAtomicRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	in := sample{Host: "x", Port: 1}
	require.NoError(t, SaveJSONAtomic(path, in))

	var out sample
	require.NoError(t, LoadJSONLenient(path, &out))
	assert.Equal(t, in, out)

	// Overwrite must replace content wholesale.
	require.NoError(t, SaveJSONAtomic(path, sample{Host: "y", Port: 2}))
	require.NoError(t, LoadJSONLenient(path, &out))
	assert.Equal(t, sample{Host: "y", Port: 2}, out)

	// No temp files may linger.
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
