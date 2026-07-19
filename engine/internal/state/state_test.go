package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkProtocolRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	doc, err := Load(path)
	require.NoError(t, err)

	// Before chunking: unknown.
	chunks, done := doc.RemainingChunks("public", "users")
	assert.Nil(t, chunks)
	assert.False(t, done)

	plan := []Chunk{{Min: "", Max: "100"}, {Min: "100", Max: "200"}, {Min: "200", Max: ""}}
	require.NoError(t, doc.SetChunks("public", "users", plan))

	// Complete the middle chunk; reload from disk and verify persistence.
	left, err := doc.CompleteChunk("public", "users", Chunk{Min: "100", Max: "200"})
	require.NoError(t, err)
	assert.Equal(t, 2, left)

	reloaded, err := Load(path)
	require.NoError(t, err)
	chunks, done = reloaded.RemainingChunks("public", "users")
	assert.False(t, done)
	assert.Equal(t, []Chunk{{Max: "100"}, {Min: "200"}}, chunks)

	// Finishing all chunks flips done, surviving another reload.
	_, err = reloaded.CompleteChunk("public", "users", Chunk{Max: "100"})
	require.NoError(t, err)
	left, err = reloaded.CompleteChunk("public", "users", Chunk{Min: "200"})
	require.NoError(t, err)
	assert.Equal(t, 0, left)

	final, err := Load(path)
	require.NoError(t, err)
	chunks, done = final.RemainingChunks("public", "users")
	assert.True(t, done)
	assert.Empty(t, chunks)
}

func TestCompleteChunkErrors(t *testing.T) {
	doc := NewInMemory()
	_, err := doc.CompleteChunk("s", "t", Chunk{Min: "1"})
	require.Error(t, err, "no plan yet")

	require.NoError(t, doc.SetChunks("s", "t", []Chunk{{Min: "1", Max: "2"}}))
	_, err = doc.CompleteChunk("s", "t", Chunk{Min: "9", Max: "10"})
	require.Error(t, err, "unknown chunk")
}

func TestCursors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	doc, err := Load(path)
	require.NoError(t, err)

	_, ok := doc.Cursor("s", "t", "updated_at")
	assert.False(t, ok)

	require.NoError(t, doc.SetCursor("s", "t", "updated_at", "2026-07-19T00:00:00Z"))

	reloaded, err := Load(path)
	require.NoError(t, err)
	v, ok := reloaded.Cursor("s", "t", "updated_at")
	assert.True(t, ok)
	assert.Equal(t, "2026-07-19T00:00:00Z", v)
}

func TestGlobalPosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	doc, err := Load(path)
	require.NoError(t, err)

	_, ok := doc.GlobalPosition()
	assert.False(t, ok)
	assert.False(t, doc.BackfilledGlobally("s.t"))

	require.NoError(t, doc.SetGlobalPosition(map[string]string{"lsn": "0/1A2B"}, "s.t"))

	reloaded, err := Load(path)
	require.NoError(t, err)
	pos, ok := reloaded.GlobalPosition()
	assert.True(t, ok)
	assert.Equal(t, "0/1A2B", pos["lsn"])
	assert.True(t, reloaded.BackfilledGlobally("s.t"))
	assert.False(t, reloaded.BackfilledGlobally("s.other"))

	// Marking more streams keeps the position; nil position means no change.
	require.NoError(t, reloaded.SetGlobalPosition(nil, "s.other"))
	pos, _ = reloaded.GlobalPosition()
	assert.Equal(t, "0/1A2B", pos["lsn"])
	assert.True(t, reloaded.BackfilledGlobally("s.other"))
}
