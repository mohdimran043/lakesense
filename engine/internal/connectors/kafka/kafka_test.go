package kafka

import (
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestConfigValidateDefaults(t *testing.T) {
	c := &Config{Brokers: []string{"localhost:9092"}, Topic: "orders"}
	require.NoError(t, c.validate())
	require.Equal(t, 1<<20, c.MaxBytes)
	require.Equal(t, 15, c.DialTimeout)

	require.Error(t, (&Config{Topic: "orders"}).validate())                                   // no brokers
	require.Error(t, (&Config{Brokers: []string{"b"}}).validate())                            // no topic
	require.Error(t, (&Config{Type: "redis", Brokers: []string{"b"}, Topic: "o"}).validate()) // wrong type
}

func TestOffsetsRoundTrip(t *testing.T) {
	// Deterministic, numerically-sorted key order regardless of map iteration.
	require.Equal(t, `{"0":10,"1":3,"2":0}`, encodeOffsets(map[int]int64{2: 0, 0: 10, 1: 3}))

	got, err := decodeOffsets(`{"0":10,"1":3,"2":0}`)
	require.NoError(t, err)
	require.Equal(t, map[int]int64{0: 10, 1: 3, 2: 0}, got)

	// Empty cursor means "from earliest".
	empty, err := decodeOffsets("")
	require.NoError(t, err)
	require.Empty(t, empty)

	_, err = decodeOffsets("not json")
	require.Error(t, err)
}

func TestValueJSON(t *testing.T) {
	require.Nil(t, valueJSON(nil))
	require.Equal(t, `{"a":1}`, valueJSON([]byte(`{"a":1}`))) // already JSON, kept as-is
	require.Equal(t, `"hello"`, valueJSON([]byte("hello")))   // raw bytes → JSON string
}

func TestHeadersJSON(t *testing.T) {
	require.Nil(t, headersJSON(nil))
	got := headersJSON([]segkafka.Header{{Key: "trace", Value: []byte("abc")}})
	require.Equal(t, `{"trace":"abc"}`, got)
}

func TestMessageRow(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	row := messageRow(segkafka.Message{
		Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte(`{"x":1}`), Time: ts,
	})
	require.Equal(t, int32(2), row[colPartition])
	require.Equal(t, int64(7), row[colOffset])
	require.Equal(t, "k", row[colKey])
	require.Equal(t, `{"x":1}`, row[colValue])
	require.Equal(t, "2026-01-02T03:04:05Z", row[colTimestamp])

	// A nil key is preserved as a null cell, not the string "".
	require.Nil(t, messageRow(segkafka.Message{Key: nil})[colKey])
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "kafka", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
	require.Contains(t, spec.Capabilities, sdk.CapFullLoad)
	require.Contains(t, spec.Capabilities, sdk.CapIncremental)
	_ = model.ModeIncremental
}
